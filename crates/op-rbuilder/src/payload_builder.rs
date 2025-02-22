use std::{fmt::Display, sync::Arc, sync::Mutex};

use crate::generator::{BlockCell, BuildArguments, PayloadBuilder};
use crate::tx_executor::{Executor, TxExecutionInfo};
use alloy_consensus::{Header, Transaction, Typed2718, EMPTY_OMMER_ROOT_HASH};
use alloy_eips::merge::BEACON_NONCE;
use alloy_primitives::{Address, Bytes, B256, U256};
use alloy_rpc_types_engine::PayloadId;
use reth_basic_payload_builder::*;
use reth_chainspec::ChainSpecProvider;
use reth_evm::{env::EvmEnv, system_calls::SystemCaller, ConfigureEvm, NextBlockEnvAttributes};
use reth_execution_types::ExecutionOutcome;
use reth_optimism_chainspec::OpChainSpec;
use reth_optimism_consensus::calculate_receipt_root_no_memo_optimism;
use reth_optimism_forks::OpHardforks;
use reth_optimism_primitives::{OpReceipt, OpTransactionSigned};
use reth_payload_builder_primitives::PayloadBuilderError;
use reth_payload_primitives::PayloadBuilderAttributes;
use reth_payload_util::PayloadTransactions;
use reth_primitives::BlockExt;
use reth_primitives::{
    proofs, transaction::SignedTransactionIntoRecoveredExt, Block, BlockBody, SealedHeader, TxType,
};
use reth_provider::{
    HashedPostStateProvider, ProviderError, StateProviderFactory, StateRootProvider,
};
use reth_revm::database::StateProviderDatabase;
use reth_transaction_pool::PoolTransaction;
use reth_transaction_pool::{BestTransactionsAttributes, TransactionPool};
use revm::{
    db::{states::bundle_state::BundleRetention, BundleState, State},
    primitives::{BlockEnv, CfgEnvWithHandlerCfg, InvalidTransaction},
    Database, DatabaseCommit,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};

use op_alloy_rpc_types_engine::OpExecutionPayloadEnvelopeV3;
use reth_optimism_payload_builder::error::OpPayloadBuilderError;
use reth_optimism_payload_builder::payload::{OpBuiltPayload, OpPayloadBuilderAttributes};
use reth_transaction_pool::pool::BestPayloadTransactions;

use futures_util::FutureExt;
use futures_util::SinkExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::WebSocketStream;

/// Optimism's payload builder
#[derive(Debug, Clone)]
pub struct OpPayloadBuilder<EvmConfig, Txs = ()> {
    /// The type responsible for creating the evm.
    pub evm_config: EvmConfig,
    /// The type responsible for yielding the best transactions for the payload if mempool
    /// transactions are allowed.
    pub best_transactions: Txs,
    /// WebSocket subscribers
    pub subscribers: Arc<Mutex<Vec<WebSocketStream<TcpStream>>>>,
    /// Channel sender for publishing messages
    pub tx: mpsc::UnboundedSender<String>,
}

impl<EvmConfig> OpPayloadBuilder<EvmConfig> {
    /// `OpPayloadBuilder` constructor.
    pub fn new(evm_config: EvmConfig) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let subscribers = Arc::new(Mutex::new(Vec::new()));

        Self::publish_task(rx, subscribers.clone());

        Self {
            evm_config,
            best_transactions: (),
            subscribers,
            tx,
        }
    }

    /// Start the WebSocket server
    pub async fn start_ws(&self, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(addr).await?;
        let subscribers = self.subscribers.clone();

        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tracing::info!("Accepted websocket connection");
                let subscribers = subscribers.clone();

                tokio::spawn(async move {
                    match accept_async(stream).await {
                        Ok(ws_stream) => {
                            let mut subs = subscribers.lock().unwrap();
                            subs.push(ws_stream);
                        }
                        Err(e) => eprintln!("Error accepting websocket connection: {}", e),
                    }
                });
            }
        });

        Ok(())
    }

    /// Background task that handles publishing messages to WebSocket subscribers
    fn publish_task(
        mut rx: mpsc::UnboundedReceiver<String>,
        subscribers: Arc<Mutex<Vec<WebSocketStream<TcpStream>>>>,
    ) {
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                let mut subscribers = subscribers.lock().unwrap();

                // Remove disconnected subscribers and send message to connected ones
                subscribers.retain_mut(|ws_stream| {
                    let message = message.clone();
                    async move {
                        ws_stream
                            .send(tokio_tungstenite::tungstenite::Message::Text(
                                message.into(),
                            ))
                            .await
                            .is_ok()
                    }
                    .now_or_never()
                    .unwrap_or(false)
                });
            }
        });
    }
}

impl<EvmConfig, Txs> OpPayloadBuilder<EvmConfig, Txs>
where
    EvmConfig: ConfigureEvm<Header = Header, Transaction = OpTransactionSigned>,
    Txs: OpPayloadTransactions,
{
    /// Send a message to be published
    pub fn send_message(&self, message: String) -> Result<(), Box<dyn std::error::Error>> {
        self.tx.send(message).map_err(|e| e.into())
    }

    /// Constructs an Optimism payload from the transactions sent via the
    /// Payload attributes by the sequencer. If the `no_tx_pool` argument is passed in
    /// the payload attributes, the transaction pool will be ignored and the only transactions
    /// included in the payload will be those sent through the attributes.
    ///
    /// Given build arguments including an Optimism client, transaction pool,
    /// and configuration, this function creates a transaction payload. Returns
    /// a result indicating success with the payload or an error in case of failure.
    fn build_payload<Client, Pool>(
        &self,
        args: BuildArguments<Pool, Client, OpPayloadBuilderAttributes>,
        best_payload: BlockCell<OpBuiltPayload>,
    ) -> Result<(), PayloadBuilderError>
    where
        Client: StateProviderFactory + ChainSpecProvider<ChainSpec = OpChainSpec>,
        Pool: TransactionPool<Transaction: PoolTransaction<Consensus = EvmConfig::Transaction>>,
    {
        let EvmEnv {
            cfg_env_with_handler_cfg,
            block_env,
        } = self
            .cfg_and_block_env(&args.config.attributes, &args.config.parent_header)
            .map_err(PayloadBuilderError::other)?;
        let BuildArguments {
            client,
            pool,
            config,
            cancel,
            ..
        } = args;

        let ctx = OpPayloadBuilderCtx {
            evm_config: self.evm_config.clone(),
            chain_spec: client.chain_spec(),
            config,
            initialized_cfg: cfg_env_with_handler_cfg,
            initialized_block_env: block_env,
            cancel,
        };

        let best = self.best_transactions.clone();

        let state_provider = client.state_by_block_hash(ctx.parent().hash())?;
        let state = StateProviderDatabase::new(&state_provider);

        let mut db = State::builder()
            .with_database(state)
            .with_bundle_update()
            .build();

        // 1. execute the pre steps and seal an early block with that
        let mut info = execute_pre_steps(&mut db, &ctx)?;
        let (payload, mut bundle_state) = build_block(db, &ctx, &info)?;

        best_payload.set(payload.clone());
        let _ = self.send_message(
            serde_json::to_string(&OpExecutionPayloadEnvelopeV3::from(payload)).unwrap_or_default(),
        );

        tracing::info!(target: "payload_builder", "Fallback block built");

        if ctx.attributes().no_tx_pool {
            tracing::info!(
                target: "payload_builder",
                "No transaction pool, skipping transaction pool processing",
            );

            // return early since we don't need to build a block with transactions from the pool
            return Ok(());
        }

        // Right now it assumes a 1 second block time (TODO)
        let gas_per_batch = ctx.block_gas_limit() / 4;
        let mut total_gas_per_batch = gas_per_batch;

        let mut flashblock_count = 0;

        // 2. loop every n time and try to build an increasing block
        loop {
            if ctx.cancel.is_cancelled() {
                tracing::info!(
                    target: "payload_builder",
                    "Job cancelled, stopping payload building",
                );
                // if the job was cancelled, stop
                return Ok(());
            }

            tracing::info!(
                target: "payload_builder",
                "Building flashblock {}",
                flashblock_count,
            );

            let state = StateProviderDatabase::new(&state_provider);

            let mut db = State::builder()
                .with_database(state)
                .with_bundle_update()
                .with_bundle_prestate(bundle_state)
                .build();

            let best_txs = best.best_transactions(&pool, ctx.best_transaction_attributes());
            ctx.execute_best_transactions(&mut info, &mut db, best_txs, total_gas_per_batch)?;

            if ctx.cancel.is_cancelled() {
                tracing::info!(
                    target: "payload_builder",
                    "Job cancelled, stopping payload building",
                );
                // if the job was cancelled, stop
                return Ok(());
            }

            let (payload, new_bundle_state) = build_block(db, &ctx, &info)?;

            best_payload.set(payload.clone());
            let _ = self.send_message(
                serde_json::to_string(&OpExecutionPayloadEnvelopeV3::from(payload))
                    .unwrap_or_default(),
            );

            bundle_state = new_bundle_state;
            total_gas_per_batch += gas_per_batch;
            flashblock_count += 1;

            std::thread::sleep(std::time::Duration::from_millis(250));
        }
    }
}

impl<EvmConfig, Txs> OpPayloadBuilder<EvmConfig, Txs>
where
    EvmConfig: ConfigureEvm<Header = Header>,
{
    /// Returns the configured [`CfgEnvWithHandlerCfg`] and [`BlockEnv`] for the targeted payload
    /// (that has the `parent` as its parent).
    pub fn cfg_and_block_env(
        &self,
        attributes: &OpPayloadBuilderAttributes,
        parent: &Header,
    ) -> Result<EvmEnv, EvmConfig::Error> {
        let next_attributes = NextBlockEnvAttributes {
            timestamp: attributes.timestamp(),
            suggested_fee_recipient: attributes.suggested_fee_recipient(),
            prev_randao: attributes.prev_randao(),
            gas_limit: parent.gas_limit,
        };
        self.evm_config
            .next_cfg_and_block_env(parent, next_attributes)
    }
}

impl<EvmConfig, Pool, Client> PayloadBuilder<Pool, Client> for OpPayloadBuilder<EvmConfig>
where
    Client: StateProviderFactory + ChainSpecProvider<ChainSpec = OpChainSpec>,
    Pool: TransactionPool<Transaction: PoolTransaction<Consensus = EvmConfig::Transaction>>,
    EvmConfig: ConfigureEvm<Header = Header, Transaction = OpTransactionSigned>,
{
    type Attributes = OpPayloadBuilderAttributes;
    type BuiltPayload = OpBuiltPayload;

    fn try_build(
        &self,
        args: BuildArguments<Pool, Client, Self::Attributes>,
        best_payload: BlockCell<Self::BuiltPayload>,
    ) -> Result<(), PayloadBuilderError> {
        self.build_payload(args, best_payload)
    }
}

pub fn build_block<EvmConfig, DB, P>(
    mut state: State<DB>,
    ctx: &OpPayloadBuilderCtx<EvmConfig>,
    info: &ExecutionInfo,
) -> Result<(OpBuiltPayload, BundleState), PayloadBuilderError>
where
    EvmConfig: ConfigureEvm<Header = Header>,
    DB: Database<Error = ProviderError> + AsRef<P>,
    P: StateRootProvider + HashedPostStateProvider,
{
    let withdrawals_root = ctx.commit_withdrawals(&mut state)?;

    // TODO: We must run this only once per block, but we are running it on every flashblock
    // merge all transitions into bundle state, this would apply the withdrawal balance changes
    // and 4788 contract call
    state.merge_transitions(BundleRetention::Reverts);

    let new_bundle = state.take_bundle();

    let block_number = ctx.block_number();
    assert_eq!(block_number, ctx.parent().number + 1);

    let execution_outcome = ExecutionOutcome::new(
        new_bundle.clone(),
        info.receipts.clone().into(),
        block_number,
        Vec::new(),
    );
    let receipts_root = execution_outcome
        .generic_receipts_root_slow(block_number, |receipts| {
            calculate_receipt_root_no_memo_optimism(
                receipts,
                &ctx.chain_spec,
                ctx.attributes().timestamp(),
            )
        })
        .expect("Number is in range");
    let logs_bloom = execution_outcome
        .block_logs_bloom(block_number)
        .expect("Number is in range");

    // // calculate the state root
    let state_provider = state.database.as_ref();
    let hashed_state = state_provider.hashed_post_state(execution_outcome.state());
    let (state_root, _trie_output) = {
        state
            .database
            .as_ref()
            .state_root_with_updates(hashed_state.clone())
            .inspect_err(|err| {
                warn!(target: "payload_builder",
                parent_header=%ctx.parent().hash(),
                    %err,
                    "failed to calculate state root for payload"
                );
            })?
    };

    // create the block header
    let transactions_root = proofs::calculate_transaction_root(&info.executed_transactions);

    // OP doesn't support blobs/EIP-4844.
    // https://specs.optimism.io/protocol/exec-engine.html#ecotone-disable-blob-transactions
    // Need [Some] or [None] based on hardfork to match block hash.
    let (excess_blob_gas, blob_gas_used) = ctx.blob_fields();
    let extra_data = ctx.extra_data()?;

    let header = Header {
        parent_hash: ctx.parent().hash(),
        ommers_hash: EMPTY_OMMER_ROOT_HASH,
        beneficiary: ctx.initialized_block_env.coinbase,
        state_root,
        transactions_root,
        receipts_root,
        withdrawals_root,
        logs_bloom,
        timestamp: ctx.attributes().payload_attributes.timestamp,
        mix_hash: ctx.attributes().payload_attributes.prev_randao,
        nonce: BEACON_NONCE.into(),
        base_fee_per_gas: Some(ctx.base_fee()),
        number: ctx.parent().number + 1,
        gas_limit: ctx.block_gas_limit(),
        difficulty: U256::ZERO,
        gas_used: info.cumulative_gas_used,
        extra_data,
        parent_beacon_block_root: ctx.attributes().payload_attributes.parent_beacon_block_root,
        blob_gas_used,
        excess_blob_gas,
        requests_hash: None,
    };

    let withdrawals = Some(ctx.attributes().payload_attributes.withdrawals().clone());
    // seal the block
    let block = Block {
        header,
        body: BlockBody {
            transactions: info.executed_transactions.clone(),
            ommers: vec![],
            withdrawals,
        },
    };

    let sealed_block: Arc<
        reth_primitives::SealedBlock<Header, alloy_consensus::BlockBody<OpTransactionSigned>>,
    > = Arc::new(block.seal_slow());
    debug!(target: "payload_builder", ?sealed_block, "sealed built block");

    Ok((
        OpBuiltPayload::new(
            ctx.payload_id(),
            sealed_block,
            info.total_fees,
            ctx.chain_spec.clone(),
            ctx.config.attributes.clone(),
            // This must be set to NONE for now because we are doing merge transitions on every flashblock
            // when it should only happen once per block, thus, it returns a confusing state back to op-reth.
            // We can live without this for now because Op syncs up the executed block using new_payload
            // calls, but eventually we would want to return the executed block here.
            None,
        ),
        new_bundle,
    ))
}

fn execute_pre_steps<EvmConfig, DB>(
    state: &mut State<DB>,
    ctx: &OpPayloadBuilderCtx<EvmConfig>,
) -> Result<ExecutionInfo, PayloadBuilderError>
where
    EvmConfig: ConfigureEvm<Header = Header, Transaction = OpTransactionSigned>,
    DB: Database<Error = ProviderError>,
{
    // 1. apply eip-4788 pre block contract call
    ctx.apply_pre_beacon_root_contract_call(state)?;

    // 2. ensure create2deployer is force deployed
    ctx.ensure_create2_deployer(state)?;

    // 3. execute sequencer transactions
    let info = ctx.execute_sequencer_transactions(state)?;

    Ok(info)
}

/// A type that returns a the [`PayloadTransactions`] that should be included in the pool.
pub trait OpPayloadTransactions: Clone + Send + Sync + Unpin + 'static {
    /// Returns an iterator that yields the transaction in the order they should get included in the
    /// new payload.
    fn best_transactions<
        Pool: TransactionPool<Transaction: PoolTransaction<Consensus = OpTransactionSigned>>,
    >(
        &self,
        pool: Pool,
        attr: BestTransactionsAttributes,
    ) -> impl PayloadTransactions<Transaction = OpTransactionSigned>;
}

impl OpPayloadTransactions for () {
    fn best_transactions<
        Pool: TransactionPool<Transaction: PoolTransaction<Consensus = OpTransactionSigned>>,
    >(
        &self,
        pool: Pool,
        attr: BestTransactionsAttributes,
    ) -> impl PayloadTransactions<Transaction = OpTransactionSigned> {
        BestPayloadTransactions::new(pool.best_transactions_with_attributes(attr))
    }
}

/// Holds the state after execution
#[derive(Debug)]
pub struct ExecutedPayload {
    /// Tracked execution info
    pub info: ExecutionInfo,
    /// Withdrawal hash.
    pub withdrawals_root: Option<B256>,
}

/// This acts as the container for executed transactions and its byproducts (receipts, gas used)
#[derive(Default, Debug)]
pub struct ExecutionInfo {
    /// All executed transactions (unrecovered).
    pub executed_transactions: Vec<OpTransactionSigned>,
    /// The recovered senders for the executed transactions.
    pub executed_senders: Vec<Address>,
    /// The transaction receipts
    pub receipts: Vec<OpReceipt>,
    /// All gas used so far
    pub cumulative_gas_used: u64,
    /// Tracks fees from executed mempool transactions
    pub total_fees: U256,
}

impl ExecutionInfo {
    /// Create a new instance with allocated slots.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            executed_transactions: Vec::with_capacity(capacity),
            executed_senders: Vec::with_capacity(capacity),
            receipts: Vec::with_capacity(capacity),
            cumulative_gas_used: 0,
            total_fees: U256::ZERO,
        }
    }

    pub fn add(&mut self, info: TxExecutionInfo) {
        self.executed_transactions.push(info.tx);
        self.executed_senders.push(info.sender);
        self.receipts.push(info.receipt);
        self.cumulative_gas_used += info.gas_used;
        self.total_fees += info.fee;
    }
}

/// Container type that holds all necessities to build a new payload.
#[derive(Debug)]
pub struct OpPayloadBuilderCtx<EvmConfig> {
    /// The type that knows how to perform system calls and configure the evm.
    pub evm_config: EvmConfig,
    /// The chainspec
    pub chain_spec: Arc<OpChainSpec>,
    /// How to build the payload.
    pub config: PayloadConfig<OpPayloadBuilderAttributes>,
    /// Evm Settings
    pub initialized_cfg: CfgEnvWithHandlerCfg,
    /// Block config
    pub initialized_block_env: BlockEnv,
    /// Marker to check whether the job has been cancelled.
    pub cancel: CancellationToken,
}

impl<EvmConfig> OpPayloadBuilderCtx<EvmConfig> {
    /// Returns the parent block the payload will be build on.
    pub fn parent(&self) -> &SealedHeader {
        &self.config.parent_header
    }

    /// Returns the builder attributes.
    pub const fn attributes(&self) -> &OpPayloadBuilderAttributes {
        &self.config.attributes
    }

    /// Returns the block gas limit to target.
    pub fn block_gas_limit(&self) -> u64 {
        self.attributes()
            .gas_limit
            .unwrap_or_else(|| self.initialized_block_env.gas_limit.saturating_to())
    }

    /// Returns the block number for the block.
    pub fn block_number(&self) -> u64 {
        self.initialized_block_env.number.to()
    }

    /// Returns the current base fee
    pub fn base_fee(&self) -> u64 {
        self.initialized_block_env.basefee.to()
    }

    /// Returns the current blob gas price.
    pub fn get_blob_gasprice(&self) -> Option<u64> {
        self.initialized_block_env
            .get_blob_gasprice()
            .map(|gasprice| gasprice as u64)
    }

    /// Returns the blob fields for the header.
    ///
    /// This will always return `Some(0)` after ecotone.
    pub fn blob_fields(&self) -> (Option<u64>, Option<u64>) {
        // OP doesn't support blobs/EIP-4844.
        // https://specs.optimism.io/protocol/exec-engine.html#ecotone-disable-blob-transactions
        // Need [Some] or [None] based on hardfork to match block hash.
        if self.is_ecotone_active() {
            (Some(0), Some(0))
        } else {
            (None, None)
        }
    }

    /// Returns the extra data for the block.
    ///
    /// After holocene this extracts the extradata from the paylpad
    pub fn extra_data(&self) -> Result<Bytes, PayloadBuilderError> {
        if self.is_holocene_active() {
            self.attributes()
                .get_holocene_extra_data(
                    self.chain_spec.base_fee_params_at_timestamp(
                        self.attributes().payload_attributes.timestamp,
                    ),
                )
                .map_err(PayloadBuilderError::other)
        } else {
            Ok(Default::default())
        }
    }

    /// Returns the current fee settings for transactions from the mempool
    pub fn best_transaction_attributes(&self) -> BestTransactionsAttributes {
        BestTransactionsAttributes::new(self.base_fee(), self.get_blob_gasprice())
    }

    /// Returns the unique id for this payload job.
    pub fn payload_id(&self) -> PayloadId {
        self.attributes().payload_id()
    }

    /// Returns true if regolith is active for the payload.
    pub fn is_regolith_active(&self) -> bool {
        self.chain_spec
            .is_regolith_active_at_timestamp(self.attributes().timestamp())
    }

    /// Returns true if ecotone is active for the payload.
    pub fn is_ecotone_active(&self) -> bool {
        self.chain_spec
            .is_ecotone_active_at_timestamp(self.attributes().timestamp())
    }

    /// Returns true if canyon is active for the payload.
    pub fn is_canyon_active(&self) -> bool {
        self.chain_spec
            .is_canyon_active_at_timestamp(self.attributes().timestamp())
    }

    /// Returns true if holocene is active for the payload.
    pub fn is_holocene_active(&self) -> bool {
        self.chain_spec
            .is_holocene_active_at_timestamp(self.attributes().timestamp())
    }

    /// Commits the withdrawals from the payload attributes to the state.
    pub fn commit_withdrawals<DB>(&self, db: &mut State<DB>) -> Result<Option<B256>, ProviderError>
    where
        DB: Database<Error = ProviderError>,
    {
        commit_withdrawals(
            db,
            &self.chain_spec,
            self.attributes().payload_attributes.timestamp,
            &self.attributes().payload_attributes.withdrawals,
        )
    }

    /// Ensure that the create2deployer is force-deployed at the canyon transition. Optimism
    /// blocks will always have at least a single transaction in them (the L1 info transaction),
    /// so we can safely assume that this will always be triggered upon the transition and that
    /// the above check for empty blocks will never be hit on OP chains.
    pub fn ensure_create2_deployer<DB>(&self, db: &mut State<DB>) -> Result<(), PayloadBuilderError>
    where
        DB: Database,
        DB::Error: Display,
    {
        reth_optimism_evm::ensure_create2_deployer(
            self.chain_spec.clone(),
            self.attributes().payload_attributes.timestamp,
            db,
        )
        .map_err(|err| {
            warn!(target: "payload_builder", %err, "missing create2 deployer, skipping block.");
            PayloadBuilderError::other(OpPayloadBuilderError::ForceCreate2DeployerFail)
        })
    }
}

impl<EvmConfig> OpPayloadBuilderCtx<EvmConfig>
where
    EvmConfig: ConfigureEvm<Header = Header, Transaction = OpTransactionSigned>,
{
    /// apply eip-4788 pre block contract call
    pub fn apply_pre_beacon_root_contract_call<DB>(
        &self,
        db: &mut DB,
    ) -> Result<(), PayloadBuilderError>
    where
        DB: Database + DatabaseCommit,
        DB::Error: Display,
    {
        SystemCaller::new(self.evm_config.clone(), self.chain_spec.clone())
            .pre_block_beacon_root_contract_call(
                db,
                &self.initialized_cfg,
                &self.initialized_block_env,
                self.attributes()
                    .payload_attributes
                    .parent_beacon_block_root,
            )
            .map_err(|err| {
                warn!(target: "payload_builder",
                    parent_header=%self.parent().hash(),
                    %err,
                    "failed to apply beacon root contract call for payload"
                );
                PayloadBuilderError::Internal(err.into())
            })?;

        Ok(())
    }

    /// Executes all sequencer transactions that are included in the payload attributes.
    pub fn execute_sequencer_transactions<DB>(
        &self,
        db: &mut State<DB>,
    ) -> Result<ExecutionInfo, PayloadBuilderError>
    where
        DB: Database<Error = ProviderError>,
    {
        let mut info = ExecutionInfo::with_capacity(self.attributes().transactions.len());

        let mut executor = Executor::new(
            self.initialized_cfg.clone(),
            &self.evm_config,
            self.initialized_block_env.clone(),
            db,
            0,
        );

        for sequencer_tx in &self.attributes().transactions {
            // A sequencer's block should never contain blob transactions.
            if sequencer_tx.value().is_eip4844() {
                return Err(PayloadBuilderError::other(
                    OpPayloadBuilderError::BlobTransactionRejected,
                ));
            }

            // Convert the transaction to a [TransactionSignedEcRecovered]. This is
            // purely for the purposes of utilizing the `evm_config.tx_env`` function.
            // Deposit transactions do not have signatures, so if the tx is a deposit, this
                // will just pull in its `from` address.
                let tx = sequencer_tx
                    .value()
                    .clone()
                    .try_into_ecrecovered()
                    .map_err(|_| {
                        PayloadBuilderError::other(OpPayloadBuilderError::TransactionEcRecoverFailed)
                    })?;

            executor.execute(tx).map_or_else(
                |err| {
                    // match err {
                    // EVMError::Transaction(err) => {
                    //     trace!(target: "payload_builder", %err, ?sequencer_tx, "Error in sequencer transaction, skipping.");
                    //     continue;
                    // }
                    // err => {
                    //     // this is an error that we should treat as fatal for this attempt
                    //     return Err(PayloadBuilderError::EvmExecutionError(err));
                    // }
                    // }
                    todo!();
                },
                |(uncommitted, executed)| {
                    uncommitted.commit();
                    info.add(executed.into());
                },
            );
        }
        Ok(info)
    }

    /// Executes the given best transactions and updates the execution info.
    ///
    /// Returns `Ok(Some(())` if the job was cancelled.
    pub fn execute_best_transactions<DB>(
        &self,
        info: &mut ExecutionInfo,
        db: &mut State<DB>,
        mut best_txs: impl PayloadTransactions<Transaction = EvmConfig::Transaction>,
        batch_gas_limit: u64,
    ) -> Result<Option<()>, PayloadBuilderError>
    where
        DB: Database<Error = ProviderError>,
    {
        let mut executor = Executor::new(
            self.initialized_cfg.clone(),
            &self.evm_config,
            self.initialized_block_env.clone(),
            db,
            self.base_fee(),
        );

        while let Some(tx) = best_txs.next(()) {
            // check in info if the txn has been executed already
            if info.executed_transactions.contains(&tx) {
                continue;
            }

            // ensure we still have capacity for this transaction
            if info.cumulative_gas_used + tx.gas_limit() > batch_gas_limit {
                // we can't fit this transaction into the block, so we need to mark it as
                // invalid which also removes all dependent transaction from
                // the iterator before we can continue
                best_txs.mark_invalid(tx.signer(), tx.nonce());
                continue;
            }

            // A sequencer's block should never contain blob or deposit transactions from the pool.
            if tx.is_eip4844() || tx.tx_type() == TxType::Deposit as u8 {
                best_txs.mark_invalid(tx.signer(), tx.nonce());
                continue;
            }

            // check if the job was cancelled, if so we can exit early
            if self.cancel.is_cancelled() {
                return Ok(Some(()));
            }

            match executor.execute(tx) {
                Ok((uncommitted, executed)) => {
                    uncommitted.commit();
                    info.add(executed.into());
                }
                Err(err) => {
                    // if let Some(invalid) = err.downcast_ref::<InvalidTransaction>() {
                    //     match invalid {
                    //         InvalidTransaction::NonceTooLow { .. } => {
                    //             trace!(target: "payload_builder", %invalid, ?tx, "skipping nonce too low transaction");
                    //         }
                    //         _ => {
                    //             trace!(target: "payload_builder", %invalid, ?tx, "skipping invalid transaction and its descendants");
                    //             best_txs.mark_invalid(tx.signer(), tx.nonce());
                    //         }
                    //     }
                    // }
                    // return Err(PayloadBuilderError::EvmExecutionError(EVMError::Transaction(
                    //     *invalid_tx,
                    // )));
                    todo!();
                }
            };
        }
        Ok(None)
    }
}
