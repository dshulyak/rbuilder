use std::{sync::Arc, sync::Mutex};

use alloy_consensus::{Header, EMPTY_OMMER_ROOT_HASH};
use alloy_eips::merge::BEACON_NONCE;
use alloy_primitives::{B256, U256};
use futures_util::FutureExt;
use futures_util::SinkExt;
use op_alloy_rpc_types_engine::OpExecutionPayloadEnvelopeV3;
use reth_chainspec::ChainSpecProvider;
use reth_evm::env::EvmEnv;
use reth_execution_types::ExecutionOutcome;
use reth_optimism_chainspec::OpChainSpec;
use reth_optimism_consensus::calculate_receipt_root_no_memo_optimism;
use reth_optimism_payload_builder::payload::{OpBuiltPayload, OpPayloadBuilderAttributes};
use reth_optimism_primitives::OpTransactionSigned;
use reth_payload_builder_primitives::PayloadBuilderError;
use reth_payload_primitives::PayloadBuilderAttributes;
use reth_primitives::{proofs, Block, BlockBody, BlockExt};
use reth_provider::{
    HashedPostStateProvider, ProviderError, StateProviderFactory, StateRootProvider,
};
use reth_revm::database::StateProviderDatabase;
use reth_transaction_pool::PoolTransaction;
use reth_transaction_pool::TransactionPool;
use revm::db::{states::bundle_state::BundleRetention, BundleState, State};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::WebSocketStream;
use tracing::{debug, warn};

use crate::actions::pre_block::OpPreBlockActions;
use crate::actions::pre_block::OpPreBlockActionsV1;
use crate::actions::tx_executor::OpTransactionsActions;
use crate::builder_vanilla::cfg_and_block_env;
use crate::components::execution_info::ExecutionInfo;
use crate::components::payload_context::OpPayloadBuilderCtx;
use crate::components::payload_transactions::{BestPoolTransactions, OpPayloadTransactions};
use crate::components::tx_executor::{ConfigureEvm, Database, OpTxExecutor, StateAccess};
use crate::generator::{BlockCell, BuildArguments, PayloadBuilder};
use crate::impl_traits;

/// Optimism's payload builder
#[derive(Debug, Clone)]
pub struct OpPayloadBuilder<Strategy, EvmConfig, Txs = BestPoolTransactions> {
    /// The type responsible for creating the evm.
    pub evm_config: EvmConfig,
    /// The type responsible for yielding the best transactions for the payload if mempool
    /// transactions are allowed.
    pub best_transactions: Txs,
    /// WebSocket subscribers
    pub subscribers: Arc<Mutex<Vec<WebSocketStream<TcpStream>>>>,
    /// Channel sender for publishing messages
    pub tx: mpsc::UnboundedSender<String>,
    /// Strategy to use for building payloads.
    pub strategy: Strategy,
}

impl<Strategy, EvmConfig> OpPayloadBuilder<Strategy, EvmConfig> {
    /// `OpPayloadBuilder` constructor.
    pub fn new(evm_config: EvmConfig, strategy: Strategy) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let subscribers = Arc::new(Mutex::new(Vec::new()));

        Self::publish_task(rx, subscribers.clone());

        Self {
            evm_config,
            best_transactions: BestPoolTransactions,
            subscribers,
            tx,
            strategy,
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

impl<Strategy, EvmConfig, Pool, Client> PayloadBuilder<Pool, Client>
    for OpPayloadBuilder<Strategy, EvmConfig>
where
    Client: StateProviderFactory + ChainSpecProvider<ChainSpec = OpChainSpec>,
    Pool: TransactionPool<Transaction: PoolTransaction<Consensus = EvmConfig::Transaction>>,
    EvmConfig: ConfigureEvm<Header = Header, Transaction = OpTransactionSigned>,
    Strategy: FlashblocksStrategy + Clone + Send + Sync + 'static,
{
    type Attributes = OpPayloadBuilderAttributes;
    type BuiltPayload = OpBuiltPayload;

    fn try_build(
        &self,
        args: BuildArguments<Pool, Client, Self::Attributes>,
        best_payload: BlockCell<Self::BuiltPayload>,
    ) -> Result<(), PayloadBuilderError> {
        self.strategy.build(
            &self.evm_config,
            args,
            best_payload,
            &self.tx,
            self.best_transactions.clone(),
        )
    }
}

trait FlashblocksStrategy: OpPreBlockActions + OpTransactionsActions {
    /// Constructs an Optimism payload from the transactions sent via the
    /// Payload attributes by the sequencer. If the `no_tx_pool` argument is passed in
    /// the payload attributes, the transaction pool will be ignored and the only transactions
    /// included in the payload will be those sent through the attributes.
    ///
    /// Given build arguments including an Optimism client, transaction pool,
    /// and configuration, this function creates a transaction payload. Returns
    /// a result indicating success with the payload or an error in case of failure.
    fn build<'a, EvmConfig, Client, Pool, Txs>(
        &'a self,
        evm_config: &'a EvmConfig,
        args: BuildArguments<Pool, Client, OpPayloadBuilderAttributes>,
        best_payload: BlockCell<OpBuiltPayload>,
        message_sender: &UnboundedSender<String>,
        txs: Txs,
    ) -> Result<(), PayloadBuilderError>
    where
        EvmConfig: ConfigureEvm<Header = Header>,
        Client: StateProviderFactory + ChainSpecProvider<ChainSpec = OpChainSpec>,
        Pool: TransactionPool<Transaction: PoolTransaction<Consensus = OpTransactionSigned>>,
        Txs: OpPayloadTransactions,
    {
        let EvmEnv {
            cfg_env_with_handler_cfg,
            block_env,
        } = cfg_and_block_env(
            evm_config,
            &args.config.attributes,
            &args.config.parent_header,
        )
        .map_err(PayloadBuilderError::other)?;
        let BuildArguments {
            client,
            pool,
            config,
            cancel,
            ..
        } = args;

        let ctx = OpPayloadBuilderCtx {
            chain_spec: client.chain_spec(),
            config,
            initialized_block_env: block_env,
            cancel,
            builder_signer: None,
            metrics: Default::default(),
        };

        let state_provider = client.state_by_block_hash(ctx.parent().hash())?;
        let state = StateProviderDatabase::new(&state_provider);
        let mut db = State::builder()
            .with_database(state)
            .with_bundle_update()
            .build();
        let mut executor = OpTxExecutor::new(
            ctx.initialized_block_env.clone(),
            cfg_env_with_handler_cfg.clone(),
            ctx.chain_spec.clone(),
            evm_config,
            &mut db,
            ctx.attributes().timestamp(),
        );

        // 1. apply eip-4788 pre block contract call
        // 2. ensure create2deployer is force deployed
        self.pre_block_actions(&ctx, &mut executor)?;

        // 3. execute sequencer transactions
        let mut info = self.execute_sequencer_transactions(&ctx, &mut executor)?;
        let (payload, mut bundle_state) = build_block(executor.db_mut(), &ctx, &info)?;

        best_payload.set(payload.clone());
        message_sender
            .send(
                serde_json::to_string(&OpExecutionPayloadEnvelopeV3::from(payload))
                    .unwrap_or_default(),
            )
            .unwrap();

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
            // TODO(dshulyak) update cumulative gas used
            // or check if evm allows to update used state
            let mut executor = OpTxExecutor::new(
                ctx.initialized_block_env.clone(),
                cfg_env_with_handler_cfg.clone(),
                ctx.chain_spec.clone(),
                evm_config,
                &mut db,
                ctx.attributes().timestamp(),
            );

            let pool = pool.clone();
            self.execute_best_transactions(
                &ctx,
                &mut executor,
                &mut info,
                |attrs| {
                    #[allow(clippy::unit_arg)]
                    txs.best_transactions(pool, attrs)
                },
                total_gas_per_batch,
            )?;

            if ctx.cancel.is_cancelled() {
                tracing::info!(
                    target: "payload_builder",
                    "Job cancelled, stopping payload building",
                );
                // if the job was cancelled, stop
                return Ok(());
            }

            let (payload, new_bundle_state) = build_block(executor.db_mut(), &ctx, &info)?;

            best_payload.set(payload.clone());
            message_sender
                .send(
                    serde_json::to_string(&OpExecutionPayloadEnvelopeV3::from(payload))
                        .unwrap_or_default(),
                )
                .unwrap();

            bundle_state = new_bundle_state;
            total_gas_per_batch += gas_per_batch;
            flashblock_count += 1;

            std::thread::sleep(std::time::Duration::from_millis(250));
        }
    }
}

pub fn build_block<DB, P>(
    state: &mut State<DB>,
    ctx: &OpPayloadBuilderCtx,
    info: &ExecutionInfo,
) -> Result<(OpBuiltPayload, BundleState), PayloadBuilderError>
where
    DB: Database<Error = ProviderError> + AsRef<P>,
    P: StateRootProvider + HashedPostStateProvider,
{
    let withdrawals_root = ctx.commit_withdrawals(state)?;

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

/// Holds the state after execution
#[derive(Debug)]
pub struct ExecutedPayload {
    /// Tracked execution info
    pub info: ExecutionInfo,
    /// Withdrawal hash.
    pub withdrawals_root: Option<B256>,
}


#[derive(Debug, Clone)]
pub struct DefaultFlashblocksBuilder{}

impl_traits!(
    DefaultFlashblocksBuilder,
    OpPreBlockActionsV1,
    OpTransactionsActions,
    FlashblocksStrategy
);