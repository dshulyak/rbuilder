use std::fmt::Debug;
use std::{fmt::Display, sync::Arc, time::Instant};

use crate::generator::BuildArguments;
use crate::tx_executor::{
    OpExecutionResult, OpTxExecutor, StateAccess, TxExecutionInfo, TxExecutor,
};
use crate::{
    generator::{BlockCell, PayloadBuilder},
    metrics::OpRBuilderMetrics,
    tx_signer::Signer,
};

use alloy_consensus::{Header, Transaction, TxEip1559, Typed2718, EMPTY_OMMER_ROOT_HASH};
use alloy_eips::merge::BEACON_NONCE;
use alloy_primitives::{Address, Bytes, TxKind, B256, U256};
use alloy_rpc_types_engine::PayloadId;
use alloy_rpc_types_eth::Withdrawals;
use op_alloy_consensus::{EIP1559ParamError, OpTypedTransaction};
use reth::core::primitives::InMemorySize;
use reth_basic_payload_builder::*;
use reth_chain_state::ExecutedBlock;
use reth_chainspec::{ChainSpecProvider, EthereumHardforks};
use reth_evm::ConfigureEvmEnv;
use reth_evm::{env::EvmEnv, NextBlockEnvAttributes};
use reth_execution_types::ExecutionOutcome;
use reth_optimism_chainspec::OpChainSpec;
use reth_optimism_consensus::calculate_receipt_root_no_memo_optimism;
use reth_optimism_evm::OpEvmConfig;
use reth_optimism_forks::OpHardforks;
use reth_optimism_payload_builder::{
    error::OpPayloadBuilderError,
    payload::{OpBuiltPayload, OpPayloadBuilderAttributes},
};
use reth_optimism_primitives::{OpPrimitives, OpReceipt, OpTransactionSigned};
use reth_payload_builder_primitives::PayloadBuilderError;
use reth_payload_primitives::PayloadBuilderAttributes;
use reth_payload_util::PayloadTransactions;
use reth_primitives::{
    proofs, transaction::SignedTransactionIntoRecoveredExt, Block, BlockBody, BlockExt,
    SealedHeader, TxType,
};
use reth_provider::{
    HashedPostStateProvider, ProviderError, StateProviderFactory, StateRootProvider,
};
use reth_revm::database::StateProviderDatabase;
use reth_transaction_pool::pool::BestPayloadTransactions;
use reth_transaction_pool::PoolTransaction;
use reth_transaction_pool::{BestTransactionsAttributes, TransactionPool};
use revm::interpreter::check;
use revm::primitives::{EVMError, InvalidTransaction};
use revm::State;
use revm::{db::states::bundle_state::BundleRetention, primitives::BlockEnv, Database};
use tokio_util::sync::CancellationToken;
use tracing::{info, trace, warn};

/// Optimism's payload builder
#[derive(Debug, Clone)]
pub struct OpPayloadBuilderVanilla<Strategy, Txs = BestPoolTransactions> {
    /// The type responsible for creating the evm.
    pub evm_config: OpEvmConfig,
    /// The builder's signer key to use for an end of block tx
    pub builder_signer: Option<Signer>,
    /// The type responsible for yielding the best transactions for the payload if mempool
    /// transactions are allowed.
    pub best_transactions: Txs,
    /// The metrics for the builder
    pub metrics: OpRBuilderMetrics,
    // Strategy to use for building payload.
    pub strategy: Strategy,
}

impl<Strategy> OpPayloadBuilderVanilla<Strategy> {
    /// `OpPayloadBuilder` constructor.
    pub fn new(
        evm_config: OpEvmConfig,
        builder_signer: Option<Signer>,
        strategy: Strategy,
    ) -> Self {
        Self {
            evm_config,
            builder_signer,
            best_transactions: BestPoolTransactions,
            metrics: Default::default(),
            strategy,
        }
    }
}

impl<Strategy, Pool, Client> PayloadBuilder<Pool, Client> for OpPayloadBuilderVanilla<Strategy>
where
    Client: StateProviderFactory + ChainSpecProvider<ChainSpec = OpChainSpec>,
    Pool: TransactionPool<Transaction: PoolTransaction<Consensus = OpTransactionSigned>>,
    Strategy: OpBuilderStrategy + Clone + Send + Sync + 'static,
{
    type Attributes = OpPayloadBuilderAttributes;
    type BuiltPayload = OpBuiltPayload;

    fn try_build(
        &self,
        args: BuildArguments<Pool, Client, Self::Attributes>,
        best_payload: BlockCell<Self::BuiltPayload>,
    ) -> Result<(), PayloadBuilderError> {
        let pool = args.pool.clone();
        let block_build_start_time = Instant::now();

        match self.build_payload(
            args,
            |attrs| {
                #[allow(clippy::unit_arg)]
                self.best_transactions.best_transactions(pool, attrs)
            },
            self.strategy.clone(),
        )? {
            BuildOutcome::Better { payload, .. } => {
                best_payload.set(payload);
                self.metrics
                    .total_block_built_duration
                    .record(block_build_start_time.elapsed());
                self.metrics.block_built_success.increment(1);
                Ok(())
            }
            BuildOutcome::Freeze(payload) => {
                best_payload.set(payload);
                self.metrics
                    .total_block_built_duration
                    .record(block_build_start_time.elapsed());
                Ok(())
            }
            BuildOutcome::Cancelled => {
                tracing::warn!("Payload build cancelled");
                Err(PayloadBuilderError::MissingPayload)
            }
            _ => {
                tracing::warn!("No better payload found");
                Err(PayloadBuilderError::MissingPayload)
            }
        }
    }
}

impl<Strategy> OpPayloadBuilderVanilla<Strategy>
where
    Strategy: OpBuilderStrategy,
{
    /// Constructs an Optimism payload from the transactions sent via the
    /// Payload attributes by the sequencer. If the `no_tx_pool` argument is passed in
    /// the payload attributes, the transaction pool will be ignored and the only transactions
    /// included in the payload will be those sent through the attributes.
    ///
    /// Given build arguments including an Optimism client, transaction pool,
    /// and configuration, this function creates a transaction payload. Returns
    /// a result indicating success with the payload or an error in case of failure.
    fn build_payload<'a, Client, Pool, Txs>(
        &self,
        args: BuildArguments<Pool, Client, OpPayloadBuilderAttributes>,
        best: impl FnOnce(BestTransactionsAttributes) -> Txs + Send + Sync + 'a,
        strategy: Strategy,
    ) -> Result<BuildOutcome<OpBuiltPayload>, PayloadBuilderError>
    where
        Client: StateProviderFactory + ChainSpecProvider<ChainSpec = OpChainSpec>,
        Pool: TransactionPool,
        Txs: PayloadTransactions<Transaction = OpTransactionSigned>,
    {
        let evm_env = self
            .cfg_and_block_env(&args.config.attributes, &args.config.parent_header)
            .map_err(PayloadBuilderError::other)?;
        let EvmEnv {
            cfg_env_with_handler_cfg,
            block_env,
        } = evm_env;

        let BuildArguments {
            client,
            pool: _,
            mut cached_reads,
            config,
            cancel,
        } = args;

        let ctx = OpPayloadBuilderCtx {
            chain_spec: client.chain_spec(),
            config,
            initialized_block_env: block_env,
            cancel,
            builder_signer: self.builder_signer,
            metrics: Default::default(),
        };
        let state = StateProviderDatabase::new(client.state_by_block_hash(ctx.parent().hash())?);

        if ctx.attributes().no_tx_pool {
            let mut state = State::builder()
                .with_database(state)
                .with_bundle_update()
                .build();
            let executor = OpTxExecutor::new(
                ctx.initialized_block_env.clone(),
                cfg_env_with_handler_cfg,
                &self.evm_config,
                &mut state,
            );
            strategy.execute(ctx, executor, best)
        } else {
            // sequencer mode we can reuse cachedreads from previous runs
            let mut state = State::builder()
                .with_database(cached_reads.as_db_mut(state))
                .with_bundle_update()
                .build();
            let executor = OpTxExecutor::new(
                ctx.initialized_block_env.clone(),
                cfg_env_with_handler_cfg,
                &self.evm_config,
                &mut state,
            );
            strategy.execute(ctx, executor, best)
        }
        .map(|out| out.with_cached_reads(cached_reads))
    }
}

impl<Txs> OpPayloadBuilderVanilla<Txs> {
    /// Returns the configured [`EvmEnv`] for the targeted payload
    /// (that has the `parent` as its parent).
    pub fn cfg_and_block_env(
        &self,
        attributes: &OpPayloadBuilderAttributes,
        parent: &Header,
    ) -> Result<EvmEnv, EIP1559ParamError> {
        let next_attributes: NextBlockEnvAttributes = NextBlockEnvAttributes {
            timestamp: attributes.timestamp(),
            suggested_fee_recipient: attributes.suggested_fee_recipient(),
            prev_randao: attributes.prev_randao(),
            gas_limit: attributes.gas_limit.unwrap_or(parent.gas_limit),
        };
        self.evm_config
            .next_cfg_and_block_env(parent, next_attributes)
    }
}


#[macro_export]
macro_rules! check {
    ($cond:expr, $rst:expr) => {
        if $cond {
            return $rst;
        }
    };
}

macro_rules! skip {
    // Pattern for expression and block
    ($condition:expr, $block:block) => {
        if $condition {
            $block
            continue;
        }
    };

    // Pattern for just expression
    ($condition:expr, $expr:expr) => {
        if $condition {
            $expr;
            continue;
        }
    };

    // Pattern for condition only
    ($condition:expr) => {
        if $condition {
            continue;
        }
    };
}

/// The strategy that builds the payload.
///
/// Payload building for optimism is composed of several steps.
/// The first steps are mandatory and defined by the protocol.
///
/// 1. first all System calls are applied.
/// 2. After canyon the forced deployed `create2deployer` must be loaded
/// 3. all sequencer transactions are executed (part of the payload attributes)
///
/// Depending on whether the node acts as a sequencer and is allowed to include additional
/// transactions (`no_tx_pool == false`):
/// 4. include additional transactions
///
/// And finally
/// 5. build the block: compute all roots (txs, state)
pub trait OpBuilderStrategy {
    /// Executes the payload and returns the outcome.
    fn execute<'a, Executor, DB, P, Txs, TxsFn>(
        &self,
        ctx: OpPayloadBuilderCtx,
        mut executor: Executor,
        best: TxsFn,
    ) -> Result<BuildOutcomeKind<OpBuiltPayload>, PayloadBuilderError>
    where
        Executor: TxExecutor<
                'a,
                DB,
                Transaction = OpTransactionSigned,
                ExecutionResult = OpExecutionResult,
            > + StateAccess<'a, DB>,
        TxsFn: FnOnce(BestTransactionsAttributes) -> Txs + Send + Sync + 'a,
        Txs: PayloadTransactions<Transaction = OpTransactionSigned>,
        DB: Database<Error = ProviderError> + AsRef<P>,
        P: StateRootProvider + HashedPostStateProvider,
    {
        info!(target: "payload_builder", id=%ctx.payload_id(), parent_header = ?ctx.parent().hash(), parent_number = ctx.parent().number, "building new payload");

        // 1. apply eip-4788 pre block contract call
        // 2. ensure create2deployer is force deployed
        self.apply_pre_block(&ctx, &mut executor)?;

        // 3. execute sequencer transactions
        let mut info = self.execute_sequencer_transactions(&ctx, &mut executor)?;

        // gas reserved for builder tx
        let message = self.block_message(&ctx);
        let builder_tx_gas = ctx
            .builder_signer()
            .map_or(0, |_| estimate_gas_for_builder_tx(&message));
        let block_gas_limit = ctx.block_gas_limit() - builder_tx_gas;

        // 4. if mem pool transactions are requested we execute them
        if !ctx.attributes().no_tx_pool {
            check!(
                self.execute_best_transactions(
                    &ctx,
                    &mut executor,
                    &mut info,
                    best,
                    block_gas_limit
                )?
                .is_some(),
                Ok(BuildOutcomeKind::Cancelled)
            );
        }

        // Add builder tx to the block
        self.add_builder_tx(&ctx, &mut executor, &mut info, builder_tx_gas, message);
        if ctx.attributes().no_tx_pool {
            Ok(BuildOutcomeKind::Freeze(self.create_payload(
                executor.db_mut(),
                ctx,
                info,
            )?))
        } else {
            Ok(BuildOutcomeKind::Better {
                payload: self.create_payload(executor.db_mut(), ctx, info)?,
            })
        }
    }

    fn block_message(&self, ctx: &OpPayloadBuilderCtx) -> Vec<u8> {
        format!("Block Number: {}", ctx.block_number())
            .as_bytes()
            .to_vec()
    }

    /// Builds the payload on top of the state.
    fn create_payload<DB, P>(
        &self,
        state: &mut State<DB>,
        ctx: OpPayloadBuilderCtx,
        info: ExecutionInfo,
    ) -> Result<OpBuiltPayload, PayloadBuilderError>
    where
        DB: Database<Error = ProviderError> + AsRef<P>,
        P: StateRootProvider + HashedPostStateProvider,
    {
        let withdrawals_root = ctx.commit_withdrawals(state)?;

        let state_merge_start_time = Instant::now();
        // merge all transitions into bundle state, this would apply the withdrawal balance changes
        // and 4788 contract call
        state.merge_transitions(BundleRetention::Reverts);
        ctx.metrics
            .state_transition_merge_duration
            .record(state_merge_start_time.elapsed());
        ctx.metrics
            .payload_num_tx
            .record(info.executed_transactions.len() as f64);

        let block_number = ctx.block_number();
        let execution_outcome = ExecutionOutcome::new(
            state.take_bundle(),
            info.receipts.into(),
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

        // calculate the state root
        let state_root_start_time = Instant::now();

        let state_provider = state.database.as_ref();
        let hashed_state = state_provider.hashed_post_state(execution_outcome.state());
        let (state_root, trie_output) = {
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

        ctx.metrics
            .state_root_calculation_duration
            .record(state_root_start_time.elapsed());

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

        // seal the block
        let block = Block {
            header,
            body: BlockBody {
                transactions: info.executed_transactions,
                ommers: vec![],
                withdrawals: ctx.withdrawals().cloned(),
            },
        };

        let sealed_block = Arc::new(block.seal_slow());
        info!(target: "payload_builder", id=%ctx.attributes().payload_id(), sealed_block_header = ?sealed_block.header, "sealed built block");

        // create the executed block data
        let executed: ExecutedBlock<OpPrimitives> = ExecutedBlock {
            block: sealed_block.clone(),
            senders: Arc::new(info.executed_senders),
            execution_output: Arc::new(execution_outcome),
            hashed_state: Arc::new(hashed_state),
            trie: Arc::new(trie_output),
        };
        let payload = OpBuiltPayload::new(
            ctx.payload_id(),
            sealed_block,
            info.total_fees,
            ctx.chain_spec.clone(),
            ctx.config.attributes,
            Some(executed),
        );
        ctx.metrics
            .payload_byte_size
            .record(payload.block().size() as f64);
        Ok(payload)
    }

    /// Apply eip-4788 pre block contract call
    /// and
    /// Ensure that the create2deployer is force-deployed at the canyon transition. Optimism
    /// blocks will always have at least a single transaction in them (the L1 info transaction),
    /// so we can safely assume that this will always be triggered upon the transition and that
    /// the above check for empty blocks will never be hit on OP chains.
    fn apply_pre_block<'a, State, DB>(
        &self,
        ctx: &OpPayloadBuilderCtx,
        state: &mut State,
    ) -> Result<(), PayloadBuilderError>
    where
        DB: Database,
        DB::Error: Display,
        State: StateAccess<'a, DB>,
    {
        // SystemCaller::new(self.evm_config.clone(), self.chain_spec.clone())
        //     .pre_block_beacon_root_contract_call(
        //         db,
        //         &self.initialized_cfg,
        //         &self.initialized_block_env,
        //         self.attributes()
        //             .payload_attributes
        //             .parent_beacon_block_root,
        //     )
        //     .map_err(|err| {
        //         warn!(target: "payload_builder",
        //             parent_header=%self.parent().hash(),
        //             %err,
        //             "failed to apply beacon root contract call for payload"
        //         );
        //         PayloadBuilderError::Internal(err.into())
        //     })?;
        reth_optimism_evm::ensure_create2_deployer(
            ctx.chain_spec.clone(),
            ctx.attributes().payload_attributes.timestamp,
            state.db_mut(),
        )
        .map_err(|err| {
            warn!(target: "payload_builder", %err, "missing create2 deployer, skipping block.");
            PayloadBuilderError::other(OpPayloadBuilderError::ForceCreate2DeployerFail)
        })?;
        Ok(())
    }

    /// Executes all sequencer transactions that are included in the payload attributes.
    fn execute_sequencer_transactions<'a, Executor, DB>(
        &self,
        ctx: &OpPayloadBuilderCtx,
        executor: &mut Executor,
    ) -> Result<ExecutionInfo, PayloadBuilderError>
    where
        DB: Database<Error = ProviderError>,
        Executor: TxExecutor<
            'a,
            DB,
            Transaction = OpTransactionSigned,
            ExecutionResult = OpExecutionResult,
        >,
    {
        let sequencer_tx_start_time = Instant::now();
        let mut info = ExecutionInfo::with_capacity(ctx.attributes().transactions.len());

        for sequencer_tx in &ctx.attributes().transactions {
            // A sequencer's block should never contain blob transactions.
            check!(
                sequencer_tx.value().is_eip4844(),
                Err(PayloadBuilderError::other(
                    OpPayloadBuilderError::BlobTransactionRejected,
                ))
            );

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

            executor.execute(&tx).map_or_else(|err| {
                match err {
                    EVMError::Transaction(err) => {
                        trace!(target: "payload_builder", %err, ?tx, "Error in sequencer transaction, skipping.");
                        Ok(())
                    }
                    err => {
                        // this is an error that we should treat as fatal for this attempt
                        Err(PayloadBuilderError::EvmExecutionError(err))
                    }
                }
            },|rst| {
                Ok(())
            })?;
        }
        ctx.metrics
            .sequencer_tx_duration
            .record(sequencer_tx_start_time.elapsed());
        Ok(info)
    }

    /// Executes the given best transactions and updates the execution info.
    ///
    /// Returns `Ok(Some(())` if the job was cancelled.
    fn execute_best_transactions<'a, Executor, DB, Txs, TxsFn>(
        &self,
        ctx: &OpPayloadBuilderCtx,
        executor: &mut Executor,
        info: &mut ExecutionInfo,
        best_txs: TxsFn,
        block_gas_limit: u64,
    ) -> Result<Option<()>, PayloadBuilderError>
    where
        DB: Database<Error = ProviderError>,
        TxsFn: FnOnce(BestTransactionsAttributes) -> Txs + Send + Sync + 'a,
        Txs: PayloadTransactions<Transaction = OpTransactionSigned>,
        Executor: TxExecutor<
            'a,
            DB,
            Transaction = OpTransactionSigned,
            ExecutionResult = OpExecutionResult,
        >,
    {
        let best_txs_start_time = Instant::now();
        let mut best_txs = best_txs(ctx.best_transaction_attributes());
        ctx.metrics
            .transaction_pool_fetch_duration
            .record(best_txs_start_time.elapsed());

        let execute_txs_start_time = Instant::now();
        let mut num_txs_considered = 0;
        let mut num_txs_simulated = 0;
        let mut num_txs_simulated_success = 0;
        let mut num_txs_simulated_fail = 0;
        let base_fee = ctx.base_fee();

        while let Some(tx) = best_txs.next(()) {
            check!(ctx.cancel.is_cancelled(), Ok(Some(())));
            skip!(info.executed_transactions.contains(&tx));

            num_txs_considered += 1;

            // ensure we still have capacity for this transaction
            // we can't fit this transaction into the block, so we need to mark it as
            // invalid which also removes all dependent transaction from
            // the iterator before we can continue
            skip!(
                info.cumulative_gas_used + tx.gas_limit() > block_gas_limit,
                best_txs.mark_invalid(tx.signer(), tx.nonce())
            );
            // a sequencer's block should never contain blob or deposit transactions from the pool.
            skip!(
                tx.is_eip4844() || tx.tx_type() == TxType::Deposit as u8,
                best_txs.mark_invalid(tx.signer(), tx.nonce())
            );

            let tx_simulation_start_time = Instant::now();

            match executor.transact(&tx) {
                Ok((result, state)) => {
                    ctx.metrics
                        .tx_simulation_duration
                        .record(tx_simulation_start_time.elapsed());
                    ctx.metrics.tx_byte_size.record(tx.size() as f64);
                    num_txs_simulated += 1;
                    if result.is_success() {
                        num_txs_simulated_success += 1;
                    } else {
                        num_txs_simulated_fail += 1;
                    }
                    ctx.metrics
                        .payload_num_tx_simulated
                        .record(num_txs_simulated as f64);
                    ctx.metrics
                        .payload_num_tx_simulated_success
                        .record(num_txs_simulated_success as f64);
                    ctx.metrics
                        .payload_num_tx_simulated_fail
                        .record(num_txs_simulated_fail as f64);

                    executor.commit(state);
                    let receipt = result.receipt();
                    // info.add(executed.into());
                }
                Err(err) => {
                    match err {
                        EVMError::Transaction(err) => {
                            if matches!(err, InvalidTransaction::NonceTooLow { .. }) {
                                // if the nonce is too low, we can skip this transaction
                                trace!(target: "payload_builder", %err, ?tx, "skipping nonce too low transaction");
                            } else {
                                // if the transaction is invalid, we can skip it and all of its
                                // descendants
                                trace!(target: "payload_builder", %err, ?tx, "skipping invalid transaction and its descendants");
                                best_txs.mark_invalid(tx.signer(), tx.nonce());
                            }
                        }
                        err => {
                            // this is an error that we should treat as fatal for this attempt
                            return Err(PayloadBuilderError::EvmExecutionError(err));
                        }
                    }
                }
            };
        }

        ctx.metrics
            .payload_tx_simulation_duration
            .record(execute_txs_start_time.elapsed());
        ctx.metrics
            .payload_num_tx_considered
            .record(num_txs_considered as f64);

        Ok(None)
    }

    fn add_builder_tx<'a, Executor, DB>(
        &self,
        ctx: &OpPayloadBuilderCtx,
        executor: &mut Executor,
        info: &mut ExecutionInfo,
        builder_tx_gas: u64,
        message: Vec<u8>,
    ) -> Option<()>
    where
        DB: Database<Error = ProviderError>,
        Executor: TxExecutor<
                'a,
                DB,
                Transaction = OpTransactionSigned,
                ExecutionResult = OpExecutionResult,
            > + StateAccess<'a, DB>,
    {
        ctx.builder_signer()
            .map(|signer| {
                let base_fee = ctx.base_fee();
                // Create message with block number for the builder to sign
                let nonce = executor
                    .db_mut()
                    .load_cache_account(signer.address)
                    .map(|acc| acc.account_info().unwrap_or_default().nonce)
                    .map_err(|_| {
                        PayloadBuilderError::other(OpPayloadBuilderError::AccountLoadFailed(
                            signer.address,
                        ))
                    })?;

                // Create the EIP-1559 transaction
                let eip1559 = OpTypedTransaction::Eip1559(TxEip1559 {
                    chain_id: ctx.chain_id(),
                    nonce,
                    gas_limit: builder_tx_gas,
                    max_fee_per_gas: base_fee.into(),
                    max_priority_fee_per_gas: 0,
                    to: TxKind::Call(Address::ZERO),
                    // Include the message as part of the transaction data
                    input: message.into(),
                    ..Default::default()
                });

                // Sign the transaction
                let builder_tx = signer
                    .sign_tx(eip1559)
                    .map_err(PayloadBuilderError::other)?;

                let receipt = executor
                    .execute(&builder_tx)
                    .map_err(PayloadBuilderError::EvmExecutionError)?;

                // Release the db reference by dropping evm
                // NOTE(dshulyak) why it was necessary?
                // drop(executor);
                // info.add(executed.into());
                Ok(())
            })
            .transpose()
            .unwrap_or_else(|err: PayloadBuilderError| {
                warn!(target: "payload_builder", %err, "Failed to add builder transaction");
                None
            })
    }
}

#[derive(Debug, Clone)]
pub struct DefaultOpBuilderStrategy;

impl OpBuilderStrategy for DefaultOpBuilderStrategy {}

#[derive(Debug, Clone)]
pub struct FlashblocksBuilderStrategy;

impl OpBuilderStrategy for FlashblocksBuilderStrategy {}

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

/// BestPoolTransactions will yield best transactions from the pool.
#[derive(Clone, Debug)]
pub struct BestPoolTransactions;

impl OpPayloadTransactions for BestPoolTransactions {
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
pub struct OpPayloadBuilderCtx {
    /// The chainspec
    pub chain_spec: Arc<OpChainSpec>,
    /// How to build the payload.
    pub config: PayloadConfig<OpPayloadBuilderAttributes>,
    /// Block config
    pub initialized_block_env: BlockEnv,
    /// Marker to check whether the job has been cancelled.
    pub cancel: CancellationToken,
    /// The builder signer
    pub builder_signer: Option<Signer>,
    /// The metrics for the builder
    pub metrics: OpRBuilderMetrics,
}

impl OpPayloadBuilderCtx {
    /// Returns the parent block the payload will be build on.
    pub fn parent(&self) -> &SealedHeader {
        &self.config.parent_header
    }

    /// Returns the builder attributes.
    pub const fn attributes(&self) -> &OpPayloadBuilderAttributes {
        &self.config.attributes
    }

    /// Returns the withdrawals if shanghai is active.
    pub fn withdrawals(&self) -> Option<&Withdrawals> {
        self.chain_spec
            .is_shanghai_active_at_timestamp(self.attributes().timestamp())
            .then(|| &self.attributes().payload_attributes.withdrawals)
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

    /// Returns the chain id
    pub fn chain_id(&self) -> u64 {
        self.chain_spec.chain.id()
    }

    /// Returns the builder signer
    pub fn builder_signer(&self) -> Option<Signer> {
        self.builder_signer
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
}

fn estimate_gas_for_builder_tx(input: impl AsRef<[u8]>) -> u64 {
    // Count zero and non-zero bytes
    let (zero_bytes, nonzero_bytes) =
        input
            .as_ref()
            .iter()
            .fold((0, 0), |(zeros, nonzeros), &byte| {
                if byte == 0 {
                    (zeros + 1, nonzeros)
                } else {
                    (zeros, nonzeros + 1)
                }
            });

    // Calculate gas cost (4 gas per zero byte, 16 gas per non-zero byte)
    let zero_cost = zero_bytes * 4;
    let nonzero_cost = nonzero_bytes * 16;

    zero_cost + nonzero_cost + 21_000
}
