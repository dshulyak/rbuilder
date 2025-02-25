use std::fmt::Debug;
use std::time::Instant;

use alloy_consensus::{transaction::Recovered, Header};
use reth_basic_payload_builder::*;
use reth_chainspec::ChainSpecProvider;
use reth_evm::{env::EvmEnv, NextBlockEnvAttributes};
use reth_optimism_chainspec::OpChainSpec;
use reth_optimism_payload_builder::payload::{OpBuiltPayload, OpPayloadBuilderAttributes};
use reth_optimism_primitives::OpTransactionSigned;
use reth_payload_builder_primitives::PayloadBuilderError;
use reth_payload_primitives::PayloadBuilderAttributes;
use reth_payload_util::PayloadTransactions;
use reth_provider::{
    HashedPostStateProvider, ProviderError, StateProviderFactory, StateRootProvider,
};
use reth_revm::database::StateProviderDatabase;
use reth_transaction_pool::PoolTransaction;
use reth_transaction_pool::{BestTransactionsAttributes, TransactionPool};
use revm::State;
use tracing::info;

use crate::actions::builder_tx::DefaultBuilderTxActions;
use crate::actions::payload::OpCreatePayloadAction;
use crate::actions::pre_block::{OpPreBlockActions, PreBlockRootContractSyscall};
use crate::actions::tx_executor::OpTransactionsActions;
use crate::components::payload_context::OpPayloadBuilderCtx;
use crate::components::payload_transactions::{BestPoolTransactions, OpPayloadTransactions};
use crate::components::tx_executor::{
    ConfigureEvm, Database, OpExecutionResult, OpTxExecutor, OpTxExecutorError, StateAccess,
    TxExecutor,
};
use crate::generator::BuildArguments;
use crate::{check, impl_traits};
use crate::{
    generator::{BlockCell, PayloadBuilder},
    metrics::OpRBuilderMetrics,
    tx_signer::Signer,
};

/// Optimism's payload builder
#[derive(Debug, Clone)]
pub struct OpPayloadBuilderVanilla<EvmConfig, Strategy, Txs = BestPoolTransactions> {
    /// The type responsible for creating the evm.
    pub evm_config: EvmConfig,
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

impl<EvmConfig, Strategy> OpPayloadBuilderVanilla<EvmConfig, Strategy> {
    /// `OpPayloadBuilder` constructor.
    pub fn new(evm_config: EvmConfig, builder_signer: Option<Signer>, strategy: Strategy) -> Self {
        Self {
            evm_config,
            builder_signer,
            best_transactions: BestPoolTransactions,
            metrics: Default::default(),
            strategy,
        }
    }
}

impl<EvmConfig, Strategy, Pool, Client> PayloadBuilder<Pool, Client>
    for OpPayloadBuilderVanilla<EvmConfig, Strategy>
where
    Client: StateProviderFactory + ChainSpecProvider<ChainSpec = OpChainSpec>,
    Pool: TransactionPool<Transaction: PoolTransaction<Consensus = OpTransactionSigned>>,
    EvmConfig: ConfigureEvm<Header = Header>,
    Strategy: VanillaStrategy + Clone + Send + Sync + 'static,
{
    type Attributes = OpPayloadBuilderAttributes;
    type BuiltPayload = OpBuiltPayload;

    fn try_build(
        &self,
        args: BuildArguments<Pool, Client, Self::Attributes>,
        best_payload: BlockCell<Self::BuiltPayload>,
    ) -> Result<(), PayloadBuilderError> {
        self.strategy.build(
            args,
            best_payload,
            &self.evm_config,
            self.builder_signer,
            &self.metrics,
            self.best_transactions.clone(),
        )
    }
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
pub trait VanillaStrategy:
    OpPreBlockActions + DefaultBuilderTxActions + OpTransactionsActions + OpCreatePayloadAction
{
    /// Constructs an Optimism payload from the transactions sent via the
    /// Payload attributes by the sequencer. If the `no_tx_pool` argument is passed in
    /// the payload attributes, the transaction pool will be ignored and the only transactions
    /// included in the payload will be those sent through the attributes.
    ///
    /// Given build arguments including an Optimism client, transaction pool,
    /// and configuration, this function creates a transaction payload. Returns
    /// a result indicating success with the payload or an error in case of failure.
    fn build<'a, Pool, Client, EvmConfig, Txs>(
        &self,
        args: BuildArguments<Pool, Client, OpPayloadBuilderAttributes>,
        best_payload: BlockCell<OpBuiltPayload>,
        evm_config: &'a EvmConfig,
        builder_signer: Option<Signer>,
        metrics: &'a OpRBuilderMetrics,
        txs: Txs,
    ) -> Result<(), PayloadBuilderError>
    where
        Client: StateProviderFactory + ChainSpecProvider<ChainSpec = OpChainSpec>,
        Pool: TransactionPool<Transaction: PoolTransaction<Consensus = OpTransactionSigned>>,
        EvmConfig: ConfigureEvm<Header = Header>,
        Txs: OpPayloadTransactions,
    {
        let pool = args.pool.clone();
        let block_build_start_time = Instant::now();

        match build_payload(
            builder_signer,
            evm_config,
            args,
            |attrs| {
                #[allow(clippy::unit_arg)]
                txs.best_transactions(pool, attrs)
            },
            self,
        )? {
            BuildOutcome::Better { payload, .. } => {
                best_payload.set(payload);
                metrics
                    .total_block_built_duration
                    .record(block_build_start_time.elapsed());
                metrics.block_built_success.increment(1);
                Ok(())
            }
            BuildOutcome::Freeze(payload) => {
                best_payload.set(payload);
                metrics
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

/// Returns the configured [`EvmEnv`] for the targeted payload
/// (that has the `parent` as its parent).
pub fn cfg_and_block_env<EvmConfig>(
    evm_config: &EvmConfig,
    attributes: &OpPayloadBuilderAttributes,
    parent: &EvmConfig::Header,
) -> Result<EvmEnv, EvmConfig::Error>
where
    EvmConfig: ConfigureEvm<Header = Header>,
{
    let next_attributes: NextBlockEnvAttributes = NextBlockEnvAttributes {
        timestamp: attributes.timestamp(),
        suggested_fee_recipient: attributes.suggested_fee_recipient(),
        prev_randao: attributes.prev_randao(),
        gas_limit: attributes.gas_limit.unwrap_or(parent.gas_limit),
    };
    evm_config.next_cfg_and_block_env(parent, next_attributes)
}

fn build_payload<'a, Client, Pool, Txs, Strategy, EvmConfig>(
    builder_signer: Option<Signer>,
    evm_config: &'a EvmConfig,
    args: BuildArguments<Pool, Client, OpPayloadBuilderAttributes>,
    best: impl FnOnce(BestTransactionsAttributes) -> Txs + Send + Sync + 'a,
    strategy: &'a Strategy,
) -> Result<BuildOutcome<OpBuiltPayload>, PayloadBuilderError>
where
    Client: StateProviderFactory + ChainSpecProvider<ChainSpec = OpChainSpec>,
    Pool: TransactionPool,
    Txs: PayloadTransactions<Transaction = OpTransactionSigned>,
    EvmConfig: ConfigureEvm<Header = Header>,
    Strategy: VanillaStrategy + ?Sized,
{
    let evm_env = cfg_and_block_env(
        evm_config,
        &args.config.attributes,
        &args.config.parent_header,
    )
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
        builder_signer: builder_signer,
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
            ctx.chain_spec.clone(),
            evm_config,
            &mut state,
            ctx.attributes().timestamp(),
        );
        execute(strategy, ctx, executor, best)
    } else {
        // sequencer mode we can reuse cachedreads from previous runs
        let mut state = State::builder()
            .with_database(cached_reads.as_db_mut(state))
            .with_bundle_update()
            .build();
        let executor = OpTxExecutor::new(
            ctx.initialized_block_env.clone(),
            cfg_env_with_handler_cfg,
            ctx.chain_spec.clone(),
            evm_config,
            &mut state,
            ctx.attributes().timestamp(),
        );
        execute(strategy, ctx, executor, best)
    }
    .map(|out| out.with_cached_reads(cached_reads))
}

fn execute<'a, Strategy, Executor, DB, P, Txs, TxsFn>(
    strategy: &'a Strategy,
    ctx: OpPayloadBuilderCtx,
    mut executor: Executor,
    best: TxsFn,
) -> Result<BuildOutcomeKind<OpBuiltPayload>, PayloadBuilderError>
where
    Strategy: VanillaStrategy + ?Sized,
    Executor: TxExecutor<
            'a,
            DB,
            Transaction = Recovered<OpTransactionSigned>,
            ExecutionResult = OpExecutionResult,
            Error = OpTxExecutorError<DB::Error>,
        > + StateAccess<'a, DB>
        + PreBlockRootContractSyscall,
    TxsFn: FnOnce(BestTransactionsAttributes) -> Txs + Send + Sync + 'a,
    Txs: PayloadTransactions<Transaction = OpTransactionSigned>,
    DB: Database<Error = ProviderError> + AsRef<P>,
    P: StateRootProvider + HashedPostStateProvider,
{
    info!(target: "payload_builder", id=%ctx.payload_id(), parent_header = ?ctx.parent().hash(), parent_number = ctx.parent().number, "building new payload");

    // 1. apply eip-4788 pre block contract call
    // 2. ensure create2deployer is force deployed
    strategy.pre_block_actions(&ctx, &mut executor)?;

    // 3. execute sequencer transactions
    let mut info = strategy.execute_sequencer_transactions(&ctx, &mut executor)?;

    // gas reserved for builder tx
    let message = strategy.block_message(&ctx);
    let builder_tx_gas = ctx
        .builder_signer()
        .map_or(0, |_| estimate_gas_for_builder_tx(&message));
    let block_gas_limit = ctx.block_gas_limit() - builder_tx_gas;

    // 4. if mem pool transactions are requested we execute them
    if !ctx.attributes().no_tx_pool {
        check!(
            strategy
                .execute_best_transactions(&ctx, &mut executor, &mut info, best, block_gas_limit)?
                .is_some(),
            Ok(BuildOutcomeKind::Cancelled)
        );
    }

    // Add builder tx to the block
    strategy.add_builder_tx(&ctx, &mut executor, &mut info, builder_tx_gas, message);
    if ctx.attributes().no_tx_pool {
        Ok(BuildOutcomeKind::Freeze(strategy.create_payload(
            executor.db_mut(),
            &ctx,
            info,
        )?))
    } else {
        Ok(BuildOutcomeKind::Better {
            payload: strategy.create_payload(executor.db_mut(), &ctx, info)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct DefaultVanillaStrategy;

impl_traits!(
    DefaultVanillaStrategy,
    OpPreBlockActions,
    OpTransactionsActions,
    OpCreatePayloadAction,
    DefaultBuilderTxActions,
    VanillaStrategy
);
