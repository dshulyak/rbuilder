use std::time::Instant;

use alloy_consensus::{transaction::Recovered, Transaction, Typed2718};
use reth::core::primitives::InMemorySize;
use reth_optimism_payload_builder::error::OpPayloadBuilderError;
use reth_optimism_primitives::OpTransactionSigned;
use reth_payload_builder_primitives::PayloadBuilderError;
use reth_payload_util::PayloadTransactions;
use reth_primitives::transaction::SignedTransactionIntoRecoveredExt;
use reth_primitives::TxType;
use reth_provider::ProviderError;
use reth_transaction_pool::BestTransactionsAttributes;
use revm::primitives::{EVMError, InvalidTransaction};
use tracing::trace;

use crate::components::{
    execution_info::ExecutionInfo,
    payload_context::OpPayloadBuilderCtx,
    tx_executor::{Database, OpExecutionResult, OpTxExecutorError, TxExecutor},
};
use crate::{check, skip};

pub trait OpTransactionsActions {
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
            Transaction = Recovered<OpTransactionSigned>,
            ExecutionResult = OpExecutionResult,
            Error = OpTxExecutorError<DB::Error>,
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
                })?
                .into();
            match executor.execute(&tx) {
                Ok(rst) => {
                    info.add(tx, rst, 0);
                }
                Err(OpTxExecutorError::EVMError(EVMError::Transaction(err))) => {
                    trace!(target: "payload_builder", %err, ?tx, "Error in sequencer transaction, skipping.");
                }
                err => {
                    err?;
                }
            }
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
            Transaction = Recovered<OpTransactionSigned>,
            ExecutionResult = OpExecutionResult,
            Error = OpTxExecutorError<DB::Error>,
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
            let tx = tx.into();
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
                    info.add(tx.into(), result, base_fee);
                }
                Err(OpTxExecutorError::EVMError(EVMError::Transaction(err))) => {
                    if matches!(err, InvalidTransaction::NonceTooLow { .. }) {
                        // if the nonce is too low, we can skip this transaction
                        trace!(target: "payload_builder", %err, ?tx, "skipping nonce too low transaction");
                    } else {
                        // if the transaction is invalid, we can skip it and all of its descendants
                        trace!(target: "payload_builder", %err, ?tx, "skipping invalid transaction and its descendants");
                        best_txs.mark_invalid(tx.signer(), tx.nonce());
                    }
                }
                err => {
                    err?;
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
}
