use alloy_consensus::{transaction::Recovered, TxEip1559};
use alloy_primitives::{Address, TxKind};
use op_alloy_consensus::OpTypedTransaction;
use reth_optimism_payload_builder::error::OpPayloadBuilderError;
use reth_optimism_primitives::OpTransactionSigned;
use reth_payload_builder_primitives::PayloadBuilderError;
use reth_provider::ProviderError;
use tracing::warn;

use crate::components::execution_info::ExecutionInfo;
use crate::components::payload_context::OpPayloadBuilderCtx;
use crate::components::tx_executor::{Database, OpExecutionResult, OpTxExecutorError, StateAccess, TxExecutor};


/// Trait for handling builder transaction related functionality
pub trait DefaultBuilderTxActions {
    /// Creates the message to include in the builder transaction
    fn block_message(&self, ctx: &OpPayloadBuilderCtx) -> Vec<u8> {
        format!("Block Number: {}", ctx.block_number())
            .as_bytes()
            .to_vec()
    }

    /// Adds a builder transaction to the block if a signer is configured
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
                Transaction = Recovered<OpTransactionSigned>,
                ExecutionResult = OpExecutionResult,
                Error = OpTxExecutorError<DB::Error>,
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
                    .map_err(PayloadBuilderError::other)?
                    .into();

                let result = executor.execute(&builder_tx)?;
                // note that base_fee is zero
                info.add(builder_tx, result, 0);
                // Release the db reference by dropping evm
                // NOTE(dshulyak) why it was necessary?
                // drop(executor);
                Ok(())
            })
            .transpose()
            .unwrap_or_else(|err: PayloadBuilderError| {
                warn!(target: "payload_builder", %err, "Failed to add builder transaction");
                None
            })
    }
}
