use std::{sync::Arc, time::Instant};

use alloy_consensus::{Header, EMPTY_OMMER_ROOT_HASH};
use alloy_eips::merge::BEACON_NONCE;
use alloy_primitives::U256;
use reth::core::primitives::InMemorySize;
use reth_chain_state::ExecutedBlock;
use reth_node_api::PayloadBuilderError;
use reth_optimism_consensus::calculate_receipt_root_no_memo_optimism;
use reth_optimism_node::OpBuiltPayload;
use reth_optimism_primitives::OpPrimitives;
use reth_payload_primitives::PayloadBuilderAttributes;
use reth_primitives::{proofs, Block, BlockBody, BlockExt};
use reth_provider::{ExecutionOutcome, HashedPostStateProvider, ProviderError, StateRootProvider};
use revm::{db::states::bundle_state::BundleRetention, State};
use tracing::{info, warn};

use crate::components::{
    execution_info::ExecutionInfo, payload_context::OpPayloadBuilderCtx, tx_executor::Database,
};

pub trait OpCreatePayloadAction {
    /// Builds the payload on top of the state.
    fn create_payload<DB, P>(
        &self,
        state: &mut State<DB>,
        ctx: &OpPayloadBuilderCtx,
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
            ctx.config.attributes.clone(),
            Some(executed),
        );
        ctx.metrics
            .payload_byte_size
            .record(payload.block().size() as f64);
        Ok(payload)
    }
}
