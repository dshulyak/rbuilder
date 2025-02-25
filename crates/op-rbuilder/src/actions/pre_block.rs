use std::fmt::Display;
use std::sync::Arc;

use alloy_primitives::FixedBytes;
use reth_evm::execute::BlockExecutionError;
use reth_node_api::PayloadBuilderError;
use reth_optimism_chainspec::OpChainSpec;
use reth_optimism_evm::ensure_create2_deployer;
use reth_optimism_payload_builder::error::OpPayloadBuilderError;
use revm::{Database, State};
use tracing::warn;

use crate::components::{payload_context::OpPayloadBuilderCtx, tx_executor::StateAccess};

/// Ensures that the CREATE2 deployer exists in the state.
///
/// This is a pre-block action that must be performed before building a block.
pub fn ensure_create2_deployer_exists<DB>(
    chain_spec: Arc<OpChainSpec>,
    timestamp: u64,
    state: &mut State<DB>,
) -> Result<(), PayloadBuilderError>
where
    DB: Database,
    DB::Error: std::fmt::Display,
{
    ensure_create2_deployer(chain_spec, timestamp, state).map_err(|err| {
        warn!(target: "payload_builder", %err, "missing create2 deployer, skipping block.");
        PayloadBuilderError::other(OpPayloadBuilderError::ForceCreate2DeployerFail)
    })
}

pub trait PreBlockRootContractSyscall {
    fn pre_block_root_contract_syscall(
        &mut self,
        parent_block_root: Option<FixedBytes<32>>,
    ) -> Result<(), BlockExecutionError>;
}

/// Trait for handling pre-block actions in payload building
pub trait OpPreBlockActions {
    /// Apply eip-4788 pre block contract call
    /// and
    /// Ensure that the create2deployer is force-deployed at the canyon transition. Optimism
    /// blocks will always have at least a single transaction in them (the L1 info transaction),
    /// so we can safely assume that this will always be triggered upon the transition and that
    /// the above check for empty blocks will never be hit on OP chains.
    fn pre_block_actions<'a, Executor, DB>(
        &self,
        ctx: &OpPayloadBuilderCtx,
        executor: &mut Executor,
    ) -> Result<(), PayloadBuilderError>
    where
        Executor: StateAccess<'a, DB> + PreBlockRootContractSyscall,
        DB: Database,
        DB::Error: Display,
    {
        executor
            .pre_block_root_contract_syscall(
                ctx.attributes().payload_attributes.parent_beacon_block_root,
            )
            .map_err(|err| {
                warn!(target: "payload_builder",
                    parent_header=%ctx.parent().hash(),
                    %err,
                    "failed to apply beacon root contract call for payload"
                );
                PayloadBuilderError::Internal(err.into())
            })?;
        ensure_create2_deployer_exists(
            ctx.chain_spec.clone(),
            ctx.attributes().payload_attributes.timestamp,
            executor.db_mut(),
        )?;
        Ok(())
    }
}
