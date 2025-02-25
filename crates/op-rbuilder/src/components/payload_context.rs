use std::fmt::Debug;
use std::sync::Arc;

use crate::components::tx_executor::{BlockEnv, Database};
use crate::{metrics::OpRBuilderMetrics, tx_signer::Signer};

use alloy_primitives::{Bytes, B256};
use alloy_rpc_types_engine::PayloadId;
use alloy_rpc_types_eth::Withdrawals;
use reth_basic_payload_builder::*;
use reth_chainspec::EthereumHardforks;
use reth_optimism_chainspec::OpChainSpec;
use reth_optimism_forks::OpHardforks;
use reth_optimism_payload_builder::payload::OpPayloadBuilderAttributes;
use reth_payload_builder_primitives::PayloadBuilderError;
use reth_payload_primitives::PayloadBuilderAttributes;
use reth_primitives::SealedHeader;
use reth_provider::ProviderError;
use reth_transaction_pool::BestTransactionsAttributes;
use revm::State;
use tokio_util::sync::CancellationToken;

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

    /// Returns true if ecotone is active for the payload.
    pub fn is_ecotone_active(&self) -> bool {
        self.chain_spec
            .is_ecotone_active_at_timestamp(self.attributes().timestamp())
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
