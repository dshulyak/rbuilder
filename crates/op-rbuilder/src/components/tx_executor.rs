use std::fmt::Display;
use std::sync::Arc;

use alloy_consensus::{transaction::Recovered, Eip658Value};
use alloy_primitives::{Address, FixedBytes};
use op_alloy_consensus::{OpDepositReceipt, OpTxType};
use reth_evm::{
    execute::BlockExecutionError, system_calls::SystemCaller, ConfigureEvm as RethConfigureEvm,
};
use reth_node_api::PayloadBuilderError;
use reth_optimism_chainspec::OpChainSpec;
use reth_optimism_forks::OpHardforks;
use reth_optimism_primitives::{OpReceipt, OpTransactionSigned};
use reth_primitives_traits::FillTxEnv;
use reth_provider::ProviderError;
use revm::{
    primitives::{
        CfgEnvWithHandlerCfg, EnvWithHandlerCfg, EvmState, ExecutionResult, ResultAndState, TxEnv,
    },
    Database as RevmDatabase, DatabaseCommit, Evm, State,
};

// this trait should not be imported here
use crate::actions::pre_block::PreBlockRootContractSyscall;

// aliases give an option to fix the breaking change in the contract
// sort of a delay, if trait/type becomes unstable change can be made

pub type BlockEnv = revm::primitives::BlockEnv;
pub type EVMError<Error> = revm::primitives::EVMError<Error>;

pub trait ConfigureEvm: RethConfigureEvm {}
impl<T: RethConfigureEvm> ConfigureEvm for T {}

pub trait Database: RevmDatabase {}
impl<T: RevmDatabase> Database for T {}

pub trait StateAccess<'state, DB: Database> {
    fn db_mut(&mut self) -> &'_ mut State<DB>;
}

pub trait TxExecutor<'state, DB: Database> {
    type Transaction;
    type ExecutionResult;
    type State;
    type Error;

    fn execute(&mut self, tx: &Self::Transaction) -> Result<Self::ExecutionResult, Self::Error> {
        let (result, state) = self.transact(tx)?;
        self.commit(state);
        Ok(result)
    }

    fn transact(
        &mut self,
        tx: &Self::Transaction,
    ) -> Result<(Self::ExecutionResult, Self::State), Self::Error>;

    fn commit(&mut self, state: Self::State);
}

pub struct OpTxExecutor<'state, DB: Database + 'state, EvmConfig: ConfigureEvm> {
    chain_spec: Arc<OpChainSpec>,
    evm: Evm<'state, EvmConfig::DefaultExternalContext<'state>, &'state mut State<DB>>,
    evm_config: &'state EvmConfig,
    initialized_cfg: CfgEnvWithHandlerCfg,
    initialized_block_env: BlockEnv,
    cumulative_gas_used: u64,
    payload_timestamp: u64,
}

impl<'state, DB: Database, EvmConfig: ConfigureEvm> OpTxExecutor<'state, DB, EvmConfig> {
    pub fn new<'a>(
        initialized_block_env: BlockEnv,
        initialized_cfg: CfgEnvWithHandlerCfg,
        chain_spec: Arc<OpChainSpec>,
        evm_config: &'a EvmConfig,
        state: &'a mut State<DB>,
        payload_timestamp: u64,
    ) -> OpTxExecutor<'a, DB, EvmConfig> {
        let env = EnvWithHandlerCfg::new_with_cfg_env(
            initialized_cfg.clone(),
            initialized_block_env.clone(),
            TxEnv::default(),
        );
        let evm = evm_config.evm_with_env(state, env);
        OpTxExecutor {
            chain_spec,
            evm,
            evm_config,
            initialized_cfg,
            initialized_block_env,
            cumulative_gas_used: 0,
            payload_timestamp,
        }
    }
}

impl<'state, DB: Database, EvmConfig: ConfigureEvm> StateAccess<'state, DB>
    for OpTxExecutor<'state, DB, EvmConfig>
{
    fn db_mut(&mut self) -> &'_ mut State<DB> {
        self.evm.db_mut()
    }
}

impl<'state, DB, EvmConfig> PreBlockRootContractSyscall for OpTxExecutor<'state, DB, EvmConfig>
where
    DB: Database,
    DB::Error: Display,
    EvmConfig: ConfigureEvm,
{
    fn pre_block_root_contract_syscall(
        &mut self,
        parent_block_root: Option<FixedBytes<32>>,
    ) -> Result<(), BlockExecutionError> {
        SystemCaller::new(self.evm_config.clone(), self.chain_spec.clone())
            .pre_block_beacon_root_contract_call(
                self.evm.db_mut(),
                &self.initialized_cfg,
                &self.initialized_block_env,
                parent_block_root,
            )
    }
}

impl<'state, DB: Database, EvmConfig: ConfigureEvm> TxExecutor<'state, DB>
    for OpTxExecutor<'state, DB, EvmConfig>
{
    type Transaction = Recovered<OpTransactionSigned>;
    type ExecutionResult = OpExecutionResult;
    type State = EvmState;
    type Error = OpTxExecutorError<DB::Error>;

    fn transact(
        &mut self,
        tx: &Self::Transaction,
    ) -> Result<(Self::ExecutionResult, Self::State), Self::Error> {
        // Cache the depositor account prior to the state transition for the deposit nonce.
        //
        // Note that this *only* needs to be done post-regolith hardfork, as deposit nonces
        // were not introduced in Bedrock. In addition, regular transactions don't have deposit
        // nonces, so we don't need to touch the DB for those.
        let depositor_nonce = (self.is_regolith_active() && tx.is_deposit())
            .then(|| {
                self.evm
                    .db_mut()
                    .load_cache_account(tx.signer())
                    .map(|acc| acc.account_info().unwrap_or_default().nonce)
            })
            .transpose()
            .map_err(|_| OpTxExecutorError::AccountLoad(tx.signer()))?;

        let mut tx_env = TxEnv::default();
        tx.tx().fill_tx_env(&mut tx_env, tx.signer());
        *self.evm.tx_mut() = tx_env;
        let ResultAndState { result, state } = self.evm.transact()?;
        self.cumulative_gas_used += result.gas_used();
        Ok((
            OpExecutionResult {
                cumulative_gas_used: self.cumulative_gas_used,
                tx_type: tx.tx().tx_type(),
                result,
                nonce: depositor_nonce,
                canyon_active: self.is_canyon_active(),
            },
            state,
        ))
    }

    fn commit(&mut self, state: Self::State) {
        self.evm.db_mut().commit(state);
    }
}

pub struct OpExecutionResult {
    // nonce is non empty only if transaction was a deposit
    nonce: Option<u64>,
    cumulative_gas_used: u64,
    tx_type: OpTxType,
    canyon_active: bool,
    result: ExecutionResult,
}

impl OpExecutionResult {
    pub fn gas_used(&self) -> u64 {
        self.result.gas_used()
    }

    pub fn is_success(&self) -> bool {
        self.result.is_success()
    }

    pub fn receipt(mut self) -> OpReceipt {
        let receipt = alloy_consensus::Receipt {
            status: Eip658Value::Eip658(self.is_success()),
            cumulative_gas_used: self.cumulative_gas_used,
            logs: self.result.into_logs(),
        };
        match self.tx_type {
            OpTxType::Legacy => OpReceipt::Legacy(receipt),
            OpTxType::Eip2930 => OpReceipt::Eip2930(receipt),
            OpTxType::Eip1559 => OpReceipt::Eip1559(receipt),
            OpTxType::Eip7702 => OpReceipt::Eip7702(receipt),
            OpTxType::Deposit => OpReceipt::Deposit(OpDepositReceipt {
                inner: receipt,
                deposit_nonce: self.nonce.take(),
                deposit_receipt_version: self.canyon_active.then_some(1),
            }),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum OpTxExecutorError<DBError> {
    #[error("failed to load account {0}")]
    AccountLoad(Address),
    #[error(transparent)]
    EVMError(#[from] EVMError<DBError>),
}

impl From<OpTxExecutorError<ProviderError>> for PayloadBuilderError {
    fn from(err: OpTxExecutorError<ProviderError>) -> Self {
        match err {
            err @ OpTxExecutorError::AccountLoad(_) => PayloadBuilderError::Other(Box::new(err)),
            OpTxExecutorError::EVMError(err) => PayloadBuilderError::EvmExecutionError(err),
        }
    }
}

impl<'_a, DB: Database, EvmConfig: ConfigureEvm> OpTxExecutor<'_a, DB, EvmConfig> {
    fn is_regolith_active(&self) -> bool {
        self.chain_spec
            .is_regolith_active_at_timestamp(self.payload_timestamp)
    }

    fn is_canyon_active(&self) -> bool {
        self.chain_spec
            .is_canyon_active_at_timestamp(self.payload_timestamp)
    }
}
