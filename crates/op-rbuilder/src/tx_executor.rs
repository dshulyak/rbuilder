use alloy_consensus::Header;
use alloy_consensus::{transaction::Recovered, Eip658Value};
use op_alloy_consensus::{OpDepositReceipt, OpTxType};
use reth_evm::ConfigureEvm;
use reth_optimism_primitives::{OpReceipt, OpTransactionSigned};
use reth_provider::ProviderError;
use revm::{
    primitives::{
        BlockEnv, CfgEnvWithHandlerCfg, EnvWithHandlerCfg, EvmState, ExecutionResult,
        ResultAndState, TxEnv,
    },
    Database, DatabaseCommit, Evm, State,
};

pub struct ExecutorEnv<EvmConfig> {
    initialized_cfg: CfgEnvWithHandlerCfg,
    config: EvmConfig,
}

impl<EvmConfig> ExecutorEnv<EvmConfig>
where
    EvmConfig: ConfigureEvm<Header = Header, Transaction = OpTransactionSigned>,
{
    pub fn new<'a, DB: Database<Error = ProviderError>>(
        &'a self,
        block_env: BlockEnv,
        state: &'a mut State<DB>,
    ) -> Executor<'a, EvmConfig, DB> {
        let env = EnvWithHandlerCfg::new_with_cfg_env(
            self.initialized_cfg.clone(),
            block_env,
            TxEnv::default(),
        );
        let config = &self.config;
        let evm = self.config.evm_with_env(state, env);
        Executor {
            config,
            evm,
            total_gas_used: 0,
        }
    }
}

pub struct Executor<
    'a,
    EvmConfig: ConfigureEvm<Header = Header, Transaction = OpTransactionSigned>,
    DB: Database,
> {
    config: &'a EvmConfig,
    evm: Evm<'a, EvmConfig::DefaultExternalContext<'a>, &'a mut State<DB>>,
    total_gas_used: u64,
}

impl<'a, EvmConfig, DB> Executor<'a, EvmConfig, DB>
where
    EvmConfig: ConfigureEvm<Header = Header, Transaction = OpTransactionSigned>,
    DB: Database<Error = ProviderError>,
{
    pub fn execute(
        &'a mut self,
        tx: &'a Recovered<OpTransactionSigned>,
    ) -> Result<ExecutedTx<'a, EvmConfig, DB>, Box<dyn std::error::Error>> {
        *self.evm.tx_mut() = self.config.tx_env(tx.tx(), tx.signer());
        let ResultAndState { result, state } = self.evm.transact().expect("no error");
        self.total_gas_used += result.gas_used();
        Ok(ExecutedTx {
            executor: self,
            tx,
            result,
            state: Some(state),
        })
    }
}

pub struct ExecutedTx<
    'a,
    EvmConfig: ConfigureEvm<Header = Header, Transaction = OpTransactionSigned>,
    DB: Database,
> {
    executor: &'a mut Executor<'a, EvmConfig, DB>,
    tx: &'a Recovered<OpTransactionSigned>,
    result: ExecutionResult,
    state: Option<EvmState>,
}

impl<'a, EvmConfig, DB> ExecutedTx<'a, EvmConfig, DB>
where
    EvmConfig: ConfigureEvm<Header = Header, Transaction = OpTransactionSigned>,
    DB: Database,
{
    pub fn commit(&mut self) {
        self.executor
            .evm
            .db_mut()
            .commit(self.state.take().expect("state can be committed only once"));
    }

    pub fn gas_used(&self) -> u64 {
        self.result.gas_used()
    }

    pub fn is_success(&self) -> bool {
        self.result.is_success()
    }

    pub fn is_halted(&self) -> bool {
        self.result.is_halt()
    }

    pub fn is_revert(&self) -> bool {
        matches!(self.result, ExecutionResult::Revert { .. })
    }

    pub fn receipt(self) -> OpReceipt {
        let receipt = alloy_consensus::Receipt {
            status: Eip658Value::Eip658(self.is_success()),
            cumulative_gas_used: self.executor.total_gas_used,
            logs: self.result.into_logs(),
        };
        match self.tx.tx_type() {
            OpTxType::Legacy => OpReceipt::Legacy(receipt),
            OpTxType::Eip2930 => OpReceipt::Eip2930(receipt),
            OpTxType::Eip1559 => OpReceipt::Eip1559(receipt),
            OpTxType::Eip7702 => OpReceipt::Eip7702(receipt),
            OpTxType::Deposit => OpReceipt::Deposit(OpDepositReceipt {
                inner: receipt,
                deposit_nonce: None,
                deposit_receipt_version: None,
            }),
        }
    }
}
