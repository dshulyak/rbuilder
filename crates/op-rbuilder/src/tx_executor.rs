use alloy_consensus::Header;
use alloy_consensus::{transaction::Recovered, Eip658Value};
use op_alloy_consensus::{OpDepositReceipt, OpTxType};
use reth_evm::ConfigureEvm;
use reth_optimism_payload_builder::error::OpPayloadBuilderError;
use reth_optimism_primitives::{OpReceipt, OpTransactionSigned};
use reth_provider::ProviderError;
use revm::primitives::AccountInfo;
use revm::{
    primitives::{
        BlockEnv, CfgEnvWithHandlerCfg, EnvWithHandlerCfg, EvmState, ExecutionResult,
        ResultAndState, TxEnv,
    },
    Database, DatabaseCommit, Evm, State,
};

#[derive(Debug)]
pub struct ExecutorEnv<EvmConfig> {
    initialized_cfg: CfgEnvWithHandlerCfg,
    config: EvmConfig,
}

impl<EvmConfig> ExecutorEnv<EvmConfig>
where
    EvmConfig: ConfigureEvm<Header = Header, Transaction = OpTransactionSigned>,
{
    pub fn new(initialized_cfg: CfgEnvWithHandlerCfg, config: EvmConfig) -> Self {
        Self {
            initialized_cfg,
            config,
        }
    }

    pub fn executor<'executor, DB: Database<Error = ProviderError>>(
        &'executor self,
        block_env: BlockEnv,
        state: &'executor mut State<DB>,
    ) -> Executor<'executor, EvmConfig, DB> {
        let env = EnvWithHandlerCfg::new_with_cfg_env(
            self.initialized_cfg.clone(),
            block_env,
            TxEnv::default(),
        );
        let evm = self.config.evm_with_env(state, env);
        Executor {
            config: &self.config,
            evm,
            total_gas_used: 0,
        }
    }
}

pub struct Executor<
    'executor,
    EvmConfig: ConfigureEvm<Header = Header, Transaction = OpTransactionSigned>,
    DB: Database,
> {
    config: &'executor EvmConfig,
    evm: Evm<'executor, EvmConfig::DefaultExternalContext<'executor>, &'executor mut State<DB>>,
    total_gas_used: u64,
}

impl<'executor, EvmConfig, DB> Executor<'executor, EvmConfig, DB>
where
    EvmConfig: ConfigureEvm<Header = Header, Transaction = OpTransactionSigned>,
    DB: Database<Error = ProviderError>,
{
    pub fn execute<'call>(
        &'call mut self,
        tx: &'call Recovered<OpTransactionSigned>,
    ) -> Result<(
        UncommittedTx<'executor, 'call, EvmConfig, DB>, 
        ExecutedTxInfo<'call>,
    ), Box<dyn std::error::Error>>
    {
        // Cache the depositor account prior to the state transition for the deposit nonce.
        //
        // Note that this *only* needs to be done post-regolith hardfork, as deposit nonces
        // were not introduced in Bedrock. In addition, regular transactions don't have deposit
        // nonces, so we don't need to touch the DB for those.
        let depositor = tx
            .is_deposit()
            .then(|| {
                self.evm
                    .db_mut()
                    .load_cache_account(tx.signer())
                    .map(|acc| acc.account_info().unwrap_or_default())
            })
            .transpose()
            .map_err(|_| OpPayloadBuilderError::AccountLoadFailed(tx.signer()))?;
        *self.evm.tx_mut() = self.config.tx_env(tx.tx(), tx.signer());
        let ResultAndState { result, state } = self.evm.transact().expect("no error");
        self.total_gas_used += result.gas_used();
        Ok((
            UncommittedTx {
                evm: &mut self.evm,
                state,
            },
            ExecutedTxInfo {
                total_gas_used: self.total_gas_used,
                tx,
                result,
                account: depositor,
            },
        ))
    }
}


pub struct UncommittedTx<
    'executor,
    'call,
    EvmConfig: ConfigureEvm<Header = Header, Transaction = OpTransactionSigned>,
    DB: Database,
> {
    evm: &'call mut Evm<
        'executor,
        EvmConfig::DefaultExternalContext<'executor>,
        &'executor mut State<DB>,
    >,
    state: EvmState,
}

impl<'executor, 'call, EvmConfig, DB> UncommittedTx<'executor, 'call, EvmConfig, DB>
where
    EvmConfig: ConfigureEvm<Header = Header, Transaction = OpTransactionSigned>,
    DB: Database,
{
    pub fn commit(self) {
        self.evm
            .db_mut()
            .commit(self.state);
    }
}

pub struct ExecutedTxInfo<'call> {
    total_gas_used: u64,
    tx: &'call Recovered<OpTransactionSigned>,
    account: Option<AccountInfo>,
    result: ExecutionResult,
}

impl<'call> ExecutedTxInfo<'call> {
    pub fn gas_used(&self) -> u64 {
        self.result.gas_used()
    }

    pub fn is_success(&self) -> bool {
        self.result.is_success()
    }

    pub fn receipt(self) -> OpReceipt {
        let receipt = alloy_consensus::Receipt {
            status: Eip658Value::Eip658(self.is_success()),
            cumulative_gas_used: self.total_gas_used,
            logs: self.result.into_logs(),
        };
        match self.tx.tx_type() {
            OpTxType::Legacy => OpReceipt::Legacy(receipt),
            OpTxType::Eip2930 => OpReceipt::Eip2930(receipt),
            OpTxType::Eip1559 => OpReceipt::Eip1559(receipt),
            OpTxType::Eip7702 => OpReceipt::Eip7702(receipt),
            OpTxType::Deposit => OpReceipt::Deposit(OpDepositReceipt {
                inner: receipt,
                deposit_nonce: self.account.map(|account| account.nonce),
                deposit_receipt_version: None,
            }),
        }
    }
}