use alloy_consensus::Header;
use alloy_consensus::Transaction;
use alloy_consensus::{transaction::Recovered, Eip658Value};
use alloy_primitives::{Address, U256};
use op_alloy_consensus::{OpDepositReceipt, OpTxType};
use reth_evm::ConfigureEvm;
use reth_optimism_payload_builder::error::OpPayloadBuilderError;
use reth_optimism_primitives::{OpReceipt, OpTransactionSigned};
use reth_provider::ProviderError;
use revm::primitives::AccountInfo;
use revm::primitives::EVMError;
use revm::{
    primitives::{
        BlockEnv, CfgEnvWithHandlerCfg, EnvWithHandlerCfg, EvmState, ExecutionResult,
        ResultAndState, TxEnv,
    },
    Database, DatabaseCommit, Evm, State,
};

pub struct Executor<
    'executor,
    EvmConfig: ConfigureEvm<Header = Header, Transaction = OpTransactionSigned>,
    DB: Database,
> {
    config: &'executor EvmConfig,
    evm: Evm<'executor, EvmConfig::DefaultExternalContext<'executor>, &'executor mut State<DB>>,
    total_gas_used: u64,
    base_fee: u64,
}

impl<'executor, EvmConfig, DB> Executor<'executor, EvmConfig, DB>
where
    EvmConfig: ConfigureEvm<Header = Header, Transaction = OpTransactionSigned>,
    DB: Database<Error = ProviderError>,
{
    pub fn new<'a>(
        initialized_cfg: CfgEnvWithHandlerCfg,
        config: &'a EvmConfig,
        block_env: BlockEnv,
        state: &'a mut State<DB>,
        base_fee: u64,
    ) -> Executor<'a, EvmConfig, DB> {
        let env = EnvWithHandlerCfg::new_with_cfg_env(initialized_cfg, block_env, TxEnv::default());
        let evm = config.evm_with_env(state, env);
        Executor {
            config: config,
            evm,
            total_gas_used: 0,
            base_fee,
        }
    }

    pub fn execute<'call>(
        &'call mut self,
        tx: Recovered<OpTransactionSigned>,
    ) -> Result<(UncommittedTx<'executor, 'call, EvmConfig, DB>, ExecutedTx), EVMError<DB::Error>>
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
            .map_err(|_| OpPayloadBuilderError::AccountLoadFailed(tx.signer()))
            .expect("err");
        *self.evm.tx_mut() = self.config.tx_env(tx.tx(), tx.signer());
        let ResultAndState { result, state } = self.evm.transact()?;
        self.total_gas_used += result.gas_used();
        Ok((
            UncommittedTx {
                evm: &mut self.evm,
                state,
            },
            ExecutedTx {
                base_fee: self.base_fee,
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
        self.evm.db_mut().commit(self.state);
    }
}

pub struct ExecutedTx {
    total_gas_used: u64,
    base_fee: u64,
    tx: Recovered<OpTransactionSigned>,
    account: Option<AccountInfo>,
    result: ExecutionResult,
}

impl ExecutedTx {
    pub fn gas_used(&self) -> u64 {
        self.result.gas_used()
    }

    pub fn is_success(&self) -> bool {
        self.result.is_success()
    }

    pub fn tx(&self) -> &Recovered<OpTransactionSigned> {
        &self.tx
    }

    pub fn info(self) -> TxExecutionInfo {
        let gas_used = self.gas_used();
        let receipt = alloy_consensus::Receipt {
            status: Eip658Value::Eip658(self.is_success()),
            cumulative_gas_used: self.total_gas_used,
            logs: self.result.into_logs(),
        };
        let receipt = match self.tx.tx_type() {
            OpTxType::Legacy => OpReceipt::Legacy(receipt),
            OpTxType::Eip2930 => OpReceipt::Eip2930(receipt),
            OpTxType::Eip1559 => OpReceipt::Eip1559(receipt),
            OpTxType::Eip7702 => OpReceipt::Eip7702(receipt),
            OpTxType::Deposit => OpReceipt::Deposit(OpDepositReceipt {
                inner: receipt,
                deposit_nonce: self.account.map(|account| account.nonce),
                deposit_receipt_version: None,
            }),
        };
        let fee = self
            .tx
            .effective_tip_per_gas(self.base_fee)
            .expect("fee is always valid; execution succeeded");
        let sender = self.tx.signer();
        TxExecutionInfo {
            tx: self.tx.into_tx(),
            sender,
            receipt,
            gas_used: gas_used,
            fee: U256::from(fee) * U256::from(gas_used),
        }
    }
}

pub struct TxExecutionInfo {
    /// The transaction that was executed.
    pub tx: OpTransactionSigned,
    /// The sender of the transaction.
    pub sender: Address,
    /// The receipt of the transaction.
    pub receipt: OpReceipt,
    /// The gas used by the transaction.
    pub gas_used: u64,
    /// The fee paid by the transaction.
    pub fee: U256,
}

impl From<ExecutedTx> for TxExecutionInfo {
    fn from(executed: ExecutedTx) -> Self {
        executed.info()
    }
}
