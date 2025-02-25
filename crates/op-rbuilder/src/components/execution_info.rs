use std::fmt::Debug;

use alloy_consensus::{transaction::Recovered, Transaction};
use alloy_primitives::{Address, U256};
use reth_optimism_primitives::{OpReceipt, OpTransactionSigned};

use crate::components::tx_executor::OpExecutionResult;


/// This acts as the container for executed transactions and its byproducts (receipts, gas used)
#[derive(Default, Debug)]
pub struct ExecutionInfo {
    /// All executed transactions (unrecovered).
    pub executed_transactions: Vec<OpTransactionSigned>,
    /// The recovered senders for the executed transactions.
    pub executed_senders: Vec<Address>,
    /// The transaction receipts
    pub receipts: Vec<OpReceipt>,
    /// All gas used so far
    pub cumulative_gas_used: u64,
    /// Tracks fees from executed mempool transactions
    pub total_fees: U256,
}

impl ExecutionInfo {
    /// Create a new instance with allocated slots.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            executed_transactions: Vec::with_capacity(capacity),
            executed_senders: Vec::with_capacity(capacity),
            receipts: Vec::with_capacity(capacity),
            cumulative_gas_used: 0,
            total_fees: U256::ZERO,
        }
    }

    pub fn add(
        &mut self,
        tx: Recovered<OpTransactionSigned>,
        result: OpExecutionResult,
        base_fee: u64,
    ) {
        self.executed_senders.push(tx.signer());
        self.cumulative_gas_used += result.gas_used();
        let miner_fee = tx
            .effective_tip_per_gas(base_fee)
            .expect("fee is always valid; execution succeeded");
        self.total_fees += U256::from(miner_fee) * U256::from(result.gas_used());
        self.executed_transactions.push(tx.into_tx());
        self.receipts.push(result.receipt());
    }
}