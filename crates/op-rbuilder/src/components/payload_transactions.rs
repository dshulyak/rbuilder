use std::fmt::Debug;

use reth_optimism_primitives::OpTransactionSigned;
use reth_payload_util::PayloadTransactions;
use reth_transaction_pool::pool::BestPayloadTransactions;
use reth_transaction_pool::PoolTransaction;
use reth_transaction_pool::{BestTransactionsAttributes, TransactionPool};

/// A type that returns a the [`PayloadTransactions`] that should be included in the pool.
pub trait OpPayloadTransactions: Clone + Send + Sync + Unpin + 'static {
    /// Returns an iterator that yields the transaction in the order they should get included in the
    /// new payload.
    fn best_transactions<
        Pool: TransactionPool<Transaction: PoolTransaction<Consensus = OpTransactionSigned>>,
    >(
        &self,
        pool: Pool,
        attr: BestTransactionsAttributes,
    ) -> impl PayloadTransactions<Transaction = OpTransactionSigned>;
}

/// BestPoolTransactions will yield best transactions from the pool.
#[derive(Clone, Debug)]
pub struct BestPoolTransactions;

impl OpPayloadTransactions for BestPoolTransactions {
    fn best_transactions<
        Pool: TransactionPool<Transaction: PoolTransaction<Consensus = OpTransactionSigned>>,
    >(
        &self,
        pool: Pool,
        attr: BestTransactionsAttributes,
    ) -> impl PayloadTransactions<Transaction = OpTransactionSigned> {
        BestPayloadTransactions::new(pool.best_transactions_with_attributes(attr))
    }
}
