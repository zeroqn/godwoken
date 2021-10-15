use anyhow::Result;
use gw_common::H256;
use gw_mem_pool::{traits::MemPoolErrorTxHandler, ErrorReceiptWithLog};
use rust_decimal::Decimal;
use smol::Task;
use sqlx::PgPool;

use crate::helper::{hex, parse_log, GwLog};

pub const MAX_RETURN_DATA: usize = 32;

pub struct ErrorReceiptIndexer {
    pool: PgPool,
}

impl ErrorReceiptIndexer {
    pub fn new(pool: PgPool) -> Self {
        ErrorReceiptIndexer { pool }
    }
}

impl MemPoolErrorTxHandler for ErrorReceiptIndexer {
    fn handle_receipt_with_log(&self, receipt_with_log: ErrorReceiptWithLog) -> Task<Result<()>> {
        let record = ErrorReceiptRecord::from(receipt_with_log);
        let pool = self.pool.clone();

        smol::spawn(async move {
            let mut db = pool.begin().await?;
            sqlx::query("INSERT INTO error_transactions (hash, block_number, transaction_index, cumulative_gas_used, gas_used, status_code, status_reason) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)")
            .bind(hex(record.tx_hash.as_slice())?)
            .bind(Decimal::from(record.block_number))
            .bind(record.transaction_index)
            .bind(Decimal::from(record.cumulative_gas_used))
            .bind(Decimal::from(record.gas_used))
            .bind(Decimal::from(record.status_code))
            .bind(record.status_reason.to_vec())
            .execute(&mut db)
            .await?;

            db.commit().await?;
            Ok(())
        })
    }
}

struct ErrorReceiptRecord {
    tx_hash: H256,
    block_number: u64,
    transaction_index: u32,
    cumulative_gas_used: u64,
    gas_used: u64,
    status_code: u32,
    status_reason: [u8; 32],
}

impl From<ErrorReceiptWithLog> for ErrorReceiptRecord {
    fn from(receipt: ErrorReceiptWithLog) -> Self {
        let basic_record = ErrorReceiptRecord {
            tx_hash: receipt.tx_hash,
            block_number: receipt.block_number,
            transaction_index: receipt.transaction_index,
            cumulative_gas_used: 0,
            gas_used: 0,
            status_code: 0,
            status_reason: {
                let mut reason = [0u8; 32];
                reason.copy_from_slice(&receipt.return_data[..MAX_RETURN_DATA]);
                reason
            },
        };

        match receipt.last_log.map(|log| parse_log(&log)).transpose() {
            Ok(Some(GwLog::PolyjuiceSystem {
                gas_used,
                cumulative_gas_used,
                created_address: _,
                status_code,
            })) => ErrorReceiptRecord {
                gas_used,
                cumulative_gas_used,
                status_code,
                ..basic_record
            },
            Err(err) => {
                log::error!("[error receipt]: parse log error {}", err);
                basic_record
            }
            _ => basic_record,
        }
    }
}
