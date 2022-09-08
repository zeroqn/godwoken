use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use gw_types::{
    offchain::{CellWithStatus, DepositInfo},
    packed::OutPoint,
};

#[async_trait]
pub trait MemPoolProvider {
    async fn estimate_next_blocktime(&self) -> Result<Duration>;
    async fn collect_deposit_cells(&self) -> Result<Vec<DepositInfo>>;
    async fn get_cell(&self, out_point: OutPoint) -> Result<Option<CellWithStatus>>;
}
