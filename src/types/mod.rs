pub mod type_utils;

use std::collections::HashMap;

use accumulators::mmr::Proof;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize, Serializer};

#[derive(Debug, Deserialize)]
pub struct Transaction {
    pub hash: String,
    #[serde(rename(deserialize = "blockNumber"))]
    pub block_number: String,
    #[serde(rename(deserialize = "transactionIndex"))]
    pub transaction_index: String,
    pub value: String,
    #[serde(rename(deserialize = "gasPrice"))]
    pub gas_price: String,
    pub gas: String,
    pub from: Option<String>,
    pub to: Option<String>,
    #[serde(rename(deserialize = "maxPriorityFeePerGas"))]
    pub max_priority_fee_per_gas: Option<String>,
    #[serde(rename(deserialize = "maxFeePerGas"))]
    pub max_fee_per_gas: Option<String>,
    #[serde(rename(deserialize = "chainId"))]
    pub chain_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct BlockHeaderWithEmptyTransaction {
    #[serde(rename(deserialize = "gasLimit"))]
    pub gas_limit: String,
    #[serde(rename(deserialize = "gasUsed"))]
    pub gas_used: String,
    #[serde(rename(deserialize = "baseFeePerGas"))]
    pub base_fee_per_gas: Option<String>,
    pub hash: String,
    pub nonce: Option<String>,
    pub number: String,
    #[serde(rename(deserialize = "receiptsRoot"))]
    pub receipts_root: String,
    #[serde(rename(deserialize = "stateRoot"))]
    pub state_root: String,
    #[serde(rename(deserialize = "transactionsRoot"))]
    pub transactions_root: String,
    #[serde(rename(deserialize = "parentHash"))]
    pub parent_hash: Option<String>,
    #[serde(rename(deserialize = "miner"))]
    pub miner: Option<String>,
    #[serde(rename(deserialize = "logsBloom"))]
    pub logs_bloom: Option<String>,
    #[serde(rename(deserialize = "difficulty"))]
    pub difficulty: Option<String>,
    #[serde(rename(deserialize = "totalDifficulty"))]
    pub total_difficulty: Option<String>,
    #[serde(rename(deserialize = "sha3Uncles"))]
    pub sha3_uncles: Option<String>,
    #[serde(rename(deserialize = "timestamp"))]
    pub timestamp: String,
    #[serde(rename(deserialize = "extraData"))]
    pub extra_data: Option<String>,
    #[serde(rename(deserialize = "mixHash"))]
    pub mix_hash: Option<String>,
    #[serde(rename(deserialize = "withdrawalsRoot"))]
    pub withdrawals_root: Option<String>,
    #[serde(rename(deserialize = "blobGasUsed"))]
    pub blob_gas_used: Option<String>,
    #[serde(rename(deserialize = "excessBlobGas"))]
    pub excess_blob_gas: Option<String>,
    #[serde(rename(deserialize = "parentBeaconBlockRoot"))]
    pub parent_beacon_block_root: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct BlockHeaderWithFullTransaction {
    #[serde(rename(deserialize = "gasLimit"))]
    pub gas_limit: String,
    #[serde(rename(deserialize = "gasUsed"))]
    pub gas_used: String,
    #[serde(rename(deserialize = "baseFeePerGas"))]
    pub base_fee_per_gas: Option<String>,
    pub hash: String,
    pub nonce: Option<String>,
    pub number: String,
    #[serde(rename(deserialize = "receiptsRoot"))]
    pub receipts_root: String,
    #[serde(rename(deserialize = "stateRoot"))]
    pub state_root: String,
    #[serde(rename(deserialize = "transactionsRoot"))]
    pub transactions_root: String,
    pub transactions: Vec<Transaction>,
    #[serde(rename(deserialize = "parentHash"))]
    pub parent_hash: Option<String>,
    #[serde(rename(deserialize = "miner"))]
    pub miner: Option<String>,
    #[serde(rename(deserialize = "logsBloom"))]
    pub logs_bloom: Option<String>,
    #[serde(rename(deserialize = "difficulty"))]
    pub difficulty: Option<String>,
    #[serde(rename(deserialize = "totalDifficulty"))]
    pub total_difficulty: Option<String>,
    #[serde(rename(deserialize = "sha3Uncles"))]
    pub sha3_uncles: Option<String>,
    #[serde(rename(deserialize = "timestamp"))]
    pub timestamp: String,
    #[serde(rename(deserialize = "extraData"))]
    pub extra_data: Option<String>,
    #[serde(rename(deserialize = "mixHash"))]
    pub mix_hash: Option<String>,
    #[serde(rename(deserialize = "withdrawalsRoot"))]
    pub withdrawals_root: Option<String>,
    #[serde(rename(deserialize = "blobGasUsed"))]
    pub blob_gas_used: Option<String>,
    #[serde(rename(deserialize = "excessBlobGas"))]
    pub excess_blob_gas: Option<String>,
    #[serde(rename(deserialize = "parentBeaconBlockRoot"))]
    pub parent_beacon_block_root: Option<String>,
}

#[derive(Clone, Debug, sqlx::FromRow)]
pub struct BlockDetails {
    pub block_hash: String,
    pub number: i64,
}

#[derive(Clone, Serialize)]
pub struct Update {
    pub latest_blocknumber: i64,
    pub latest_roothash: String,
    pub update_timestamp: DateTime<Utc>,
}

pub struct ProofWrapper {
    pub proof: Proof,
}

impl Serialize for ProofWrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = HashMap::new();

        // Convert to owned String values
        let element_index = self.proof.element_index.to_string();
        let element_hash = self.proof.element_hash.to_string();
        let siblings_hashes = serde_json::to_string(&self.proof.siblings_hashes)
            .map_err(serde::ser::Error::custom)?;
        let peaks_hashes =
            serde_json::to_string(&self.proof.peaks_hashes).map_err(serde::ser::Error::custom)?;
        let elements_count = self.proof.elements_count.to_string();

        // Insert owned values into the map
        map.insert("element_index", element_index);
        map.insert("element_hash", element_hash);
        map.insert("sibling_hashes", siblings_hashes);
        map.insert("peaks_hashes", peaks_hashes);
        map.insert("elements_count", elements_count);

        map.serialize(serializer)
    }
}
