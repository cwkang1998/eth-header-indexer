//! # Ethereum RPC Client
//!
//! This module provides functionality for interacting with Ethereum JSON-RPC endpoints to fetch
//! blockchain data including block headers, transactions, and network state information.
//!
//! ## Key Features
//!
//! - **Resilient RPC Operations**: Built-in retry logic with exponential backoff
//! - **Batch Processing**: Efficient batch fetching of multiple blocks
//! - **Type Safety**: Strongly typed blockchain data structures
//! - **Async/Await Support**: Fully asynchronous operations using Tokio
//! - **Configurable Timeouts**: Per-request timeout configuration
//!
//! ## Architecture
//!
//! The module centers around the [`EthereumRpcProvider`] trait which defines the interface
//! for blockchain data fetching. The primary implementation is [`EthereumJsonRpcClient`]
//! which handles HTTP JSON-RPC communication with Ethereum nodes.
//!
//! ## Usage Examples
//!
//! ### Fetching Latest Block
//! ```rust,no_run
//! use fossil_headers_db::rpc::get_latest_finalized_blocknumber;
//!
//! # async fn example() -> eyre::Result<()> {
//! let latest_block = get_latest_finalized_blocknumber(Some(30)).await?;
//! println!("Latest finalized block: {}", latest_block.value());
//! # Ok(())
//! # }
//! ```
//!
//! ### Fetching Block Data
//! ```rust,no_run
//! use fossil_headers_db::rpc::get_full_block_by_number;
//! use fossil_headers_db::types::BlockNumber;
//!
//! # async fn example() -> eyre::Result<()> {
//! let block_number = BlockNumber::from_trusted(19000000);
//! let block_data = get_full_block_by_number(block_number, Some(30)).await?;
//! println!("Block hash: {}", block_data.hash);
//! # Ok(())
//! # }
//! ```

use crate::errors::{BlockchainError, Result};
use crate::types::BlockNumber;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;
use std::{future::Future, time::Duration};
use tokio::time::sleep;
use tracing::{error, warn};

#[cfg(test)]
use serial_test::serial;

// TODO: instead of keeping this static, make it passable as a dependency.
// This should allow us to test this module.
static CLIENT: OnceLock<Client> = OnceLock::new();
static NODE_CONNECTION_STRING: OnceLock<Option<String>> = OnceLock::new();

// Arbitrarily set, can be set somewhere else.
const MAX_RETRIES: u8 = 5;

#[derive(Serialize, Deserialize, Debug)]
pub struct RpcResponse<T> {
    pub result: T,
}

#[derive(Serialize, Deserialize, Debug)]
struct RpcRequest<'a, T> {
    jsonrpc: &'a str,
    id: String,
    method: &'a str,
    params: T,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Transaction {
    pub hash: String,
    #[serde(rename = "blockNumber")]
    pub block_number: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: String,
    pub value: String,
    #[serde(rename = "gasPrice")]
    pub gas_price: Option<String>,
    pub gas: String,
    pub from: Option<String>,
    pub to: Option<String>,
    #[serde(rename = "maxPriorityFeePerGas")]
    pub max_priority_fee_per_gas: Option<String>,
    #[serde(rename = "maxFeePerGas")]
    pub max_fee_per_gas: Option<String>,
    #[serde(rename = "chainId")]
    pub chain_id: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
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
    pub receipts_root: Option<String>,
    #[serde(rename(deserialize = "stateRoot"))]
    pub state_root: Option<String>,
    #[serde(rename(deserialize = "transactionsRoot"))]
    pub transactions_root: Option<String>,
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

#[derive(Debug, Deserialize, Clone)]
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
    pub receipts_root: Option<String>,
    #[serde(rename(deserialize = "stateRoot"))]
    pub state_root: Option<String>,
    #[serde(rename(deserialize = "transactionsRoot"))]
    pub transactions_root: Option<String>,
    pub transactions: Vec<Transaction>,
    #[serde(rename(deserialize = "parentHash"))]
    pub parent_hash: Option<String>,
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

/// Fetches the latest finalized block number from the Ethereum network.
///
/// This function queries the Ethereum node for the most recent block that has been
/// finalized by the network consensus mechanism. Finalized blocks are considered
/// safe from reorganization.
///
/// # Arguments
///
/// * `timeout` - Optional timeout in seconds for the RPC request. If `None`, uses default timeout.
///
/// # Returns
///
/// Returns a `Result<BlockNumber>` containing the latest finalized block number on success.
///
/// # Errors
///
/// This function will return an error if:
/// - The `NODE_CONNECTION_STRING` environment variable is not set
/// - The RPC request fails or times out
/// - The response cannot be parsed as a valid block number
/// - Network connectivity issues occur
///
/// # Examples
///
/// ```rust,no_run
/// use fossil_headers_db::rpc::get_latest_finalized_blocknumber;
///
/// # async fn example() -> eyre::Result<()> {
/// // Get latest finalized block with 30 second timeout
/// let latest_block = get_latest_finalized_blocknumber(Some(30)).await?;
/// println!("Latest finalized block: {}", latest_block.value());
///
/// // Use default timeout
/// let latest_block = get_latest_finalized_blocknumber(None).await?;
/// # Ok(())
/// # }
/// ```
pub async fn get_latest_finalized_blocknumber(timeout: Option<u64>) -> Result<BlockNumber> {
    // TODO: Id should be different on every request, this is how request are identified by us and by the node.
    let params = RpcRequest {
        jsonrpc: "2.0",
        id: "0".to_string(),
        method: "eth_getBlockByNumber",
        params: ("finalized", false),
    };

    let blockheader = make_retrying_rpc_call::<_, BlockHeaderWithEmptyTransaction>(
        &params,
        timeout,
        MAX_RETRIES.into(),
    )
    .await
    .map_err(|e| {
        BlockchainError::rpc_connection(format!("Failed to get latest finalized block number: {e}"))
    })?;

    BlockNumber::from_hex(&blockheader.number)
}

/// Fetches complete block data including all transactions for a specific block number.
///
/// This function retrieves comprehensive blockchain data for a given block, including
/// the block header information and full transaction details. This is more expensive
/// than fetching just block headers but provides complete block state.
///
/// # Arguments
///
/// * `number` - The block number to fetch data for
/// * `timeout` - Optional timeout in seconds for the RPC request. If `None`, uses default timeout.
///
/// # Returns
///
/// Returns a `Result<BlockHeaderWithFullTransaction>` containing the complete block data
/// including all transaction details.
///
/// # Errors
///
/// This function will return an error if:
/// - The `NODE_CONNECTION_STRING` environment variable is not set
/// - The specified block number doesn't exist on the network
/// - The RPC request fails or times out
/// - The response cannot be parsed as valid block data
/// - Network connectivity issues occur
///
/// # Examples
///
/// ```rust,no_run
/// use fossil_headers_db::rpc::get_full_block_by_number;
/// use fossil_headers_db::types::BlockNumber;
///
/// # async fn example() -> eyre::Result<()> {
/// let block_number = BlockNumber::from_trusted(19000000);
/// let block_data = get_full_block_by_number(block_number, Some(30)).await?;
///
/// println!("Block hash: {}", block_data.hash);
/// println!("Number of transactions: {}", block_data.transactions.len());
/// println!("Gas used: {}", block_data.gas_used);
/// # Ok(())
/// # }
/// ```
pub async fn get_full_block_by_number(
    number: BlockNumber,
    timeout: Option<u64>,
) -> Result<BlockHeaderWithFullTransaction> {
    let params = RpcRequest {
        jsonrpc: "2.0",
        id: "0".to_string(),
        method: "eth_getBlockByNumber",
        params: (format!("0x{:x}", number.value()), true),
    };

    make_retrying_rpc_call::<_, BlockHeaderWithFullTransaction>(
        &params,
        timeout,
        MAX_RETRIES.into(),
    )
    .await
}

// TODO: Make this work as expected
#[allow(dead_code)]
pub async fn batch_get_full_block_by_number(
    numbers: Vec<BlockNumber>,
    timeout: Option<u64>,
) -> Result<Vec<BlockHeaderWithFullTransaction>> {
    let mut params = Vec::new();
    for number in numbers {
        let num_str = number.value().to_string();
        params.push(RpcRequest {
            jsonrpc: "2.0",
            id: num_str,
            method: "eth_getBlockByNumber",
            params: (format!("0x{:x}", number.value()), true),
        });
    }
    make_rpc_call::<_, Vec<BlockHeaderWithFullTransaction>>(&params, timeout).await
}

fn get_connection_string() -> Result<&'static str> {
    NODE_CONNECTION_STRING
        .get_or_init(|| {
            dotenvy::var("NODE_CONNECTION_STRING")
                .map_err(|e| error!("Failed to get NODE_CONNECTION_STRING: {}", e))
                .ok()
        })
        .as_ref()
        .ok_or_else(|| {
            BlockchainError::configuration("NODE_CONNECTION_STRING", "Environment variable not set")
        })
        .map(std::string::String::as_str)
}

async fn send_http_request<T: Serialize + Sync>(
    params: &T,
    connection_string: &str,
    timeout: Option<u64>,
) -> Result<reqwest::Response> {
    let request_builder = CLIENT
        .get_or_init(Client::new)
        .post(connection_string)
        .json(params);

    let response = match timeout {
        Some(seconds) => {
            request_builder
                .timeout(Duration::from_secs(seconds))
                .send()
                .await
        }
        None => request_builder.send().await,
    };

    response.map_err(|e| {
        error!("HTTP request error: {:?}", e);
        e.into()
    })
}

async fn parse_rpc_response<R: for<'de> Deserialize<'de>>(
    response: reqwest::Response,
) -> Result<R> {
    let text = response.text().await.map_err(|e| {
        error!("Failed to read response body: {:?}", e);
        e
    })?;

    serde_json::from_str::<RpcResponse<R>>(&text)
        .map(|parsed| parsed.result)
        .map_err(|e| {
            error!(
                "Deserialization error: {:?}\nResponse snippet: {:?}",
                e, text
            );
            e.into()
        })
}

async fn make_rpc_call<T: Serialize + Sync, R: for<'de> Deserialize<'de>>(
    params: &T,
    timeout: Option<u64>,
) -> Result<R> {
    let connection_string = get_connection_string()?;
    let response = send_http_request(params, connection_string, timeout).await?;
    parse_rpc_response(response).await
}

async fn make_retrying_rpc_call<T: Serialize + Sync, R: for<'de> Deserialize<'de> + Send>(
    params: &T,
    timeout: Option<u64>,
    max_retries: u32,
) -> Result<R> {
    let mut attempts = 0;
    loop {
        match make_rpc_call(params, timeout).await {
            Ok(result) => return Ok(result),
            Err(e) => {
                attempts += 1;
                if attempts > max_retries {
                    warn!("Operation failed with error: {:?}. Max retries reached", e);
                    return Err(e);
                }
                let backoff = Duration::from_secs(2_u64.pow(attempts));
                warn!(
                    "Operation failed with error: {:?}. Retrying in {:?} (Attempt {}/{})",
                    e, backoff, attempts, max_retries
                );
                sleep(backoff).await;
            }
        }
    }
}
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum BlockTransaction {
    Full(Box<Transaction>),
    Hash(String),
}

impl TryFrom<BlockTransaction> for Transaction {
    type Error = BlockchainError;

    fn try_from(value: BlockTransaction) -> std::result::Result<Self, Self::Error> {
        match value {
            BlockTransaction::Full(tx) => Ok(*tx),
            BlockTransaction::Hash(_) => Err(BlockchainError::internal(
                "Cannot convert hash into Transaction",
            )),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BlockHeader {
    #[serde(rename = "gasLimit")]
    pub gas_limit: String,
    #[serde(rename = "gasUsed")]
    pub gas_used: String,
    #[serde(rename(deserialize = "baseFeePerGas"))]
    pub base_fee_per_gas: Option<String>,
    pub hash: String,
    pub nonce: Option<String>,
    pub number: String,
    #[serde(rename = "receiptsRoot")]
    pub receipts_root: Option<String>,
    #[serde(rename = "stateRoot")]
    pub state_root: Option<String>,
    #[serde(rename = "transactionsRoot")]
    pub transactions_root: Option<String>,
    pub transactions: Vec<BlockTransaction>,
    #[serde(rename = "parentHash")]
    pub parent_hash: Option<String>,
    pub miner: Option<String>,
    #[serde(rename = "logsBloom")]
    pub logs_bloom: Option<String>,
    #[serde(rename = "difficulty")]
    pub difficulty: Option<String>,
    #[serde(rename = "totalDifficulty")]
    pub total_difficulty: Option<String>,
    #[serde(rename = "sha3Uncles")]
    pub sha3_uncles: Option<String>,
    #[serde(rename = "timestamp")]
    pub timestamp: String,
    #[serde(rename = "extraData")]
    pub extra_data: Option<String>,
    #[serde(rename = "mixHash")]
    pub mix_hash: Option<String>,
    #[serde(rename = "withdrawalsRoot")]
    pub withdrawals_root: Option<String>,
    #[serde(rename = "blobGasUsed")]
    pub blob_gas_used: Option<String>,
    #[serde(rename = "excessBlobGas")]
    pub excess_blob_gas: Option<String>,
    #[serde(rename = "parentBeaconBlockRoot")]
    pub parent_beacon_block_root: Option<String>,
}

#[allow(dead_code)]
pub trait EthereumRpcProvider {
    fn get_latest_finalized_blocknumber(
        &self,
        timeout: Option<u64>,
    ) -> impl Future<Output = Result<BlockNumber>> + Send;
    fn get_full_block_by_number(
        &self,
        number: BlockNumber,
        include_tx: bool,
        timeout: Option<u64>,
    ) -> impl Future<Output = Result<BlockHeader>> + Send;
}

#[derive(Debug, Clone)]
pub struct EthereumJsonRpcClient {
    client: reqwest::Client,
    connection_string: String,
    max_retries: u32,
}

impl EthereumJsonRpcClient {
    #[allow(dead_code)]
    #[must_use]
    pub fn new(connection_string: String, max_retries: u32) -> Self {
        Self {
            client: reqwest::Client::new(),
            connection_string,
            max_retries,
        }
    }
}

impl EthereumJsonRpcClient {
    async fn send_http_request<T: Serialize + Send + Sync>(
        &self,
        params: T,
        timeout: Option<u64>,
    ) -> Result<reqwest::Response> {
        let connection_string = self.connection_string.clone();

        let response = match timeout {
            Some(seconds) => {
                self.client
                    .post(connection_string)
                    .timeout(Duration::from_secs(seconds))
                    .json(&params)
                    .send()
                    .await
            }
            None => {
                self.client
                    .post(connection_string)
                    .json(&params)
                    .send()
                    .await
            }
        };

        response.map_err(|e| {
            error!("HTTP request error: {:?}", e);
            e.into()
        })
    }

    async fn parse_response<R: for<'de> Deserialize<'de> + Send>(
        &self,
        response: reqwest::Response,
    ) -> Result<R> {
        let text = response.text().await.map_err(|e| {
            error!("Failed to read response body: {:?}", e);
            e
        })?;

        serde_json::from_str::<RpcResponse<R>>(&text)
            .map(|parsed| parsed.result)
            .map_err(|e| {
                error!(
                    "Deserialization error: {:?}\nResponse snippet: {:?}",
                    e, text
                );
                e.into()
            })
    }

    async fn make_rpc_call<T: Serialize + Send + Sync, R: for<'de> Deserialize<'de> + Send>(
        &self,
        params: T,
        timeout: Option<u64>,
    ) -> Result<R> {
        let response = self.send_http_request(params, timeout).await?;
        self.parse_response(response).await
    }

    async fn make_retrying_rpc_call<
        T: Serialize + Send + Sync + Clone,
        R: for<'de> Deserialize<'de> + Send,
    >(
        &self,
        params: T,
        timeout: Option<u64>,
    ) -> Result<R> {
        let mut attempts = 0;
        loop {
            match self.make_rpc_call(params.clone(), timeout).await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    attempts += 1;
                    if attempts > self.max_retries {
                        warn!("Operation failed with error: {:?}. Max retries reached", e);
                        return Err(e);
                    }
                    let backoff = Duration::from_secs(2_u64.pow(attempts));
                    warn!(
                        "Operation failed with error: {:?}. Retrying in {:?} (Attempt {}/{})",
                        e, backoff, attempts, self.max_retries
                    );
                    sleep(backoff).await;
                }
            }
        }
    }
}

impl EthereumRpcProvider for EthereumJsonRpcClient {
    async fn get_latest_finalized_blocknumber(&self, timeout: Option<u64>) -> Result<BlockNumber> {
        let params = RpcRequest {
            jsonrpc: "2.0",
            id: "0".to_string(),
            method: "eth_getBlockByNumber",
            params: ("finalized", false),
        };

        match self
            .make_retrying_rpc_call::<_, BlockHeader>(&params, timeout)
            .await
            .map_err(|e| {
                BlockchainError::rpc_connection(format!("Failed to get latest block number: {e}"))
            }) {
            Ok(blockheader) => Ok(BlockNumber::from_hex(&blockheader.number)?),
            Err(e) => Err(e),
        }
    }

    async fn get_full_block_by_number(
        &self,
        number: BlockNumber,
        include_tx: bool,
        timeout: Option<u64>,
    ) -> Result<BlockHeader> {
        let params = RpcRequest {
            jsonrpc: "2.0",
            id: "0".to_string(),
            method: "eth_getBlockByNumber",
            params: (format!("0x{:x}", number.value()), include_tx),
        };

        self.make_retrying_rpc_call::<_, BlockHeader>(&params, timeout)
            .await
    }
}

// Attempts to convert a vector of BlockTransaction type into the Transaction type
pub fn try_convert_full_tx_vector(block_tx_vec: Vec<BlockTransaction>) -> Result<Vec<Transaction>> {
    let mut result_vec = vec![];

    for block_tx in block_tx_vec {
        let tx = block_tx.try_into()?;
        result_vec.push(tx);
    }

    Ok(result_vec)
}

/// BIG TODO:
/// Handle rpc errors correctly!
/// Currently error cases are not handled as well.
#[cfg(test)]
mod tests;

#[cfg(test)]
mod integration_tests {
    use std::thread;

    use super::*;
    use tokio::{
        fs,
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    async fn get_fixtures_for_tests() -> BlockHeader {
        let block_21598014_string =
            fs::read_to_string("tests/fixtures/eth_getBlockByNumber_21598014.json")
                .await
                .unwrap();

        let block_21598014_response =
            serde_json::from_str::<RpcResponse<BlockHeader>>(&block_21598014_string).unwrap();

        block_21598014_response.result
    }

    // Helper function to start a TCP server that returns predefined JSON-RPC responses
    // Taken from katana codebase.
    pub fn start_mock_rpc_server(addr: String, responses: Vec<Option<BlockHeader>>) {
        use tokio::runtime::Builder;

        thread::spawn(move || {
            // Using the current thread runtime to allow for a more linear test
            Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    let listener = TcpListener::bind(addr).await.unwrap();

                    let mut counter = 0;

                    loop {
                        let (mut socket, _) = listener.accept().await.unwrap();

                        // Read the request
                        let mut buffer = [0; 512];
                        let _ = socket.read(&mut buffer).await.unwrap();

                        let buffer: Vec<u8> = buffer.into_iter().take_while(|&x| x != 0).collect();

                        let request_str = String::from_utf8_lossy(&buffer);

                        // Find the JSON body after the empty line in HTTP request
                        let body = if let Some(idx) = request_str.find("\r\n\r\n") {
                            &request_str[idx + 4..]
                        } else {
                            ""
                        };

                        // Get the response from the pre-determined list, and if the list is
                        // exhausted, return the last response.
                        let selected_response = if counter < responses.len() {
                            responses[counter].clone()
                        } else {
                            responses.last().unwrap().clone()
                        };

                        // Parse the JSON body and check the value
                        let response = if let Ok(json) =
                            serde_json::from_str::<RpcRequest<(String, bool)>>(body)
                        {
                            match selected_response {
                                Some(data) => {
                                    // Based on the params return full or hashed txs.
                                    // Here assuming the provided responses are always with full txs.
                                    let final_response = if json.params.1 {
                                        data
                                    } else {
                                        // If set to false, then remove the full txs and replace with the hashed ones.
                                        BlockHeader {
                                            transactions: data
                                                .transactions
                                                .iter()
                                                .map(|tx| match tx {
                                                    BlockTransaction::Full(full_tx) => {
                                                        BlockTransaction::Hash(full_tx.hash.clone())
                                                    }
                                                    other => other.clone(),
                                                })
                                                .collect(),
                                            ..data
                                        }
                                    };

                                    let final_response =
                                        serde_json::to_string(&final_response).unwrap();
                                    format!(
                                        r#"{{ "jsonrpc": "2.0", "id": 1, "result": {} }}"#,
                                        final_response
                                    )
                                }
                                None => "{}".to_string(),
                            }
                        } else {
                            // TODO: handle this and add tests.

                            // Return an error response if JSON parsing fails
                            r#"{"error": "Invalid JSON format"}"#.to_string()
                        };

                        // After reading, we send the pre-determined response
                        let http_response = format!(
                            "HTTP/1.1 200 OK\r\ncontent-length: {}\r\ncontent-type: \
                             application/json\r\n\r\n{}",
                            response.len(),
                            response
                        );

                        socket.write_all(http_response.as_bytes()).await.unwrap();
                        socket.flush().await.unwrap();
                        counter += 1;
                    }
                });
        });
    }

    #[tokio::test]
    async fn test_try_from_for_full_tx() {
        // Since this is a full tx, then try from should work
        let header = get_fixtures_for_tests().await;

        // Get one of the tx to test.
        let tx = header.transactions[0].clone();

        let expected_tx = tx.clone();

        let expected_tx = if let BlockTransaction::Full(full_tx) = expected_tx {
            full_tx
        } else {
            panic!("unexpected error due to tx type, should not happen.")
        };

        let actual_tx: Result<Transaction> = Transaction::try_from(tx.clone());

        let actual_tx = actual_tx.unwrap();

        assert_eq!(expected_tx.block_number, actual_tx.block_number);
        assert_eq!(expected_tx.chain_id, actual_tx.chain_id);
        assert_eq!(expected_tx.from, actual_tx.from);
        assert_eq!(expected_tx.to, actual_tx.to);
        assert_eq!(expected_tx.transaction_index, actual_tx.transaction_index);
        assert_eq!(expected_tx.gas, actual_tx.gas);
        assert_eq!(expected_tx.gas_price, actual_tx.gas_price);
        assert_eq!(expected_tx.max_fee_per_gas, actual_tx.max_fee_per_gas);
        assert_eq!(
            expected_tx.max_priority_fee_per_gas,
            actual_tx.max_priority_fee_per_gas
        );
        assert_eq!(expected_tx.value, actual_tx.value);
        assert_eq!(expected_tx.hash, actual_tx.hash);
    }

    #[tokio::test]
    async fn test_try_into_for_full_tx() {
        // Since this is a full tx, then try from should work
        let header = get_fixtures_for_tests().await;

        // Get one of the tx to test.
        let tx = header.transactions[0].clone();

        let expected_tx = if let BlockTransaction::Full(full_tx) = tx.clone() {
            full_tx
        } else {
            panic!("unexpected error due to tx type, should not happen.")
        };

        let actual_tx: Result<Transaction> = tx.try_into();
        assert!(actual_tx.is_ok());

        let actual_tx = actual_tx.unwrap();

        assert_eq!(expected_tx.block_number, actual_tx.block_number);
        assert_eq!(expected_tx.chain_id, actual_tx.chain_id);
        assert_eq!(expected_tx.from, actual_tx.from);
        assert_eq!(expected_tx.to, actual_tx.to);
        assert_eq!(expected_tx.transaction_index, actual_tx.transaction_index);
        assert_eq!(expected_tx.gas, actual_tx.gas);
        assert_eq!(expected_tx.gas_price, actual_tx.gas_price);
        assert_eq!(expected_tx.max_fee_per_gas, actual_tx.max_fee_per_gas);
        assert_eq!(
            expected_tx.max_priority_fee_per_gas,
            actual_tx.max_priority_fee_per_gas
        );
        assert_eq!(expected_tx.value, actual_tx.value);
        assert_eq!(expected_tx.hash, actual_tx.hash);
    }

    #[tokio::test]
    async fn test_try_convert_full_tx_vector() {
        let header = get_fixtures_for_tests().await;

        // Get one of the tx to test.
        let expected_tx = header.transactions[0].clone();
        let converted_tx = try_convert_full_tx_vector(vec![expected_tx.clone()]);

        let expected_tx: Transaction = expected_tx.try_into().unwrap();

        assert!(converted_tx.is_ok());
        let actual_tx = converted_tx.unwrap()[0].clone();

        assert_eq!(expected_tx.hash, actual_tx.hash);
        assert_eq!(expected_tx.block_number, actual_tx.block_number);
    }

    #[tokio::test]
    async fn test_try_convert_full_tx_vector_should_fail_for_hash_tx() {
        let txs = try_convert_full_tx_vector(vec![BlockTransaction::Hash("0x12345ab".to_string())]);
        assert!(txs.is_err());
    }

    #[tokio::test]
    async fn test_try_convert_full_tx_vector_should_be_empty_when_empty_vec_given() {
        let txs = try_convert_full_tx_vector(vec![]);
        assert!(txs.is_ok());
        assert!(txs.unwrap().is_empty());
    }

    #[tokio::test]
    #[serial]
    async fn test_max_retries_should_affect_number_of_retries() {
        let rpc_response = get_fixtures_for_tests().await;
        start_mock_rpc_server(
            "127.0.0.1:8091".to_owned(),
            vec![None, None, None, Some(rpc_response), None], // introduce empty responses
        );

        // Set max retries to 2, which shouldn't have any success cases.
        let client = EthereumJsonRpcClient::new("http://127.0.0.1:8091".to_owned(), 2);

        let block =
            client.get_full_block_by_number(BlockNumber::from_trusted(21598014), false, None);

        let block = block.await;
        assert!(block.is_err());
    }

    #[tokio::test]
    #[serial]
    async fn test_get_full_block_by_number_should_retry_when_failed() {
        let rpc_response = get_fixtures_for_tests().await;
        start_mock_rpc_server(
            "127.0.0.1:8092".to_owned(),
            vec![None, Some(rpc_response.clone())], // introduce an empty response to induce failure
        );

        let client = EthereumJsonRpcClient::new("http://127.0.0.1:8092".to_owned(), 2);
        let block =
            client.get_full_block_by_number(BlockNumber::from_trusted(21598014), false, None);

        let block = block.await.unwrap();
        assert_eq!(block.hash, rpc_response.hash);
        assert_eq!(block.number, rpc_response.number);
    }

    #[tokio::test]
    #[serial]
    async fn test_get_latest_finalized_blocknumber_should_retry_when_failed() {
        let rpc_response = get_fixtures_for_tests().await;
        start_mock_rpc_server(
            "127.0.0.1:8093".to_owned(),
            vec![None, Some(rpc_response.clone())], // introduce an empty response to induce failure
        );

        let client = EthereumJsonRpcClient::new("http://127.0.0.1:8093".to_owned(), 2);
        let block_number = client.get_latest_finalized_blocknumber(None);

        let block_number = block_number.await.unwrap();
        assert_eq!(
            block_number,
            BlockNumber::from_trusted(
                i64::from_str_radix(rpc_response.number.strip_prefix("0x").unwrap(), 16).unwrap()
            )
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_get_full_block_by_number_without_tx() {
        let rpc_response = get_fixtures_for_tests().await;
        start_mock_rpc_server(
            "127.0.0.1:8094".to_owned(),
            vec![Some(rpc_response.clone())],
        );

        let client = EthereumJsonRpcClient::new("http://127.0.0.1:8094".to_owned(), 1);
        let block =
            client.get_full_block_by_number(BlockNumber::from_trusted(21598014), false, None);

        let block = block.await.unwrap();
        assert_eq!(block.hash, rpc_response.hash);
        assert_eq!(block.number, rpc_response.number);
        for (idx, block_tx) in block.transactions.iter().enumerate() {
            if let BlockTransaction::Hash(hash) = block_tx {
                let fixture_tx: Transaction =
                    rpc_response.transactions[idx].clone().try_into().unwrap();
                assert_eq!(hash.to_owned(), fixture_tx.hash)
            } else {
                panic!("returned tx are full.")
            }
        }
    }

    #[tokio::test]
    #[serial]
    async fn test_get_full_block_by_number_with_tx() {
        let rpc_response = get_fixtures_for_tests().await;
        start_mock_rpc_server(
            "127.0.0.1:8095".to_owned(),
            vec![Some(rpc_response.clone())],
        );

        let client = EthereumJsonRpcClient::new("http://127.0.0.1:8095".to_owned(), 1);
        let block =
            client.get_full_block_by_number(BlockNumber::from_trusted(21598014), true, None);

        let block = block.await.unwrap();
        assert_eq!(block.hash, rpc_response.hash);
        assert_eq!(block.number, rpc_response.number);
        for (idx, block_tx) in block.transactions.iter().enumerate() {
            if let BlockTransaction::Full(tx) = block_tx {
                let fixture_tx: Transaction = rpc_response.clone().transactions[idx]
                    .clone()
                    .try_into()
                    .unwrap();

                assert_eq!(tx.hash, fixture_tx.hash);
                assert_eq!(tx.block_number, fixture_tx.block_number);
                assert_eq!(tx.from, fixture_tx.from);
                assert_eq!(tx.to, fixture_tx.to);
                assert_eq!(tx.transaction_index, fixture_tx.transaction_index);
                assert_eq!(tx.value, fixture_tx.value);
            } else {
                panic!("returned tx are hashed.")
            }
        }
    }

    #[tokio::test]
    #[serial]
    async fn test_get_latest_finalized_blocknumber() {
        let rpc_response = get_fixtures_for_tests().await;
        start_mock_rpc_server(
            "127.0.0.1:8096".to_owned(),
            vec![Some(rpc_response.clone())],
        );

        let client = EthereumJsonRpcClient::new("http://127.0.0.1:8096".to_owned(), 1);
        let block_number = client.get_latest_finalized_blocknumber(None);

        let block_number = block_number.await.unwrap();
        assert_eq!(
            block_number,
            BlockNumber::from_trusted(
                i64::from_str_radix(rpc_response.number.strip_prefix("0x").unwrap(), 16).unwrap()
            )
        );
    }

    // TODO: Add more tests for the failure cases.
}
