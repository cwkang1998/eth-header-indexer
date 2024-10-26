use crate::types::type_utils::convert_hex_string_to_i64;
use crate::types::BlockDetails;
use crate::types::BlockHeaderWithFullTransaction;
use anyhow::{Context, Result};
use sqlx::postgres::PgConnectOptions;
use sqlx::ConnectOptions;
use sqlx::QueryBuilder;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::OnceCell;
use tracing::{error, info, warn};

static DB_POOL: OnceCell<Arc<Pool<Postgres>>> = OnceCell::const_new();
pub const DB_MAX_CONNECTIONS: u32 = 1000;

pub async fn get_db_pool() -> Result<Arc<Pool<Postgres>>> {
    match DB_POOL.get() {
        Some(pool) => Ok(pool.clone()),
        None => {
            let mut conn_options: PgConnectOptions = dotenvy::var("DB_CONNECTION_STRING")
                .expect("DB_CONNECTION_STRING must be set")
                .parse()?;
            conn_options = conn_options
                .log_slow_statements(tracing::log::LevelFilter::Debug, Duration::new(120, 0));

            let pool = PgPoolOptions::new()
                .max_connections(DB_MAX_CONNECTIONS)
                .connect_with(conn_options)
                .await?;
            let arc_pool = Arc::new(pool);
            match DB_POOL.set(arc_pool.clone()) {
                Ok(_) => Ok(arc_pool),
                Err(_) => Ok(DB_POOL.get().expect("Pool was just set").clone()),
            }
        }
    }
}

pub async fn create_tables() -> Result<()> {
    let pool = get_db_pool().await?;
    sqlx::query(include_str!("./sql/blockheaders_table.sql"))
        .execute(&*pool)
        .await
        .context("Failed to create blockheaders table")?;
    sqlx::query(include_str!("./sql/transactions_table.sql"))
        .execute(&*pool)
        .await
        .context("Failed to create transactions table")?;
    Ok(())
}

/**
 * Retrieves the blocknumber of the latest stored blockheader
 *
 * @Returns blocknumber, else -1 if table is empty
 */
pub async fn get_last_stored_blocknumber() -> Result<i64> {
    let pool = get_db_pool().await.context("Failed to get database pool")?;
    let result: (i64,) = sqlx::query_as("SELECT COALESCE(MAX(number), -1) FROM blockheaders")
        .fetch_one(&*pool)
        .await
        .context("Failed to get last stored block number")?;

    Ok(result.0)
}

/**
 * Returns the first missing blocknumber in between provided numbers (inclusive)
 */
pub async fn find_first_gap(start: i64, end: i64) -> Result<Option<i64>> {
    let pool = get_db_pool().await.context("Failed to get database pool")?;
    let result: Option<(i64,)> = sqlx::query_as(
        r#"
        WITH RECURSIVE number_series(n) AS (
            SELECT $1
            UNION ALL
            SELECT n + 1 FROM number_series WHERE n < $2
        )
        SELECT n FROM number_series
        WHERE n NOT IN (SELECT number FROM blockheaders WHERE number BETWEEN $1 AND $2)
        LIMIT 1
        "#,
    )
    .bind(start)
    .bind(end)
    .fetch_optional(&*pool)
    .await
    .context("Failed to find first gap")?;

    Ok(result.map(|r| r.0))
}

pub async fn write_blockheader(block_header: BlockHeaderWithFullTransaction) -> Result<()> {
    let pool = get_db_pool().await?;
    let mut tx = pool.begin().await?;

    // // Print block_header details
    // info!("Block Header Details: {:?}", block_header);

    // Insert block header
    let result = sqlx::query(
        r#"
        INSERT INTO blockheaders (
            block_hash, number, gas_limit, gas_used, base_fee_per_gas,
            nonce, transaction_root, receipts_root, state_root,
            parent_hash, miner, logs_bloom, difficulty, totalDifficulty,
            sha3_uncles, timestamp, extra_data, mix_hash, withdrawals_root, 
            blob_gas_used, excess_blob_gas, parent_beacon_block_root
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
        ON CONFLICT (number)
        DO UPDATE SET 
            block_hash = EXCLUDED.block_hash,
            gas_limit = EXCLUDED.gas_limit,
            gas_used = EXCLUDED.gas_used,
            base_fee_per_gas = EXCLUDED.base_fee_per_gas,
            nonce = EXCLUDED.nonce,
            transaction_root = EXCLUDED.transaction_root,
            receipts_root = EXCLUDED.receipts_root,
            state_root = EXCLUDED.state_root,
            parent_hash = EXCLUDED.parent_hash,
            miner = EXCLUDED.miner,
            logs_bloom = EXCLUDED.logs_bloom,
            difficulty = EXCLUDED.difficulty,
            totalDifficulty = EXCLUDED.totalDifficulty,
            sha3_uncles = EXCLUDED.sha3_uncles,
            timestamp = EXCLUDED.timestamp,
            extra_data = EXCLUDED.extra_data,
            mix_hash = EXCLUDED.mix_hash,
            withdrawals_root = EXCLUDED.withdrawals_root,
            blob_gas_used = EXCLUDED.blob_gas_used,
            excess_blob_gas = EXCLUDED.excess_blob_gas,
            parent_beacon_block_root = EXCLUDED.parent_beacon_block_root;
        "#,
    )
    .bind(&block_header.hash)
    .bind(convert_hex_string_to_i64(&block_header.number))
    .bind(convert_hex_string_to_i64(&block_header.gas_limit))
    .bind(convert_hex_string_to_i64(&block_header.gas_used))
    .bind(&block_header.base_fee_per_gas)
    .bind(&block_header.nonce)
    .bind(&block_header.transactions_root)
    .bind(&block_header.receipts_root)
    .bind(&block_header.state_root)
    .bind(&block_header.parent_hash)
    .bind(&block_header.miner)
    .bind(&block_header.logs_bloom)
    .bind(&block_header.difficulty)
    .bind(&block_header.total_difficulty)
    .bind(&block_header.sha3_uncles)
    .bind(convert_hex_string_to_i64(&block_header.timestamp))
    .bind(&block_header.extra_data)
    .bind(&block_header.mix_hash)
    .bind(&block_header.withdrawals_root)
    .bind(&block_header.blob_gas_used)
    .bind(&block_header.excess_blob_gas)
    .bind(&block_header.parent_beacon_block_root)
    .execute(&mut *tx) // Changed this line
    .await
    .with_context(|| format!("Failed to insert block header for block number: {}", block_header.number))
    .map_err(|e| {
        error!("Detailed error: {:?}", e);
        e
    })?;

    if result.rows_affected() == 0 {
        warn!(
            "Block already exists: -- block number: {}, block hash: {}",
            block_header.number, block_header.hash
        );
        return Ok(());
    } else {
        info!(
            "Inserted block number: {}, block hash: {}",
            block_header.number, block_header.hash
        );
    }

    // Insert transactions
    if !block_header.transactions.is_empty() {
        let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
            "INSERT INTO transactions (
                block_number, transaction_hash, transaction_index,
                from_addr, to_addr, value, gas_price,
                max_priority_fee_per_gas, max_fee_per_gas, gas, chain_id
            ) ",
        );

        query_builder.push_values(block_header.transactions.iter(), |mut b, tx| {
            b.push_bind(convert_hex_string_to_i64(&tx.block_number))
                .push_bind(&tx.hash)
                .push_bind(convert_hex_string_to_i64(&tx.transaction_index))
                .push_bind(&tx.from)
                .push_bind(&tx.to)
                .push_bind(&tx.value)
                .push_bind(&tx.gas_price)
                .push_bind(&tx.max_priority_fee_per_gas)
                .push_bind(&tx.max_fee_per_gas)
                .push_bind(&tx.gas)
                .push_bind(&tx.chain_id);
        });

        query_builder.push(" ON CONFLICT (transaction_hash) DO NOTHING");

        let query = query_builder.build();
        let result = query
            .execute(&mut *tx)
            .await
            .context("Failed to insert transactions")?;

        info!(
            "Inserted {} transactions for block {}",
            result.rows_affected(),
            block_header.number
        );
    }

    tx.commit().await.context("Failed to commit transaction")?;
    Ok(())
}

/**
 * Retrieves next n numbers and hashes after provided blocknumber
 *
 * @Returns blocknumbers and hashes wrapped in a BlockDetails struct
 */
pub async fn get_blockheaders(start_blocknumber: i64, limit: i32) -> Result<Vec<BlockDetails>> {
    let pool = get_db_pool().await?;
    let result: Vec<BlockDetails> = sqlx::query_as(
        r#"
        SELECT block_hash, number FROM blockheaders
            WHERE number > $1
            ORDER BY number ASC
            LIMIT $2
        "#,
    )
    .bind(start_blocknumber)
    .bind(limit)
    .fetch_all(&*pool)
    .await
    .context("Failed to get blockheaders")?;

    Ok(result)
}
