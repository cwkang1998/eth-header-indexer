use eyre::{eyre, Report, Result};
use serde::Deserialize;
use sqlx::Postgres;
use std::sync::Arc;
use tracing::error;

use crate::db::DbConnection;

#[derive(Debug, Deserialize, sqlx::FromRow)]
#[allow(dead_code)]
/// Internal data transfer object for index metadata.
///
/// This struct is used internally for database operations and is not part of the public API.
#[doc(hidden)]
pub struct IndexMetadataDto {
    pub id: i64,
    pub current_latest_block_number: i64,
    pub indexing_starting_block_number: i64,
    pub is_backfilling: bool,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub backfilling_block_number: Option<i64>,
}

/// Retrieves the current indexing metadata from the database.
///
/// This function fetches the indexing progress and status information that tracks
/// the state of blockchain data indexing operations. The metadata includes information
/// about current progress, backfilling status, and indexing boundaries.
///
/// # Arguments
///
/// * `db` - Shared reference to the database connection
///
/// # Returns
///
/// Returns a `Result<Option<IndexMetadataDto>>` where:
/// - `Some(IndexMetadataDto)` contains the metadata if it exists
/// - `None` if no metadata record exists (e.g., first-time setup)
///
/// # Errors
///
/// This function will return an error if:
/// - The database query fails to execute
/// - The connection to the database is lost
/// - The metadata table structure is invalid
///
/// # Examples
///
/// ```rust,no_run
/// use fossil_headers_db::repositories::index_metadata::get_index_metadata;
/// use fossil_headers_db::db::DbConnection;
///
/// # async fn example() -> eyre::Result<()> {
/// let db_conn_string = "postgresql://user:pass@localhost/db".to_string();
/// let db = DbConnection::new(db_conn_string).await?;
///
/// match get_index_metadata(db).await? {
///     Some(metadata) => {
///         println!("Latest block: {}", metadata.current_latest_block_number);
///         println!("Is backfilling: {}", metadata.is_backfilling);
///     },
///     None => println!("No indexing metadata found - first run?"),
/// }
/// # Ok(())
/// # }
/// ```
// TODO: allow dead code for now. Adding tests in future PRs should allow us to remove this.
#[allow(dead_code)]
pub async fn get_index_metadata(db: Arc<DbConnection>) -> Result<Option<IndexMetadataDto>> {
    let db = db.as_ref();
    let result: Result<IndexMetadataDto, sqlx::Error> = sqlx::query_as(
        r"
            SELECT 
                id,
                current_latest_block_number,
                indexing_starting_block_number,
                is_backfilling,
                updated_at,
                backfilling_block_number
            FROM index_metadata
            ",
    )
    .fetch_one(&db.pool)
    .await;

    let result: Option<IndexMetadataDto> = match result {
        Ok(result) => Some(result),
        Err(err) => match err {
            sqlx::Error::RowNotFound => None,
            err => {
                error!("Failed to get indexer metadata: {}", err);
                return Err(Report::new(err));
            }
        },
    };

    Ok(result)
}

#[allow(dead_code)]
pub async fn set_is_backfilling(
    db_tx: &mut sqlx::Transaction<'_, Postgres>,
    is_backfilling: bool,
) -> Result<()> {
    sqlx::query(
        r"
            UPDATE index_metadata
            SET is_backfilling = $1,
            updated_at = CURRENT_TIMESTAMP
            ",
    )
    .bind(is_backfilling)
    .execute(&mut **db_tx)
    .await?;

    Ok(())
}

#[allow(dead_code)]
pub async fn set_initial_indexing_status(
    db: Arc<DbConnection>,
    current_latest_block_number: i64,
    indexing_starting_block_number: i64,
    is_backfilling: bool,
) -> Result<()> {
    // Check if there's already an entry, if it does then we can skip and only update.
    let result = sqlx::query(
        r"
            SELECT id
            FROM index_metadata
            ",
    )
    .fetch_one(&db.pool)
    .await;

    if result.is_ok() {
        let result = sqlx::query(
            r"
                UPDATE index_metadata
                SET current_latest_block_number = $1,
                    indexing_starting_block_number = $2,
                    is_backfilling = $3,
                    updated_at = CURRENT_TIMESTAMP
                ",
        )
        .bind(current_latest_block_number)
        .bind(indexing_starting_block_number)
        .bind(is_backfilling)
        .execute(&db.pool)
        .await?;

        if result.rows_affected() != 1 {
            error!("Failed to update initial indexing status");
            return Err(eyre!("Failed to update initial indexing status".to_owned(),));
        }

        return Ok(());
    }

    let result = sqlx::query(
        r"
            INSERT INTO index_metadata (
                current_latest_block_number,
                indexing_starting_block_number,
                is_backfilling
            ) VALUES (
                $1,
                $2,
                $3
            )
            ",
    )
    .bind(current_latest_block_number)
    .bind(indexing_starting_block_number)
    .bind(is_backfilling)
    .execute(&db.pool)
    .await?;

    if result.rows_affected() != 1 {
        error!("Failed to insert initial indexing status");
        return Err(eyre!("Failed to insert initial indexing status".to_owned(),));
    }

    Ok(())
}

#[allow(dead_code)]
pub async fn update_latest_quick_index_block_number_query(
    db_tx: &mut sqlx::Transaction<'_, Postgres>,
    block_number: i64,
) -> Result<()> {
    sqlx::query(
        r"
            UPDATE index_metadata
            SET current_latest_block_number = $1,
            updated_at = CURRENT_TIMESTAMP
            ",
    )
    .bind(block_number)
    .execute(&mut **db_tx)
    .await?;

    Ok(())
}

#[allow(dead_code)]
pub async fn update_backfilling_block_number_query(
    db_tx: &mut sqlx::Transaction<'_, Postgres>,
    block_number: i64,
) -> Result<()> {
    sqlx::query(
        r"
            UPDATE index_metadata
            SET backfilling_block_number = $1,
            updated_at = CURRENT_TIMESTAMP
            ",
    )
    .bind(block_number)
    .execute(&mut **db_tx)
    .await?;

    Ok(())
}

#[serial_test::serial]
#[cfg(test)]
mod tests {
    use std::env;

    use super::*;

    // TODO: do transactions for all the other functions as well
    // Ideally, we should allow tx or db, however the executor trait is tricky to utilize

    fn get_test_db_connection() -> String {
        env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgresql://postgres:postgres@localhost:5433/fossil_test".to_string()
        })
    }

    #[tokio::test]
    async fn test_update_latest_quick_index_block_number_query() {
        let url = get_test_db_connection();
        let db = DbConnection::new(url).await.unwrap();
        let mut tx = db.pool.begin().await.unwrap();

        sqlx::query(
            "INSERT INTO index_metadata (
                current_latest_block_number,
                indexing_starting_block_number,
                is_backfilling
            ) VALUES (
                123123,
                0,
                false
            )",
        )
        .execute(&mut *tx)
        .await
        .unwrap();

        let result: Result<IndexMetadataDto, sqlx::Error> =
            sqlx::query_as("SELECT * FROM index_metadata")
                .fetch_one(&mut *tx)
                .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap().current_latest_block_number, 123_123);

        update_latest_quick_index_block_number_query(&mut tx, 1)
            .await
            .unwrap();

        let result: Result<IndexMetadataDto, sqlx::Error> =
            sqlx::query_as("SELECT * FROM index_metadata")
                .fetch_one(&mut *tx)
                .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap().current_latest_block_number, 1);

        tx.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn test_update_backfilling_block_number_query() {
        let url = get_test_db_connection();
        let db = DbConnection::new(url).await.unwrap();
        let mut tx = db.pool.begin().await.unwrap();

        sqlx::query(
            "INSERT INTO index_metadata (
                current_latest_block_number,
                indexing_starting_block_number,
                is_backfilling
            ) VALUES (
                123_123,
                0,
                false
            )",
        )
        .execute(&mut *tx)
        .await
        .unwrap();

        let result: Result<IndexMetadataDto, sqlx::Error> =
            sqlx::query_as("SELECT * FROM index_metadata")
                .fetch_one(&mut *tx)
                .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap().backfilling_block_number, None);

        update_backfilling_block_number_query(&mut tx, 100_000)
            .await
            .unwrap();

        let result: Result<IndexMetadataDto, sqlx::Error> =
            sqlx::query_as("SELECT * FROM index_metadata")
                .fetch_one(&mut *tx)
                .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap().backfilling_block_number.unwrap(), 100_000);

        tx.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn test_set_is_backfilling() {
        let url = get_test_db_connection();
        let db = DbConnection::new(url).await.unwrap();
        let mut tx = db.pool.begin().await.unwrap();

        sqlx::query(
            "INSERT INTO index_metadata (
                current_latest_block_number,
                indexing_starting_block_number,
                is_backfilling
            ) VALUES (
                123_123,
                0,
                false
            )",
        )
        .execute(&mut *tx)
        .await
        .unwrap();

        let result: Result<IndexMetadataDto, sqlx::Error> =
            sqlx::query_as("SELECT * FROM index_metadata")
                .fetch_one(&mut *tx)
                .await;

        assert!(result.is_ok());
        assert!(!result.unwrap().is_backfilling);

        set_is_backfilling(&mut tx, true).await.unwrap();

        let result: Result<IndexMetadataDto, sqlx::Error> =
            sqlx::query_as("SELECT * FROM index_metadata")
                .fetch_one(&mut *tx)
                .await;

        assert!(result.is_ok());
        assert!(result.unwrap().is_backfilling);

        tx.rollback().await.unwrap();
    }
}
