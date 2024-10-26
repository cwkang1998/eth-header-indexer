use anyhow::Result;
use axum::{
    extract::Path,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use tracing::info;

use crate::types::ProofWrapper;
use crate::{fossil_mmr, types::Update};

pub struct Error(anyhow::Error);

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Error: {}", self.0),
        )
            .into_response()
    }
}

impl<E> From<E> for Error
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

pub async fn get_mmr_latest() -> Result<Json<Update>, Error> {
    info!("Received request for latest mmr");

    let res = fossil_mmr::get_mmr_stats().await?;
    Ok(Json(res))
}

pub async fn get_mmr_proof(Path(blocknumber): Path<i64>) -> Result<Json<ProofWrapper>, Error> {
    info!("Received request for proof for block {blocknumber}");

    let res = fossil_mmr::get_proof(blocknumber).await?;
    Ok(Json(ProofWrapper { proof: res }))
}
