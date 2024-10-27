use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};

pub struct Error(eyre::Error);

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Error: {}", self.0),
        )
            .into_response()
    }
}
