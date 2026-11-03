//! OCI distribution error responses.

use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde_json::Value;

/// An error returned to clients as an OCI error body:
/// `{"errors":[{"code":"...","message":"...","detail":...}]}`.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{status} {code}: {message}")]
pub struct RegistryError {
    /// HTTP status code.
    pub status: StatusCode,
    /// OCI error code, e.g. `BLOB_UNKNOWN`.
    pub code: &'static str,
    /// Human readable message.
    pub message: String,
    /// Optional structured detail.
    pub detail: Option<Value>,
}

impl RegistryError {
    /// Creates an error with the given status, code and message.
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            detail: None,
        }
    }

    /// Attaches a `detail` value.
    pub fn with_detail(mut self, detail: Value) -> Self {
        self.detail = Some(detail);
        self
    }

    pub(crate) fn blob_unknown(digest: &str) -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "BLOB_UNKNOWN",
            "blob unknown to registry",
        )
        .with_detail(serde_json::json!({ "digest": digest }))
    }

    pub(crate) fn upload_unknown() -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "BLOB_UPLOAD_UNKNOWN",
            "blob upload unknown to registry",
        )
    }

    pub(crate) fn name_unknown(name: &str) -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "NAME_UNKNOWN",
            "repository name not known to registry",
        )
        .with_detail(serde_json::json!({ "name": name }))
    }

    pub(crate) fn manifest_unknown(reference: &str) -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "MANIFEST_UNKNOWN",
            "manifest unknown",
        )
        .with_detail(serde_json::json!({ "reference": reference }))
    }

    pub(crate) fn digest_invalid(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "DIGEST_INVALID", message)
    }

    pub(crate) fn unsupported(message: impl Into<String>) -> Self {
        Self::new(StatusCode::METHOD_NOT_ALLOWED, "UNSUPPORTED", message)
    }

    pub(crate) fn method_unknown() -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "METHOD_UNKNOWN",
            "We don't understand your method + url",
        )
    }

    pub(crate) fn range_invalid(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::RANGE_NOT_SATISFIABLE,
            "BLOB_UPLOAD_INVALID",
            message,
        )
    }

    pub(crate) fn internal(err: impl std::fmt::Display) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL_SERVER_ERROR",
            err.to_string(),
        )
    }
}

impl From<std::io::Error> for RegistryError {
    fn from(err: std::io::Error) -> Self {
        Self::internal(err)
    }
}

impl IntoResponse for RegistryError {
    fn into_response(self) -> Response {
        let mut entry = serde_json::json!({ "code": self.code, "message": self.message });
        if let Some(detail) = self.detail {
            entry["detail"] = detail;
        }
        let body = serde_json::to_vec(&serde_json::json!({ "errors": [entry] }))
            .expect("error body serializes");
        let mut resp = (self.status, body).into_response();
        resp.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        resp
    }
}
