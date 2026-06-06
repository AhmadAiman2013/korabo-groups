use aws_sdk_dynamodb::Error as DynamoError;
use aws_sdk_dynamodb::operation::batch_get_item::BatchGetItemError;
use aws_sdk_dynamodb::operation::delete_item::DeleteItemError;
use aws_sdk_dynamodb::operation::get_item::GetItemError;
use aws_sdk_dynamodb::operation::put_item::PutItemError;
use aws_sdk_dynamodb::operation::query::QueryError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use lambda_http::tracing::error;
use serde_dynamo::Error;
use serde_json::json;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DynamoDBError {
    #[error("DynamoDB error: {0}")]
    DynamoDB(#[from] DynamoError),

    #[error("DynamoDB put item error: {0}")]
    PutItem(#[from] PutItemError),

    #[error("DynamoDB get item error: {0}")]
    GetItem(#[from] GetItemError),

    #[error("DynamoDB batch get item error: {0}")]
    BatchGetItem(#[from] BatchGetItemError),

    #[error("Query error: {0}")]
    QueryError(#[from] QueryError),

    #[error("Update error: {0}")]
    UpdateError(#[from] UpdateItemError),

    #[error("Delete error: {0}")]
    DeleteError(#[from] DeleteItemError),

    #[error("Serialization error: {0}")]
    Serialization(#[from] Error),

    #[error("Build error: {0}")]
    BuildError(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Already exist: {0}")]
    AlreadyExists(String),

    #[error("Precondition failed: {0}")]
    PreconditionFailed(String),
}

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Forbidden")]
    Forbidden,
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    BadRequest(String),
    #[error("Internal server error")]
    Internal(String),

    #[error(transparent)]
    Repository(#[from] DynamoDBError),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, body) = match &self {
            AppError::Forbidden => (StatusCode::FORBIDDEN, self.to_string()),
            AppError::Conflict(msg) => (StatusCode::CONFLICT, msg.clone()),
            AppError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            AppError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.clone()),
            AppError::Repository(DynamoDBError::NotFound(id)) => (StatusCode::NOT_FOUND, id.clone()),
            AppError::Repository(e) => {
                error!("Repository error: {:?}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
            }
        };
        (status, Json(json!({ "error": body }))).into_response()
    }
}
