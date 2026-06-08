use crate::GroupsRepository;
use axum::Json;
use jwt::JwtPublicKey;
use serde_json::{Value, json};
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub repo: Arc<GroupsRepository>,
    pub groups_table: String,
    pub members_table: String,
    pub jwt: JwtPublicKey,
}

impl AsRef<JwtPublicKey> for AppState {
    fn as_ref(&self) -> &JwtPublicKey {
        &self.jwt
    }
}

pub async fn health_check() -> Json<Value> {
    let health = true;
    match health {
        true => Json(json!({ "status": "healthy" })),
        false => Json(json!({ "status": "unhealthy" })),
    }
}
