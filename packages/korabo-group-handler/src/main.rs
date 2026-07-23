mod group_handler;

use crate::group_handler::{
    create_group, get_group, list_groups, my_groups,
};
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::Client;
use axum::Router;
use axum::http::Method;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::routing::{get, post};
use group_core::{AppState, GroupsRepository, health_check};
use jwt::JwtPublicKey;
use lambda_http::{Error, run, tracing};
use std::env::var;
use std::sync::Arc;
use tower_http::cors::CorsLayer;

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing::init_default_subscriber();

    let jwt = JwtPublicKey::from_jwks_file(
        var("JWT_ISSUER").expect("JWT_ISSUER must be set"),
        var("JWT_AUDIENCE").expect("JWT_AUDIENCE must be set"),
    )
    .expect("Failed to load JWKS");

    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let dynamo = Client::new(&config);
    let groups_table = String::from("korabo_study_groups");
    let members_table = String::from("korabo_group_members");
    let repo = Arc::new(GroupsRepository::new(dynamo));

    let origins = [
        "http://localhost:5173".parse()?,
        "https://koraboweb.online".parse()?,
        "https://d-2rw4lmweh4.execute-api.ap-southeast-1.amazonaws.com".parse()?,
    ];

    let cors = CorsLayer::new()
        .allow_origin(origins)
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers([CONTENT_TYPE, AUTHORIZATION]);

    let state = AppState {
        repo,
        groups_table,
        members_table,
        jwt,
    };

    let app = Router::new()
        .nest(
            "/group",
            Router::new()
                .route("/health", get(health_check))
                .route("/groups", post(create_group).get(list_groups))
                .route("/groups/{group_id}", get(get_group))
                .route("/users/me", get(my_groups))
                .with_state(state),
        )
        .layer(cors);

    run(app).await
}
