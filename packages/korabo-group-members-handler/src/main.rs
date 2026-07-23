mod members_handler;

use crate::members_handler::{
    AppState, approve_member, join_group, leave_group, list_members, remove_member,
    transfer_ownership,
};
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::Client;
use axum::Router;
use axum::http::Method;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::routing::{delete, get, post};
use group_core::{AppState as baseAppState, GroupsRepository, get_parameter, health_check};
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

    let ssm_client = aws_sdk_ssm::Client::new(&config);
    let queue_url = get_parameter(&ssm_client, "/korabo/prod/sqs/group").await?;

    let sqs = aws_sdk_sqs::Client::new(&config);

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
        .allow_headers([CONTENT_TYPE, AUTHORIZATION])
        .allow_credentials(true);

    let base = baseAppState {
        repo,
        groups_table,
        members_table,
        jwt,
    };

    let state = AppState {
        base,
        queue_url,
        sqs,
    };

    let app = Router::new()
        .nest(
            "/members",
            Router::new()
                .route("/health", get(health_check))
                .route("/{group_id}/join", post(join_group))
                .route("/{group_id}/leave", delete(leave_group))
                .route("/{group_id}/members", get(list_members))
                .route(
                    "/{group_id}/members/{user_id}/approve",
                    post(approve_member),
                )
                .route(
                    "/{group_id}/members/{user_id}/remove",
                    delete(remove_member),
                )
                .route(
                    "/{group_id}/members/{user_id}/transfer-ownership",
                    post(transfer_ownership),
                )
                .with_state(state),
        )
        .layer(cors);

    run(app).await
}
