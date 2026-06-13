use crate::event_handler::AppState;
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::Client;
use group_core::GroupsRepository;
use lambda_runtime::{run, service_fn, tracing, Error};
use std::sync::Arc;

mod event_handler;

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing::init_default_subscriber();

    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = Client::new(&config);
    let groups_table = String::from("korabo_study_groups");
    let members_table = String::from("korabo_group_members");
    let repo = Arc::new(GroupsRepository::new(client));

    let state = AppState {
        repo,
        groups_table,
        members_table,
    };

    run(service_fn(move |event| {
        let state = state.clone();
        async move { event_handler::function_handler(event, state).await }
    }))
    .await
}


