use aws_lambda_events::sqs::SqsEvent;
use group_core::{DynamoDBError, GroupEvent, GroupsRepository};
use lambda_runtime::tracing::error;
use lambda_runtime::{Error, LambdaEvent};
use serde_json::from_str;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub repo: Arc<GroupsRepository>,
    pub groups_table: String,
    pub members_table: String,
}

pub async fn function_handler(event: LambdaEvent<SqsEvent>, state: AppState) -> Result<(), Error> {
    for record in event.payload.records {
        let body = match record.body {
            Some(b) => b,
            None => {
                error!(
                    "Received SQS record with no body, message_id: {:?}",
                    record.message_id
                );
                continue;
            }
        };

        let group_event: GroupEvent = match from_str(&body) {
            Ok(e) => e,
            Err(err) => {
                error!("Failed to deserialize message: {:?} body: {}", err, body);
                continue;
            }
        };

        if let Err(err) = process_event(&state, group_event).await {
            error!(
                "Failed to process message_id {:?}: {}",
                record.message_id, err
            );
            return Err(err.into());
        }
    }
    Ok(())
}

async fn process_event(state: &AppState, msg: GroupEvent) -> Result<(), DynamoDBError> {
    match msg {
        GroupEvent::JoinGroup {
            member,
            group_id,
            count_delta,
        } => {
            state.repo.put_member(&state.members_table, &member).await?;
            if count_delta > 0 {
                state
                    .repo
                    .update_member_count(&state.groups_table, &group_id, count_delta)
                    .await?;
            }
            Ok(())
        }
        GroupEvent::LeaveGroup {
            group_id,
            user_id,
            was_active,
        } => {
            state
                .repo
                .delete_member(&state.members_table, &group_id, &*user_id)
                .await?;
            if was_active {
                state
                    .repo
                    .update_member_count(&state.groups_table, &group_id, -1)
                    .await?;
            }
            Ok(())
        }
        GroupEvent::ApproveMember { group_id } => {
            state
                .repo
                .update_member_count(&state.groups_table, &group_id, 1)
                .await?;
            Ok(())
        }
        GroupEvent::RemoveMember {
            group_id,
            user_id,
            was_active,
        } => {
            state
                .repo
                .delete_member(&state.members_table, &group_id, &*user_id)
                .await?;
            if was_active {
                state
                    .repo
                    .update_member_count(&state.groups_table, &group_id, -1)
                    .await?;
            }
            Ok(())
        }
    }
}
