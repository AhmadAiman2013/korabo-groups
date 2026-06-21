use aws_lambda_events::sqs::SqsEvent;
use aws_sdk_sqs::Client;
use chrono::Utc;
use group_core::{publish_sqs_noti_event, DynamoDBError, GroupEvent, GroupsRepository};
use lambda_runtime::tracing::error;
use lambda_runtime::{Error, LambdaEvent};
use serde_json::from_str;
use std::sync::Arc;
use uuid::Uuid;
use ws_core::types::{NotificationTargeting, SqsNotificationEvent};

#[derive(Clone)]
pub struct AppState {
    pub repo: Arc<GroupsRepository>,
    pub groups_table: String,
    pub members_table: String,
    pub queue_url: String,
    pub sqs: Client,
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
            owner_id,
            count_delta,
        } => {
            state.repo.put_member(&state.members_table, &member).await?;
            match count_delta {
                1 => {
                    state
                        .repo
                        .update_member_count(&state.groups_table, &group_id, count_delta)
                        .await?;
                }
                0 => {
                    let event = SqsNotificationEvent {
                        event_id: Uuid::new_v4().to_string(),
                        event_type: "JoinGroup".to_string(),
                        actor_id: member.user_id,
                        targeting: NotificationTargeting {
                            user_ids: vec![owner_id],
                            group_id: Some(group_id),
                            exclude_user_ids: None,
                        },
                        payload: serde_json::to_value("").unwrap(),
                        created_at: Utc::now().to_rfc3339(),
                    };

                    if let Err(e) =
                        publish_sqs_noti_event(&state.sqs, &state.queue_url, &event).await
                    {
                        error!("Failed to publish SQS notification event: {}", e)
                    }
                }
                _ => {}
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
            
            let group = state.repo.get_group(&state.groups_table, &group_id).await?;

            let event = SqsNotificationEvent {
                event_id: Uuid::new_v4().to_string(),
                event_type: "LeaveGroup".to_string(),
                actor_id: user_id,
                targeting: NotificationTargeting {
                    user_ids: vec![group.owner_id],
                    group_id: Some(group_id),
                    exclude_user_ids: None,
                },
                payload: serde_json::to_value("").unwrap(),
                created_at: Utc::now().to_rfc3339(),
            };

            if let Err(e) =
                publish_sqs_noti_event(&state.sqs, &state.queue_url, &event).await
            {
                error!("Failed to publish SQS notification event: {}", e)
            }
            
            Ok(())
        }
        GroupEvent::ApproveMember {
            group_id,
            user_id,
            owner_id,
        } => {
            state
                .repo
                .update_member_count(&state.groups_table, &group_id, 1)
                .await?;

            let event = SqsNotificationEvent {
                event_id: Uuid::new_v4().to_string(),
                event_type: "ApproveMember".to_string(),
                actor_id: owner_id.to_string(),
                targeting: NotificationTargeting {
                    user_ids: vec![owner_id, user_id],
                    group_id: Some(group_id),
                    exclude_user_ids: None,
                },
                payload: serde_json::to_value("").unwrap(),
                created_at: Utc::now().to_rfc3339(),
            };

            if let Err(e) = publish_sqs_noti_event(&state.sqs, &state.queue_url, &event).await {
                error!("Failed to publish SQS notification event: {}", e)
            }

            Ok(())
        }
        GroupEvent::RemoveMember {
            group_id,
            owner_id,
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

            let event = SqsNotificationEvent {
                event_id: Uuid::new_v4().to_string(),
                event_type: "RemoveMember".to_string(),
                actor_id: owner_id.to_string(),
                targeting: NotificationTargeting {
                    user_ids: vec![user_id],
                    group_id: Some(group_id),
                    exclude_user_ids: None,
                },
                payload: serde_json::to_value("").unwrap(),
                created_at: Utc::now().to_rfc3339(),
            };

            if let Err(e) = publish_sqs_noti_event(&state.sqs, &state.queue_url, &event).await {
                error!("Failed to publish SQS notification event: {}", e)
            }
            Ok(())
        }
    }
}
