use crate::AppError;
use crate::{GroupEvent, SqsError};
use aws_sdk_sqs::Client;
use serde_json;
use ws_core::types::SqsNotificationEvent;

pub async fn get_parameter(
    ssm_client: &aws_sdk_ssm::Client,
    secret_name: &str,
) -> Result<String, AppError> {
    let resp = ssm_client
        .get_parameter()
        .name(secret_name)
        .with_decryption(false)
        .send()
        .await?;

    if let Some(parameter) = resp.parameter {
        if let Some(value) = parameter.value {
            Ok(value)
        } else {
            Err(AppError::NotFound(format!(
                "Value not found for parameter: {}",
                secret_name
            )))
        }
    } else {
        Err(AppError::NotFound(format!(
            "Parameter not found: {}",
            secret_name
        )))
    }
}

pub async fn publish_sqs_event(
    sqs_client: &Client,
    queue_url: &str,
    event: &GroupEvent,
) -> Result<(), AppError> {
    let body = serde_json::to_string(event)?;

    sqs_client
        .send_message()
        .queue_url(queue_url)
        .message_body(body)
        .send()
        .await
        .map_err(SqsError::SendMessageError)?;

    Ok(())
}

pub async fn publish_sqs_noti_event(
    sqs_client: &Client,
    queue_url: &str,
    event: &SqsNotificationEvent,
) -> Result<(), AppError> {
    let body = serde_json::to_string(event)?;

    sqs_client
        .send_message()
        .queue_url(queue_url)
        .message_body(body)
        .send()
        .await
        .map_err(SqsError::SendMessageError)?;

    Ok(())
}

