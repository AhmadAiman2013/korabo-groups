use aws_sdk_dynamodb::types::AttributeValue;
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::Utc;
use group_core::{
    AppError, AppState, CreateGroupRequest, GroupMember, ListGroupsQuery, RoleType, StatusType,
    StudyGroup,
};
use jwt::AuthClaims;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use uuid::Uuid;

// POST /group/groups

pub async fn create_group(
    State(state): State<AppState>,
    AuthClaims(claims): AuthClaims,
    Json(body): Json<CreateGroupRequest>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    if body.name.trim().is_empty() || body.primary_subject.trim().is_empty() {
        return Err(AppError::BadRequest(
            "Group name cannot be empty".to_string(),
        ));
    }

    let group_id = Uuid::new_v4().to_string();
    let now = Utc::now().to_rfc3339();

    let user_id = claims.sub.clone();
    let subject_tags = body.subject_tags.unwrap_or_else(|| Vec::new());

    let group = StudyGroup {
        group_id: group_id.clone(),
        owner_id: user_id.clone(),
        name: body.name,
        description: body.description,
        primary_subject: body.primary_subject,
        subject_tags,
        group_type: body.group_type,
        member_count: 1,
        created_at: now.clone(),
    };

    state.repo.put_group(&state.groups_table, &group).await?;

    let owner = GroupMember {
        group_id,
        user_id,
        role: RoleType::Owner,
        status: StatusType::Active,
        joined_at: now,
    };

    state.repo.put_member(&state.members_table, &owner).await?;

    Ok((
        StatusCode::CREATED,
        Json(json!({
            "group_id": group.group_id
        })),
    ))
}

// GET /group/groups

pub async fn list_groups(
    State(state): State<AppState>,
    AuthClaims(_claims): AuthClaims,
    Query(params): Query<ListGroupsQuery>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let limit = params.limit.unwrap_or(20).clamp(1, 50);

    // Decode cursor from query param if present
    let cursor = params.cursor.as_deref().map(decode_cursor).transpose()?;

    let (groups, next_key) = match params.subject {
        Some(ref subject) => {
            state
                .repo
                .list_groups_by_subject(&state.groups_table, subject, limit, cursor)
                .await?
        }
        None => {
            state
                .repo
                .list_all_groups(&state.groups_table, limit, cursor)
                .await?
        }
    };

    let next_cursor = next_key.as_ref().map(encode_cursor).transpose()?;

    let count = groups.len();
    Ok((
        StatusCode::OK,
        Json(json!({
            "groups": groups,
            "nextCursor": next_cursor,
            "count": count,
        })),
    ))
}

fn encode_cursor(key: &HashMap<String, AttributeValue>) -> Result<String, AppError> {
    let simple: BTreeMap<String, String> = key
        .iter()
        .filter_map(|(k, v)| v.as_s().ok().map(|s| (k.clone(), s.clone())))
        .collect();

    let json_str = serde_json::to_string(&simple)
        .map_err(|e| AppError::Internal(format!("Cursor encoding error: {}", e)))?;
    Ok(URL_SAFE_NO_PAD.encode(json_str))
}

fn decode_cursor(cursor: &str) -> Result<HashMap<String, AttributeValue>, AppError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|e| AppError::Internal(format!("Cursor decoding error: {}", e)))?;

    let simple: BTreeMap<String, String> = serde_json::from_slice(&bytes)
        .map_err(|e| AppError::Internal(format!("Deserialize cursor error: {}", e)))?;
    Ok(simple
        .into_iter()
        .map(|(k, v)| (k, AttributeValue::S(v)))
        .collect())
}

pub async fn get_group(
    State(state): State<AppState>,
    AuthClaims(_claims): AuthClaims,
    Path(group_id): Path<String>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let group = state.repo.get_group(&state.groups_table, &group_id).await?;

    Ok((
        StatusCode::OK,
        Json(json!({
            "group": group
        })),
    ))
}

pub async fn my_groups(
    State(state): State<AppState>,
    AuthClaims(claims): AuthClaims,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let groups = state
        .repo
        .list_user_groups(&state.members_table, &state.groups_table, &claims.sub)
        .await?;

    Ok((StatusCode::OK, Json(json!({ "groups": groups }))))
}
