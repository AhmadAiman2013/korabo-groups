use aws_sdk_sqs::Client;
use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use chrono::Utc;
use group_core::RoleType::{Member, Owner};
use group_core::StatusType::{Active, Pending};
use group_core::{
    AppError,
    AppState as baseAppState,
    GroupEvent,
    GroupMember,
    GroupType,
    TransferOwnershipQuery,
    publish_sqs_event,
};
use jwt::AuthClaims;
use jwt::JwtPublicKey;
use serde_json::{Value, json};
use std::ops::Deref;

#[derive(Clone)]
pub struct AppState {
    pub base: baseAppState,
    pub queue_url: String,
    pub sqs: Client,
}

impl Deref for AppState {
    type Target = baseAppState;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl AsRef<JwtPublicKey> for AppState {
    fn as_ref(&self) -> &JwtPublicKey {
        self.base.as_ref()
    }
}

// POST /members/{group_id}/join
pub async fn join_group(
    State(state): State<AppState>,
    AuthClaims(claims): AuthClaims,
    Path(group_id): Path<String>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let group = state.repo.get_group(&state.groups_table, &group_id).await?;
    let owner_id = group.owner_id;
    let user_id = &claims.sub;

    if state
        .repo
        .get_member(&state.members_table, &group_id, user_id)
        .await
        .is_ok()
    {
        return Err(AppError::Conflict(
            "User is already a member of the group".to_string(),
        ));
    }

    let (status, count_delta, msg) = match group.group_type {
        GroupType::Public => (Active, 1, "Successfully joined the group."),
        GroupType::Private => (
            Pending,
            0,
            "Your request to join the group is pending approval.",
        ),
    };

    let member = GroupMember {
        group_id: group_id.clone(),
        user_id: user_id.to_string(),
        role: Member,
        status,
        joined_at: Utc::now().to_rfc3339(),
    };

    publish_sqs_event(&state.sqs, &state.queue_url, &GroupEvent::JoinGroup { member, group_id, owner_id, count_delta }).await?;

    Ok((StatusCode::CREATED, Json(json!({"message": msg}))))
}

// DEL /members/{group_id}/leave
pub async fn leave_group(
    State(state): State<AppState>,
    AuthClaims(claims): AuthClaims,
    Path(group_id): Path<String>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let user_id = &claims.sub;
    let member = state
        .repo
        .get_member(&state.members_table, &group_id, user_id)
        .await?;

    if matches!(&member.role, &Owner) {
        let owner_count = state
            .repo
            .query_owner_count(&state.groups_table, &group_id)
            .await?;

        if owner_count <= 1 {
            return Err(AppError::BadRequest(
                "Group owner cannot leave the group. Please transfer ownership or delete the group."
                    .to_string(),
            ));
        }
    }

    let was_active = matches!(&member.status, &Active);

    publish_sqs_event(&state.sqs, &state.queue_url, &GroupEvent::LeaveGroup { group_id, user_id: user_id.to_owned(), was_active }).await?;

    Ok((
        StatusCode::OK,
        Json(json!({"message": "Successfully left the group."})),
    ))
}

// GET /members/{group_id}/members
pub async fn list_members(
    State(state): State<AppState>,
    AuthClaims(claims): AuthClaims,
    Path(group_id): Path<String>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let requester = state
        .repo
        .get_member(&state.members_table, &group_id, &claims.sub)
        .await?;

    if matches!(requester.status, Pending) {
        return Err(AppError::Forbidden);
    }

    let raw_members = state
        .repo
        .list_group_members(&state.members_table, &group_id)
        .await?;

    let is_owner = matches!(requester.role, Owner);

    // filter out pending if not owner
    let members: Vec<GroupMember> = raw_members
        .into_iter()
        .filter(|member| is_owner || matches!(member.status, Active))
        .collect();

    let count = members.len();
    Ok((
        StatusCode::OK,
        Json(json!({ "members": members, "count": count, "is_owner": is_owner })),
    ))
}

// POST /members/{group_id}/members/{user_id}/approve
pub async fn approve_member(
    State(state): State<AppState>,
    AuthClaims(claims): AuthClaims,
    Path((group_id, user_id)): Path<(String, String)>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let requester = state
        .repo
        .get_member(&state.members_table, &group_id, &claims.sub)
        .await?;

    if !matches!(requester.role, Owner) {
        return Err(AppError::Forbidden);
    }

    state
        .repo
        .approve_member_status(&state.members_table, &group_id, &user_id)
        .await?;

    publish_sqs_event(&state.sqs, &state.queue_url, &GroupEvent::ApproveMember { group_id, user_id, owner_id: requester.user_id }).await?;

    Ok((
        StatusCode::OK,
        Json(json!({"message": "Member approval is being processed"})),
    ))
}

// DEL /members/{group_id}/members/{user_id}/remove
pub async fn remove_member(
    State(state): State<AppState>,
    AuthClaims(claims): AuthClaims,
    Path((group_id, user_id)): Path<(String, String)>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let requester = state
        .repo
        .get_member(&state.members_table, &group_id, &claims.sub)
        .await?;

    if !matches!(requester.role, Owner) {
        return Err(AppError::Forbidden);
    }

    let target = state
        .repo
        .get_member(&state.members_table, &group_id, &user_id)
        .await?;

    if matches!(&target.role, &Owner) {
        return Err(AppError::BadRequest(
            "Cannot remove the group owner. Please transfer ownership or delete the group."
                .to_string(),
        ));
    }

    let was_active = matches!(&target.status, &Active);

    publish_sqs_event(&state.sqs, &state.queue_url, &GroupEvent::RemoveMember { group_id, user_id: user_id.to_owned(), was_active }).await?;

    Ok((
        StatusCode::OK,
        Json(json!({"message": "Successfully removed the group." })),
    ))
}

// POST /members/{group_id}/members/{user_id}/transfer-ownership
pub async fn transfer_ownership(
    State(state): State<AppState>,
    AuthClaims(claims): AuthClaims,
    Path((group_id, user_id)): Path<(String, String)>,
    Json(body): Json<TransferOwnershipQuery>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let requester = state
        .repo
        .get_member(&state.members_table, &group_id, &claims.sub)
        .await?;

    if !matches!(requester.role, Owner) {
        return Err(AppError::Forbidden);
    }

    state
        .repo
        .transfer_ownership(
            &state.groups_table,
            &group_id,
            &user_id,
            &*body.new_owner_id,
        )
        .await?;

    Ok((
        StatusCode::OK,
        Json(json!({"message": "Ownership transferred successfully."})),
    ))
}
