use aws_sdk_sqs::Client;
use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use chrono::Utc;
use group_core::RoleType::{Member, Owner};
use group_core::StatusType::{Active, Pending};
use group_core::GroupType::{Private, Public};
use group_core::{
    AppError, AppState as baseAppState, DynamoDBError, GroupEvent, GroupMember,
    TransferOwnershipQuery, publish_sqs_event,
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

// Helper to load an optional membership for a user. Converts NotFound -> None,
// and propagates other repository errors as AppError.
async fn get_member_optional(
    state: &AppState,
    group_id: &str,
    user_id: &str,
) -> Result<Option<GroupMember>, AppError> {
    match state
        .repo
        .get_member(&state.members_table, &group_id.to_string(), user_id)
        .await
    {
        Ok(m) => Ok(Some(m)),
        Err(e) => match e {
            DynamoDBError::NotFound(_) => Ok(None),
            _ => Err(e.into()),
        },
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
        Public => (Active, 1, "Successfully joined the group."),
        Private => (
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

    publish_sqs_event(
        &state.sqs,
        &state.queue_url,
        &GroupEvent::JoinGroup {
            member,
            group_id,
            owner_id,
            count_delta,
        },
    )
    .await?;

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
        return Err(AppError::BadRequest(
            "Group owner cannot leave the group. Please transfer ownership or delete the group."
                .to_string(),
        ));
    }

    let was_active = matches!(&member.status, &Active);

    publish_sqs_event(
        &state.sqs,
        &state.queue_url,
        &GroupEvent::LeaveGroup {
            group_id,
            user_id: user_id.to_owned(),
            was_active,
        },
    )
    .await?;

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
    let group = state.repo.get_group(&state.groups_table, &group_id).await?;

    // Try to load the requester membership. If it's not found treat as None (not a member).
    // Any other repository error should be propagated.
    let requester_opt = get_member_optional(&state, &group_id, &claims.sub).await?;

    let is_owner = requester_opt.as_ref().map(|r| matches!(r.role, Owner)).unwrap_or(false);

    // Access rules for private groups: non-members cannot list members. Pending members
    // are not allowed to list other members either (only owner/active members may).
    if matches!(group.group_type, Private) {
        let req = requester_opt.as_ref().ok_or(AppError::Forbidden)?;
        if matches!(req.status, Pending) && !is_owner {
            return Err(AppError::Forbidden);
        }
    }

    let raw_members = state
        .repo
        .list_group_members(&state.members_table, &group_id)
        .await?;

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

    publish_sqs_event(
        &state.sqs,
        &state.queue_url,
        &GroupEvent::ApproveMember {
            group_id,
            user_id,
            owner_id: requester.user_id,
        },
    )
    .await?;

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

    publish_sqs_event(
        &state.sqs,
        &state.queue_url,
        &GroupEvent::RemoveMember {
            group_id,
            owner_id: requester.user_id,
            user_id: user_id.to_owned(),
            was_active,
        },
    )
    .await?;

    Ok((
        StatusCode::OK,
        Json(json!({"message": format!("Successfully removed {} from group.", user_id) })),
    ))
}

// POST /members/{group_id}/members/{user_id}/transfer-ownership
pub async fn transfer_ownership(
    State(state): State<AppState>,
    AuthClaims(claims): AuthClaims,
    Path((group_id, user_id)): Path<(String, String)>,
    Json(body): Json<TransferOwnershipQuery>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    // The path segment must represent the caller themselves (self-transfer-out semantics).
    // Reject early and clearly if the client sent someone else's id here — don't silently
    // use the wrong value for to write.

    eprintln!(
        "TRANSFER_DEBUG group_id={} path_user_id={} claims_sub={} new_owner_id={}",
        group_id, user_id, claims.sub, body.new_owner_id
    );

    if user_id != claims.sub {
        return Err(AppError::Forbidden);
    }

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
            &claims.sub,
            &*body.new_owner_id,
        )
        .await?;

    Ok((
        StatusCode::OK,
        Json(json!({"message": "Ownership transferred successfully."})),
    ))
}

pub async fn get_my_membership(
    State(state): State<AppState>,
    AuthClaims(claims): AuthClaims,
    Path(group_id): Path<String>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let member = get_member_optional(&state, &group_id, &claims.sub).await?;

    match member {
        Some(m) => Ok((
            StatusCode::OK,
            Json(json!({
                "is_member": true,
                "role": m.role,
                "status": m.status,
            })),
        )),
        None => Ok((
            StatusCode::OK,
            Json(json!({
                "is_member": false,
                "role": Value::Null,
                "status": Value::Null,
            })),
        )),
    }
}
