use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use chrono::Utc;
use group_core::RoleType::{Member, Owner};
use group_core::StatusType::{Active, Pending};
use group_core::{AppError, AppState, GroupMember, GroupType};
use jwt::AuthClaims;
use serde_json::{Value, json};

// POST /members/{group_id}/join
pub async fn join_group(
    State(state): State<AppState>,
    AuthClaims(claims): AuthClaims,
    Path(group_id): Path<String>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let group = state.repo.get_group(&state.groups_table, &group_id).await?;
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

    state.repo.put_member(&state.members_table, &member).await?;

    if count_delta > 0 {
        state
            .repo
            .update_member_count(&state.groups_table, &group_id, count_delta)
            .await?;
    }

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

    state
        .repo
        .delete_member(&state.members_table, &group_id, user_id)
        .await?;

    if matches!(&member.status, &Active) {
        state
            .repo
            .update_member_count(&state.groups_table, &group_id, -1)
            .await?;
    }

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

    let members = state
        .repo
        .list_group_members(&state.members_table, &group_id)
        .await?;
    let count = members.len();
    Ok((
        StatusCode::OK,
        Json(json!({ "members": members, "count": count })),
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
    state
        .repo
        .update_member_count(&state.groups_table, &group_id, 1)
        .await?;
    Ok((
        StatusCode::OK,
        Json(json!({"message": "Member approved successfully."})),
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

    if !matches!(requester.role, Owner) && claims.sub != user_id {
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

    state
        .repo
        .delete_member(&state.members_table, &group_id, &user_id)
        .await?;

    if matches!(&target.status, &Active) {
        state
            .repo
            .update_member_count(&state.groups_table, &group_id, -1)
            .await?;
    }

    Ok((
        StatusCode::OK,
        Json(json!({"message": "Successfully removed the group." })),
    ))
}
