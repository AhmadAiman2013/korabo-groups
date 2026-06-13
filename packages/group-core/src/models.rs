use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GroupType {
    Public,
    Private,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RoleType {
    Owner,
    Member,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StatusType {
    Active,
    Pending,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StudyGroup {
    pub group_id: String,
    pub owner_id: String,
    pub name: String,
    pub description: String,
    /// Used as the GSI partition key for efficient subject queries
    pub primary_subject: String,
    /// Additional subject tags stored as a DynamoDB StringSet
    pub subject_tags: Vec<String>,
    /// "public" | "private"
    pub group_type: GroupType,
    pub member_count: i64,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMember {
    pub group_id: String,
    pub user_id: String,
    /// "owner" | "member"
    pub role: RoleType,
    /// "active" | "pending"
    pub status: StatusType,
    pub joined_at: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateGroupRequest {
    pub name: String,
    pub description: String,
    pub primary_subject: String,
    pub subject_tags: Option<Vec<String>>,
    /// "public" | "private"
    pub group_type: GroupType,
}

#[derive(Debug, Deserialize)]
pub struct ListGroupsQuery {
    pub subject: Option<String>,
    pub limit: Option<i32>,
    pub cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TransferOwnershipQuery {
    pub new_owner_id: String,
}

//  ------- SQS Model -----
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "event_type")]
pub enum GroupEvent {
    JoinGroup {
        member: GroupMember,
        group_id: String,
        count_delta: i64,
    },
    LeaveGroup {
        group_id: String,
        user_id: String,
        was_active: bool,
    },
    ApproveMember {
        group_id: String,
    },
    RemoveMember {
        group_id: String,
        user_id: String,
        was_active: bool,
    },
}
