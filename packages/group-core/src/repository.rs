use crate::{DynamoDBError, GroupMember, StudyGroup};
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::operation::put_item::PutItemError::ConditionalCheckFailedException as ConditionalCheckPutError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError::ConditionalCheckFailedException as ConditionalCheckUpdateError;
use aws_sdk_dynamodb::types::{AttributeValue, KeysAndAttributes};
use serde_dynamo::aws_sdk_dynamodb_1::from_item;
use serde_dynamo::to_item;
use std::collections::HashMap;

// helpers
fn group_pk(group_id: &str) -> AttributeValue {
    AttributeValue::S(format!("GROUP#{group_id}"))
}

fn metadata_sk() -> AttributeValue {
    AttributeValue::S("METADATA".to_string())
}

fn member_sk(user_id: &str) -> AttributeValue {
    AttributeValue::S(format!("MEMBER#{user_id}"))
}

pub struct GroupsRepository {
    client: Client,
}

impl GroupsRepository {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    // - korabo study groups --- //

    pub async fn put_group(&self, table: &str, group: &StudyGroup) -> Result<(), DynamoDBError> {
        let mut items: HashMap<String, AttributeValue> = to_item(group)?;
        items.insert("PK".to_string(), group_pk(&group.group_id));
        items.insert("SK".to_string(), metadata_sk());
        items.insert("entity_type".to_string(), AttributeValue::S("GROUP".into()));

        self.client
            .put_item()
            .table_name(table)
            .set_item(Some(items))
            .send()
            .await
            .map_err(|e| e.into_service_error())?;

        Ok(())
    }

    pub async fn get_group(
        &self,
        table: &str,
        group_id: &str,
    ) -> Result<StudyGroup, DynamoDBError> {
        let resp = self
            .client
            .get_item()
            .table_name(table)
            .key("PK", group_pk(group_id))
            .key("SK", metadata_sk())
            .send()
            .await
            .map_err(|e| e.into_service_error())?;

        let item = resp
            .item
            .ok_or_else(|| DynamoDBError::NotFound(group_id.to_string()))?;
        from_item(item).map_err(|e| e.into())
    }

    // Query by entity_type GS1, Return all groups
    pub async fn list_all_groups(
        &self,
        table: &str,
        limit: i32,
        cursor: Option<HashMap<String, AttributeValue>>,
    ) -> Result<(Vec<StudyGroup>, Option<HashMap<String, AttributeValue>>), DynamoDBError> {
        let mut req = self
            .client
            .query()
            .table_name(table)
            .index_name("entity_type-index")
            .key_condition_expression("entity_type = :et")
            .expression_attribute_values(":et", AttributeValue::S("GROUP".into()))
            .limit(limit);

        if let Some(start_key) = cursor {
            req = req.set_exclusive_start_key(Some(start_key));
        }

        let resp = req.send().await.map_err(|e| e.into_service_error())?;

        let next_cursor = resp.last_evaluated_key().cloned();

        let groups = resp
            .items()
            .iter()
            .filter_map(|item| from_item::<StudyGroup>(item.clone()).ok())
            .collect();

        Ok((groups, next_cursor))
    }

    // Query by primary_subject GSI. Returns (groups, has_more).
    pub async fn list_groups_by_subject(
        &self,
        table: &str,
        subject: &str,
        limit: i32,
        cursor: Option<HashMap<String, AttributeValue>>,
    ) -> Result<(Vec<StudyGroup>, Option<HashMap<String, AttributeValue>>), DynamoDBError> {
        let mut req = self
            .client
            .query()
            .table_name(table)
            .index_name("subject-index")
            .key_condition_expression("primary_subject = :sub")
            .expression_attribute_values(":sub", AttributeValue::S(subject.to_string()))
            .limit(limit);

        if let Some(start_key) = cursor {
            req = req.set_exclusive_start_key(Some(start_key));
        }

        let resp = req.send().await.map_err(|e| e.into_service_error())?;

        let next_cursor = resp.last_evaluated_key().cloned();
        let groups = resp
            .items()
            .iter()
            .filter_map(|item| from_item::<StudyGroup>(item.clone()).ok())
            .collect();

        Ok((groups, next_cursor))
    }

    pub async fn update_member_count(
        &self,
        table: &str,
        group_id: &str,
        delta: i64,
    ) -> Result<(), DynamoDBError> {
        self.client
            .update_item()
            .table_name(table)
            .key("PK", group_pk(group_id))
            .key("SK", metadata_sk())
            .update_expression("SET member_count = member_count + :d")
            .expression_attribute_values(":d", AttributeValue::N(delta.to_string()))
            .send()
            .await
            .map_err(|e| e.into_service_error())?;

        Ok(())
    }

    // - korabo groups members --- //
    pub async fn get_member(
        &self,
        table: &str,
        group_id: &str,
        user_id: &str,
    ) -> Result<GroupMember, DynamoDBError> {
        let resp = self
            .client
            .get_item()
            .table_name(table)
            .key("PK", group_pk(group_id))
            .key("SK", member_sk(user_id))
            .send()
            .await
            .map_err(|e| e.into_service_error())?;

        match resp.item {
            None => Err(DynamoDBError::NotFound(format!(
                "Membership for user {} in group {} not found",
                user_id, group_id
            ))),
            Some(item) => Ok(from_item(item)?),
        }
    }

    // Insert new member. Condition rejects if (PK, SK) already exists.
    pub async fn put_member(&self, table: &str, member: &GroupMember) -> Result<(), DynamoDBError> {
        let mut items: HashMap<String, AttributeValue> = to_item(member)?;
        items.insert("PK".to_string(), group_pk(&member.group_id));
        items.insert("SK".to_string(), member_sk(&member.user_id));

        self.client
            .put_item()
            .table_name(table)
            .set_item(Some(items))
            .condition_expression("attribute_not_exists(PK) AND attribute_not_exists(SK)")
            .send()
            .await
            .map_err(|e| {
                let service_err = e.into_service_error();
                match &service_err {
                    ConditionalCheckPutError(_) => DynamoDBError::AlreadyExists(format!(
                        "User {} is already a member of group {}",
                        member.user_id, member.group_id
                    )),
                    _ => DynamoDBError::from(service_err),
                }
            })?;

        Ok(())
    }

    pub async fn delete_member(
        &self,
        table: &str,
        group_id: &str,
        user_id: &str,
    ) -> Result<(), DynamoDBError> {
        self.client
            .delete_item()
            .table_name(table)
            .key("PK", group_pk(group_id))
            .key("SK", member_sk(user_id))
            .send()
            .await
            .map_err(|e| e.into_service_error())?;

        Ok(())
    }

    // Conditional  update: only succeeds when status == "pending"
    pub async fn approve_member_status(
        &self,
        table: &str,
        group_id: &str,
        user_id: &str,
    ) -> Result<(), DynamoDBError> {
        self.client
            .update_item()
            .table_name(table)
            .key("PK", group_pk(group_id))
            .key("SK", member_sk(user_id))
            .update_expression("SET #s = :active")
            .expression_attribute_names("#s", "status")
            .expression_attribute_values(":active", AttributeValue::S("active".into()))
            .expression_attribute_values(":pending", AttributeValue::S("pending".into()))
            .condition_expression("#s = :pending")
            .send()
            .await
            .map_err(|e| {
                let service_err = e.into_service_error();
                match &service_err {
                    ConditionalCheckUpdateError(_) => {
                        DynamoDBError::PreconditionFailed("Member status is not 'pending'".into())
                    }
                    _ => DynamoDBError::from(service_err),
                }
            })?;

        Ok(())
    }

    /// Query all SK = MEMBER#* items for a group
    pub async fn list_group_members(
        &self,
        table: &str,
        group_id: &str,
    ) -> Result<Vec<GroupMember>, DynamoDBError> {
        let resp = self
            .client
            .query()
            .table_name(table)
            .key_condition_expression("PK = :pk AND begins_with(SK, :prefix)")
            .expression_attribute_values(":pk", group_pk(group_id))
            .expression_attribute_values(":prefix", AttributeValue::S("MEMBER#".into()))
            .send()
            .await
            .map_err(|e| e.into_service_error())?;

        Ok(resp
            .items()
            .iter()
            .filter_map(|item| from_item::<GroupMember>(item.clone()).ok())
            .collect())
    }

    pub async fn list_user_groups(
        &self,
        members_table: &str,
        groups_table: &str,
        user_id: &str,
    ) -> Result<Vec<StudyGroup>, DynamoDBError> {
        // 1. Query membership index
        let resp = self
            .client
            .query()
            .table_name(members_table)
            .index_name("user_id-index")
            .key_condition_expression("user_id = :uid")
            .filter_expression("#s = :active")
            .expression_attribute_names("#s", "status")
            .expression_attribute_values(":uid", AttributeValue::S(user_id.to_string()))
            .expression_attribute_values(":active", AttributeValue::S("active".into()))
            .send()
            .await
            .map_err(|e| e.into_service_error())?;

        let groups_ids: Vec<String> = resp
            .items()
            .iter()
            .filter_map(|item| from_item::<GroupMember>(item.clone()).ok())
            .map(|m| m.group_id)
            .collect();

        if groups_ids.is_empty() {
            return Ok(vec![]);
        }

        // 2. BatchGetItem - all groups in one request
        let keys: Vec<HashMap<String, AttributeValue>> = groups_ids
            .iter()
            .map(|group_id| {
                let mut key = HashMap::new();
                key.insert("PK".to_string(), group_pk(group_id));
                key.insert("SK".to_string(), metadata_sk());
                key
            })
            .collect();

        let keys_and_attrs = KeysAndAttributes::builder()
            .set_keys(Some(keys))
            .build()
            .map_err(|e| DynamoDBError::BuildError(e.to_string()))?;

        let batch_resp = self
            .client
            .batch_get_item()
            .request_items(groups_table, keys_and_attrs)
            .send()
            .await
            .map_err(|e| e.into_service_error())?;

        let groups = batch_resp
            .responses()
            .and_then(|r| r.get(groups_table))
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| from_item::<StudyGroup>(item.clone()).ok())
                    .collect()
            });

        match groups {
            Some(g) => Ok(g),
            None => Ok(vec![]),
        }
    }

    pub async fn transfer_ownership(
        &self,
        table: &str,
        group_id: &str,
        current_owner_id: &str,
        new_owner_id: &str,
    ) -> Result<(), DynamoDBError> {
        self.client
            .update_item()
            .table_name(table)
            .key("PK", group_pk(group_id))
            .key("SK", metadata_sk())
            .update_expression("SET owner_id = :new_owner")
            .condition_expression("owner_id = :current_owner")
            .expression_attribute_values(":new_owner", AttributeValue::S(new_owner_id.to_string()))
            .expression_attribute_values(
                ":current_owner",
                AttributeValue::S(current_owner_id.to_string()),
            )
            .send()
            .await
            .map_err(|e| e.into_service_error())?;
        Ok(())
    }

    pub async fn query_owner_count(
        &self,
        table: &str,
        owner_id: &str,
    ) -> Result<i64, DynamoDBError> {
        let mut count: i64 = 0;
        let mut last_evaluated_key = None;

        loop {
            let resp = self
                .client
                .query()
                .table_name(table)
                .index_name("owner_id-index")
                .key_condition_expression("owner_id = :owner_id")
                .expression_attribute_values(":owner_id", AttributeValue::S(owner_id.to_string()))
                .select(aws_sdk_dynamodb::types::Select::Count)
                .set_exclusive_start_key(last_evaluated_key)
                .send()
                .await
                .map_err(|e| e.into_service_error())?;

            count += resp.count as i64;

            if resp.last_evaluated_key.is_none() {
                break;
            }
            last_evaluated_key = resp.last_evaluated_key().cloned();
        }
        Ok(count)
    }
}
