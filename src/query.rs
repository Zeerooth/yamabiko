use std::cmp::Ordering;
use std::fmt::Display;
use std::ops::{BitAnd, BitOr};

use git2::{ObjectType, TreeWalkResult};

use crate::serialization::DataFormat;
use crate::Collection;

pub struct QueryBuilder {
    query: Option<QueryGroup>,
}

pub fn q<V: Display>(field: &str, comparator: Ordering, value: V) -> QueryGroup {
    QueryGroup {
        next_group: Vec::new(),
        field_query: FieldQuery {
            field: field.to_string(),
            value: value.to_string(),
            comparator,
        },
    }
}

#[derive(Debug)]
pub struct QueryGroup {
    next_group: Vec<(QueryGroup, Chain)>,
    field_query: FieldQuery,
}

impl QueryGroup {
    fn resolve(&self, data_format: &DataFormat, data: &[u8]) -> bool {
        let mut result = data_format.match_field(
            data,
            &self.field_query.field,
            &self.field_query.value,
            self.field_query.comparator,
        );
        for group in &self.next_group {
            result = match group.1 {
                Chain::And => result && group.0.resolve(&data_format, &data),
                Chain::Or => result || group.0.resolve(&data_format, &data),
            };
        }
        result
    }
}

impl BitOr for QueryGroup {
    type Output = Self;

    fn bitor(mut self, rhs: Self) -> Self::Output {
        self.next_group.push((rhs, Chain::Or));
        self
    }
}

impl BitAnd for QueryGroup {
    type Output = Self;

    fn bitand(mut self, rhs: Self) -> Self::Output {
        self.next_group.push((rhs, Chain::And));
        self
    }
}

#[derive(Debug)]
enum Chain {
    And,
    Or,
}

#[derive(Debug)]
struct FieldQuery {
    field: String,
    value: String,
    comparator: Ordering,
}

pub struct QueryResult {
    pub results: Vec<String>,
    pub count: usize,
}

impl Iterator for QueryResult {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

impl QueryBuilder {
    pub fn new() -> Self {
        Self { query: None }
    }

    pub fn query(mut self, query: QueryGroup) -> Self {
        self.query = Some(query);
        self
    }

    pub fn execute<'a>(&self, collection: &Collection) -> QueryResult {
        // imply scanning
        let mut keys: Vec<String> = Vec::new();
        let repo = collection.repository();
        let tree = Collection::current_commit(&repo, "main")
            .unwrap()
            .tree()
            .unwrap();
        tree.walk(git2::TreeWalkMode::PostOrder, |_, entry| {
            if entry.kind() != Some(ObjectType::Blob) {
                return TreeWalkResult::Skip;
            }
            let key = entry.name().unwrap().to_string();
            let blob = entry.to_object(&repo).unwrap();
            let blob_content = blob.as_blob().unwrap().content();
            if let Some(query) = &self.query {
                if query.resolve(&collection.data_format, blob_content) {
                    keys.push(key);
                }
            } else {
                keys.push(key)
            }
            TreeWalkResult::Ok
        })
        .unwrap();
        let count = keys.len();
        QueryResult {
            results: keys,
            count,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        query::{q, QueryBuilder},
        test::*,
        OperationTarget,
    };
    use std::cmp::Ordering::*;

    #[test]
    fn test_simple_query() {
        let (db, _td) = create_db();
        db.set(
            "a",
            SampleDbStruct {
                str_val: String::from("value"),
            },
            OperationTarget::Main,
        );
        db.set(
            "b",
            SampleDbStruct {
                str_val: String::from("other value"),
            },
            OperationTarget::Main,
        );
        let query_result = QueryBuilder::new()
            .query(q("str_val", Equal, "value") | q("non_existing_val", Equal, "a"))
            .execute(&db);
        assert_eq!(query_result.count, 1);
    }

    #[test]
    fn test_complex_query() {
        let (db, _td) = create_db();
        db.set(
            "a",
            ComplexDbStruct::new(String::from("value"), 22),
            OperationTarget::Main,
        );
        db.set(
            "b",
            ComplexDbStruct::new(String::from("value"), 4),
            OperationTarget::Main,
        );
        db.set(
            "c",
            ComplexDbStruct::new(String::from("different"), 22),
            OperationTarget::Main,
        );

        let query_result = QueryBuilder::new()
            .query(
                q("str_val", Equal, "different")
                    | (q("usize_val", Less, 10) & q("str_val", Equal, "value")),
            )
            .execute(&db);
        assert_eq!(query_result.count, 1);
    }
}
