use std::cmp::Ordering;
use std::fmt::Display;
use std::ops::{BitAnd, BitOr};
use std::path::Path;
use std::sync::Arc;

use git2::Repository;
use parking_lot::{Mutex, MutexGuard};
use serde::Deserialize;

use crate::serialization::DataFormat;
use crate::Collection;

pub struct QueryBuilder {
    query: Option<QueryGroup>,
}

pub fn q<V: Display>(field: &str, value: V, comparator: Ordering) -> QueryGroup {
    QueryGroup {
        next_group: Vec::new(),
        field_query: FieldQuery {
            field: field.to_string(),
            value: value.to_string(),
            comparator,
        },
    }
}

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
            }
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

enum Chain {
    And,
    Or,
}

struct FieldQuery {
    field: String,
    value: String,
    comparator: Ordering,
}

pub struct QueryResult {
    data: Vec<String>,
    count: usize,
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

    fn query(mut self, query: QueryGroup) -> Self {
        self.query = Some(query);
        self
    }

    fn execute<'a>(&self, collection: &Collection) -> QueryResult {
        // imply scanning
        let mut keys: Vec<String> = Vec::new();
        let repo = collection.repository();
        let tree = Collection::current_commit(&repo, "main")
            .unwrap()
            .tree()
            .unwrap();
        for obj in repo.index().unwrap().iter() {
            if obj.mode != 0o100644 {
                continue;
            }
            let path = String::from_utf8(obj.path).unwrap();
            let key = path.split_at(4).1.to_string();
            let blob_path = tree.get_path(&Path::new(&path)).ok();
            let blob = blob_path.unwrap().to_object(&repo).unwrap();
            let blob_content = blob.as_blob().unwrap().content();
            if let Some(query) = &self.query {
                if query.resolve(&collection.data_format, blob_content) {
                    keys.push(key);
                }
            } else {
                keys.push(key)
            }
        }

        let count = keys.len();
        QueryResult { data: keys, count }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        query::{q, QueryBuilder},
        test::*,
        OperationTarget,
    };

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
            .query(
                q("str_val", "value", std::cmp::Ordering::Equal)
                    | q("non_existing_val", "a", std::cmp::Ordering::Equal),
            )
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
                q("str_val", "different", std::cmp::Ordering::Equal)
                    | (q("usize_val", 10, std::cmp::Ordering::Less)
                        & q("str_val", "value", std::cmp::Ordering::Equal)),
            )
            .execute(&db);
        assert_eq!(query_result.count, 1);
    }
}
