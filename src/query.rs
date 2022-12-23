use std::cmp::Ordering;
use std::fmt::Display;
use std::path::Path;
use std::sync::Arc;

use git2::Repository;
use parking_lot::{Mutex, MutexGuard};
use serde::Deserialize;

use crate::serialization::DataFormat;
use crate::Collection;

pub struct QueryBuilder {
    fields: Vec<FieldQuery>,
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
        Self { fields: Vec::new() }
    }

    fn field<V: Display>(mut self, field: &str, value: V, comparator: Ordering) -> Self {
        self.fields.push(FieldQuery {
            field: field.to_string(),
            value: value.to_string(),
            comparator,
        });
        self
    }

    fn execute<'a>(&self, collection: &Collection) -> QueryResult {
        // imply scanning
        let mut keys: Vec<String> = Vec::new();
        let mut completed_initial_scan = false;
        let repo = collection.repository();
        let tree = Collection::current_commit(&repo, "main")
            .unwrap()
            .tree()
            .unwrap();
        for field in &self.fields {
            if !completed_initial_scan {
                for obj in repo.index().unwrap().iter() {
                    if obj.mode != 0o100644 {
                        continue;
                    }
                    let path = String::from_utf8(obj.path).unwrap();
                    let key = path.split_at(4).1.to_string();
                    let blob_path = tree.get_path(&Path::new(&path)).ok();
                    let blob = blob_path.unwrap().to_object(&repo).unwrap();
                    let blob_content = blob.as_blob().unwrap().content();
                    if collection.data_format.match_field(
                        blob_content,
                        &field.field,
                        &field.value,
                        field.comparator,
                    ) {
                        keys.push(key);
                    }
                }
                completed_initial_scan = true;
            } else {
                keys.retain(|key| {
                    let blob_path = tree
                        .get_path(&Path::new(&Collection::construct_path_to_key(key)))
                        .ok();
                    let blob = blob_path.unwrap().to_object(&repo).unwrap();
                    let blob_content = blob.as_blob().unwrap().content();
                    collection.data_format.match_field(
                        blob_content,
                        &field.field,
                        &field.value,
                        field.comparator,
                    )
                })
            }
        }
        let count = keys.len();
        QueryResult { data: keys, count }
    }
}

#[cfg(test)]
mod tests {
    use crate::{query::QueryBuilder, test::*, OperationTarget};

    #[test]
    fn test_empty_query() {
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
            .field("str_val", "value", std::cmp::Ordering::Equal)
            .execute(&db);
        assert_eq!(query_result.count, 1);
    }
}
