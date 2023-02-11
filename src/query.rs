use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::ops::{BitAnd, BitOr};

use git2::{IndexEntry, ObjectType, Oid, Repository, TreeWalkResult};
use parking_lot::MutexGuard;

use crate::index::Index;
use crate::serialization::DataFormat;
use crate::Collection;

#[derive(Debug, PartialEq)]
pub enum FieldType {
    Int(i64),
    Float(f64),
    String(String),
}

impl From<f64> for FieldType {
    fn from(number: f64) -> Self {
        Self::Float(number)
    }
}

impl From<i64> for FieldType {
    fn from(number: i64) -> Self {
        Self::Int(number)
    }
}

impl From<String> for FieldType {
    fn from(text: String) -> Self {
        Self::String(text)
    }
}

impl From<&str> for FieldType {
    fn from(text: &str) -> Self {
        Self::String(text.to_owned())
    }
}

impl PartialEq<serde_json::Value> for FieldType {
    fn eq(&self, other: &serde_json::Value) -> bool {
        match self {
            FieldType::Float(f) => other.as_f64().map(|x| &x == f).unwrap_or(false),
            FieldType::Int(i) => other.as_i64().map(|x| &x == i).unwrap_or(false),
            FieldType::String(s) => other.as_str().map(|x| &x == s).unwrap_or(false),
        }
    }
}

impl PartialOrd<serde_json::Value> for FieldType {
    fn partial_cmp(&self, other: &serde_json::Value) -> Option<Ordering> {
        match self {
            FieldType::Float(f) => other.as_f64().map(|x| x.partial_cmp(f)).unwrap_or(None),
            FieldType::Int(i) => other.as_i64().map(|x| x.partial_cmp(i)).unwrap_or(None),
            FieldType::String(s) => other.as_str().map(|x| x.partial_cmp(s)).unwrap_or(None),
        }
    }
}

impl PartialOrd<Self> for FieldType {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self {
            FieldType::Float(sf) => match other {
                FieldType::Int(oi) => (*oi as f64).partial_cmp(sf),
                FieldType::Float(of) => of.partial_cmp(sf),
                FieldType::String(_) => None,
            },
            FieldType::Int(si) => match other {
                FieldType::Int(oi) => oi.partial_cmp(si),
                FieldType::Float(of) => (of).partial_cmp(&(*si as f64)),
                FieldType::String(_) => None,
            },
            FieldType::String(ss) => match other {
                FieldType::String(os) => os.partial_cmp(ss),
                _ => None,
            },
        }
    }
}

impl ToString for FieldType {
    fn to_string(&self) -> String {
        match self {
            Self::Int(v) => v.to_string(),
            Self::String(v) => v.to_string(),
            Self::Float(v) => v.to_string(),
        }
    }
}

impl FieldType {
    fn from_index_entry(index_entry: &IndexEntry) -> Option<Self> {
        let val = String::from_utf8_lossy(Index::extract_value(&index_entry));
        let res = match index_entry.ino {
            0 => Some(Self::from(
                f64::from_bits(u64::from_str_radix(&val, 16).ok()?) as i64,
            )),
            1 => Some(Self::from(val.to_string())),
            2 => Some(Self::from(f64::from_bits(
                u64::from_str_radix(&val, 16).ok()?,
            ))),
            _ => None,
        };
        res
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ResolutionStrategy {
    Scan,
    UseIndexes(Vec<Index>),
}

pub struct QueryBuilder {
    query: Option<QueryGroup>,
}

pub fn q<V: Into<FieldType>>(field: &str, comparator: Ordering, value: V) -> QueryGroup {
    QueryGroup {
        next_group: Vec::new(),
        field_query: FieldQuery {
            field: field.to_string(),
            value: value.into(),
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

    fn resolution_strategy<'a, 'b>(
        &'a self,
        indexes: &'b HashMap<String, Index>,
    ) -> ResolutionStrategy
    where
        'b: 'a,
    {
        let mut indexes_used: Vec<Index> = Vec::new();
        match indexes.get(&self.field_query.field) {
            Some(index) => indexes_used.push(index.clone()),
            None => return ResolutionStrategy::Scan,
        }
        for group in &self.next_group {
            match group.0.resolution_strategy(indexes) {
                ResolutionStrategy::Scan => match group.1 {
                    Chain::And => return ResolutionStrategy::UseIndexes(indexes_used),
                    Chain::Or => return ResolutionStrategy::Scan,
                },
                ResolutionStrategy::UseIndexes(mut ind) => indexes_used.append(&mut ind),
            }
        }
        return ResolutionStrategy::UseIndexes(indexes_used);
    }

    fn resolve_with_indexes<'a, 'i, I>(
        &self,
        index_iterator: &mut I,
        repo: &'a MutexGuard<Repository>,
        results: &mut HashSet<Oid>,
        chain: Chain,
        data_format: &DataFormat,
        main_tree: &git2::Tree,
    ) where
        I: Iterator<Item = &'i Index>,
    {
        match index_iterator.next() {
            Some(index) => {
                let git_index = index.git_index(&repo);
                let mut new_res = HashSet::new();
                let mut cur = match self.field_query.comparator {
                    Ordering::Less => 0,
                    Ordering::Equal => {
                        match git_index.find_prefix(self.field_query.prefix_query()) {
                            Ok(v) => v,
                            Err(_) => 0,
                        }
                    }
                    Ordering::Greater => match git_index.len() {
                        0 => 0,
                        _ => git_index.len() - 1,
                    },
                };
                loop {
                    let entry = match git_index.get(cur) {
                        Some(v) => v,
                        None => break,
                    };
                    let val = FieldType::from_index_entry(&entry);
                    match val {
                        Some(v) => {
                            let cmp = self.field_query.value.partial_cmp(&v);
                            if cmp == Some(self.field_query.comparator) {
                                new_res.insert(entry.id);
                            } else if cmp.is_some() {
                                break;
                            }
                        }
                        None => {}
                    }
                    if (cur <= 0 && self.field_query.comparator == Ordering::Greater)
                        || (cur >= git_index.len()
                            && self.field_query.comparator != Ordering::Greater)
                    {
                        break;
                    }
                    match self.field_query.comparator {
                        Ordering::Less => cur += 1,
                        Ordering::Equal => cur += 1,
                        Ordering::Greater => cur -= 1,
                    }
                }
                match chain {
                    Chain::Or => {
                        results.extend(&new_res);
                    }
                    Chain::And => {
                        results.retain(|x| new_res.contains(x));
                    }
                }
                if results.is_empty() {
                    return;
                }
                for g in &self.next_group {
                    g.0.resolve_with_indexes(
                        index_iterator,
                        repo,
                        results,
                        g.1,
                        data_format,
                        main_tree,
                    );
                }
            }
            None => {
                if results.is_empty() {
                    main_tree
                        .walk(git2::TreeWalkMode::PostOrder, |_, entry| {
                            if entry.kind() != Some(ObjectType::Blob) {
                                return TreeWalkResult::Skip;
                            }
                            let blob = entry.to_object(&repo).unwrap();
                            let blob_content = blob.as_blob().unwrap().content();
                            if self.resolve(data_format, blob_content) {
                                results.insert(entry.id());
                            }
                            TreeWalkResult::Ok
                        })
                        .unwrap();
                } else {
                    // scan only matching elements
                    results.retain(|v| {
                        let entry = main_tree.get_id(*v).unwrap();
                        let blob = entry.to_object(&repo).unwrap();
                        let blob_content = blob.as_blob().unwrap().content();
                        self.resolve(data_format, blob_content)
                    });
                }
            }
        }
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

#[derive(Debug, Copy, Clone)]
enum Chain {
    And,
    Or,
}

#[derive(Debug)]
struct FieldQuery {
    field: String,
    value: FieldType,
    comparator: Ordering,
}

impl FieldQuery {
    fn prefix_query(&self) -> String {
        match &self.value {
            FieldType::Int(v) => format!(
                "{}/{:16x}",
                match v.is_positive() {
                    true => 1,
                    false => 0,
                },
                (*v as f64).to_bits()
            ),
            FieldType::Float(v) => format!(
                "{}/{:16x}",
                match v.is_sign_positive() {
                    true => 1,
                    false => 0,
                },
                v.to_bits()
            ),
            FieldType::String(s) => s.to_owned(),
        }
    }
}

pub struct QueryResult {
    pub results: Vec<git2::Oid>,
    pub count: usize,
    pub resolution_strategy: ResolutionStrategy,
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
        let repo = collection.repository();
        let tree = Collection::current_commit(&repo, "main")
            .unwrap()
            .tree()
            .unwrap();
        let all_indexes = Collection::index_field_map(&repo);
        let query = self.query.as_ref().unwrap();
        let resolution_strategy = query.resolution_strategy(&all_indexes);
        let dbg_resolution_strategy = resolution_strategy.clone();
        let indexes_to_use = match resolution_strategy {
            ResolutionStrategy::Scan => Vec::new(),
            ResolutionStrategy::UseIndexes(ind) => ind,
        };
        let mut keys = HashSet::new();
        query.resolve_with_indexes(
            &mut indexes_to_use.iter(),
            &repo,
            &mut keys,
            Chain::Or,
            &collection.data_format,
            &tree,
        );
        let count = keys.len();
        QueryResult {
            results: keys.into_iter().collect(),
            count,
            resolution_strategy: dbg_resolution_strategy,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        index::{Index, IndexType},
        query::{q, QueryBuilder},
        test::*,
        OperationTarget,
    };
    use std::cmp::Ordering::*;

    use super::ResolutionStrategy;

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
            ComplexDbStruct::new(String::from("value"), 22, 1.0),
            OperationTarget::Main,
        );
        db.set(
            "b",
            ComplexDbStruct::new(String::from("value"), 4, 1.0),
            OperationTarget::Main,
        );
        db.set(
            "c",
            ComplexDbStruct::new(String::from("different"), 22, 1.0),
            OperationTarget::Main,
        );

        let query_result = QueryBuilder::new()
            .query(
                q("str_val", Equal, "different")
                    | (q("usize_val", Less, 10) & q("str_val", Equal, "value")),
            )
            .execute(&db);
        assert_eq!(query_result.count, 2);
    }

    #[test]
    fn test_float_number_query() {
        let (db, _td) = create_db();
        db.set(
            "a",
            ComplexDbStruct::new(String::from("value"), 22, 3.14),
            OperationTarget::Main,
        );
        let query_result = QueryBuilder::new()
            .query(q("float_val", Less, 22.1))
            .execute(&db);
        assert_eq!(query_result.count, 1);
    }

    #[test]
    fn test_resolution_strategy_and_index() {
        let (db, _td) = create_db();
        db.add_index("usize_val", IndexType::Numeric, OperationTarget::Main);
        let result = QueryBuilder::new()
            .query(q("usize_val", Equal, 22) & q("str_val", Equal, "qwerty"))
            .execute(&db);
        assert_eq!(
            result.resolution_strategy,
            ResolutionStrategy::UseIndexes(vec![Index::new(
                "usize_val#numeric.index",
                "usize_val",
                IndexType::Numeric
            )])
        )
    }

    #[test]
    fn test_resolution_strategy_or_no_index() {
        let (db, _td) = create_db();
        db.add_index("usize_val", IndexType::Numeric, OperationTarget::Main);
        let result = QueryBuilder::new()
            .query(q("usize_val", Equal, 22) | q("str_val", Equal, "qwerty"))
            .execute(&db);
        assert_eq!(result.resolution_strategy, ResolutionStrategy::Scan)
    }

    #[test]
    fn test_query_results_with_index() {
        let (db, _td) = create_db();
        db.add_index("usize_val", IndexType::Numeric, OperationTarget::Main);
        let result = QueryBuilder::new()
            .query(q("usize_val", Greater, 22))
            .execute(&db);
        db.set(
            "a",
            ComplexDbStruct::new(String::from("value"), 200, 3.14),
            OperationTarget::Main,
        );
        db.set(
            "b",
            ComplexDbStruct::new(String::from("value"), 22, 3.14),
            OperationTarget::Main,
        );
        db.set(
            "c",
            ComplexDbStruct::new(String::from("value"), 0, 3.14),
            OperationTarget::Main,
        );
        assert_eq!(
            result.resolution_strategy,
            ResolutionStrategy::UseIndexes(vec![Index::new(
                "usize_val#numeric.index",
                "usize_val",
                IndexType::Numeric
            )])
        )
    }

    #[test]
    fn test_query_results_every_ordering() {
        let (db, _td) = create_db();
        db.add_index("usize_val", IndexType::Numeric, OperationTarget::Main);
        const INIT_DB_SIZE: usize = 1_000;
        let hm: [usize; INIT_DB_SIZE] = core::array::from_fn(|i| i + 1);
        let hm2 = hm.iter().map(|x| {
            (
                format!("key-{}", x),
                ComplexDbStruct::new(String::from("test value"), *x, *x as f64),
            )
        });
        db.set_batch(hm2, OperationTarget::Main);
        let query_result = QueryBuilder::new()
            .query(
                q("usize_val", Less, 100)
                    | q("usize_val", Equal, 500)
                    | q("usize_val", Greater, 900),
            )
            .execute(&db);
        assert_eq!(query_result.count, 200);
        let index = Index::new("usize_val#numeric.index", "usize_val", IndexType::Numeric);
        assert_eq!(
            query_result.resolution_strategy,
            ResolutionStrategy::UseIndexes(vec![index.clone(), index.clone(), index])
        )
    }
}
