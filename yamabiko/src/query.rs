use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::ops::{BitAnd, BitOr};

use git2::{ObjectType, Oid, Repository, Tree, TreeWalkResult};

use crate::field::Field;
use crate::index::Index;
use crate::serialization::DataFormat;
use crate::{debug, error, Collection, RepositoryAbstraction};

#[derive(Debug, Clone, PartialEq)]
pub enum ResolutionStrategy {
    Scan,
    UseIndexes(Vec<Index>),
}

#[derive(Default)]
pub struct QueryBuilder {
    query: Option<QueryGroup>,
    limit: Option<usize>,
}

pub fn q<V: Into<Field>>(field: &str, comparator: Ordering, value: V) -> QueryGroup {
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
                Chain::And => result && group.0.resolve(data_format, data),
                Chain::Or => result || group.0.resolve(data_format, data),
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
        ResolutionStrategy::UseIndexes(indexes_used)
    }

    #[allow(clippy::too_many_arguments)]
    fn resolve_with_indexes<'a, 'i, I>(
        &self,
        index_iterator: &mut I,
        repo: &Repository,
        results: &mut HashSet<Oid>,
        chain: Chain,
        data_format: &DataFormat,
        main_tree: &git2::Tree,
        limit: usize,
    ) -> Result<(), error::QueryError>
    where
        I: Iterator<Item = &'i Index>,
    {
        match index_iterator.next() {
            Some(index) => {
                let git_index = index.git_index(repo);
                let mut new_res = HashSet::new();
                let mut cur = match self.field_query.comparator {
                    Ordering::Less => 0,
                    Ordering::Equal => git_index
                        .find_prefix(self.field_query.prefix_query())
                        .unwrap_or(0),
                    Ordering::Greater => match git_index.len() {
                        0 => 0,
                        _ => git_index.len() - 1,
                    },
                };
                while let Some(entry) = git_index.get(cur) {
                    let val = Field::from_index_entry(&entry);
                    debug!("found the following value in the index: {:?}", val);
                    if let Some(v) = val {
                        let cmp = self.field_query.value.partial_cmp(&v);
                        if cmp == Some(self.field_query.comparator) {
                            new_res.insert(entry.id);
                        } else if cmp.is_some() {
                            break;
                        }
                    }
                    if (cur == 0 && self.field_query.comparator == Ordering::Greater)
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
                    return Ok(());
                }
                for g in &self.next_group {
                    g.0.resolve_with_indexes(
                        index_iterator,
                        repo,
                        results,
                        g.1,
                        data_format,
                        main_tree,
                        limit,
                    )?;
                }
            }
            None => {
                debug!("No index; Scanning...");
                if results.is_empty() {
                    main_tree.walk(git2::TreeWalkMode::PostOrder, |_, entry| {
                        debug!("Found an entry {}", entry.id());
                        let entry_kind = entry.kind();
                        if entry_kind != Some(ObjectType::Blob) {
                            debug!("Type is {:?}, skipping", entry_kind);
                            return TreeWalkResult::Skip;
                        }
                        let blob = entry.to_object(repo).unwrap();
                        let blob_content = blob.as_blob().unwrap().content();
                        if self.resolve(data_format, blob_content) {
                            results.insert(entry.id());
                        }
                        if results.len() >= limit {
                            return TreeWalkResult::Abort;
                        }
                        TreeWalkResult::Ok
                    })?;
                } else {
                    // scan only matching elements
                    let mut retained = 0;
                    results.retain(|v| {
                        if retained >= limit {
                            return false;
                        }
                        let entry = main_tree.get_id(*v).unwrap();
                        let blob = entry.to_object(repo).unwrap();
                        let blob_content = blob.as_blob().unwrap().content();
                        let res = self.resolve(data_format, blob_content);
                        if res {
                            retained += 1;
                        }
                        res
                    });
                }
            }
        }
        Ok(())
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
    value: Field,
    comparator: Ordering,
}

impl FieldQuery {
    fn prefix_query(&self) -> String {
        match &self.value {
            Field::Int(v) => format!(
                "{}/{:16x}",
                match v.is_positive() {
                    true => 1,
                    false => 0,
                },
                (*v as f64).to_bits()
            ),
            Field::Float(v) => format!(
                "{}/{:16x}",
                match v.is_sign_positive() {
                    true => 1,
                    false => 0,
                },
                v.to_bits()
            ),
            Field::String(s) => s.to_owned(),
        }
    }
}

pub struct QueryResult {
    pub results: HashSet<git2::Oid>,
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
    /// Create QueryBuilder with the set query expression
    pub fn query(query: QueryGroup) -> Self {
        Self {
            query: Some(query),
            limit: None,
        }
    }

    /// Create a QueryBuilder that returns all keys in the collection
    pub fn all() -> Self {
        Self {
            query: None,
            limit: None,
        }
    }

    // Set the optional limit to the results returned
    // This can greatly reduce query times when scanning the collection
    // Note that if there is no advantage to be gained from the limit, more results will be returned
    pub fn maybe_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn resultion_strategy(
        &self,
        collection: &Collection,
    ) -> Result<ResolutionStrategy, error::QueryError> {
        let repo = collection.repository();
        let resolution_strategy = match &self.query {
            Some(q) => {
                let all_indexes = Collection::index_field_map(repo);
                q.resolution_strategy(&all_indexes)
            }
            None => ResolutionStrategy::Scan,
        };
        Ok(resolution_strategy)
    }

    fn walk_the_tree(
        results: &mut HashSet<Oid>,
        tree: Tree,
        limit: Option<usize>,
    ) -> Result<(), git2::Error> {
        tree.walk(git2::TreeWalkMode::PostOrder, |_, entry| {
            debug!("Found an entry {}", entry.id());
            let entry_kind = entry.kind();
            if entry_kind != Some(ObjectType::Blob) {
                debug!("Type is {:?}, skipping", entry_kind);
                return TreeWalkResult::Skip;
            }
            results.insert(entry.id());
            if let Some(limit) = limit {
                if results.len() >= limit {
                    return TreeWalkResult::Abort;
                }
            }
            TreeWalkResult::Ok
        })
    }

    pub fn execute(&self, collection: &Collection) -> Result<QueryResult, error::QueryError> {
        let repo = collection.repository();
        let resolution_strategy = self.resultion_strategy(collection)?;
        debug!(
            "determined the resolution strategy: {:?}",
            resolution_strategy.clone()
        );
        let mut keys = HashSet::new();
        let tree = Collection::current_commit(repo, "main")?.tree()?;
        if let Some(query) = &self.query {
            let indexes_to_use = match resolution_strategy {
                ResolutionStrategy::Scan => Vec::new(),
                ResolutionStrategy::UseIndexes(ref ind) => ind.clone(),
            };
            debug!("executing a query: {:?}", query);
            query.resolve_with_indexes(
                &mut indexes_to_use.iter(),
                repo,
                &mut keys,
                Chain::Or,
                &collection.data_format,
                &tree,
                self.limit.unwrap_or(usize::MAX),
            )?;
        } else {
            Self::walk_the_tree(&mut keys, tree, self.limit)?;
        }
        let count = keys.len();
        Ok(QueryResult {
            results: keys,
            count,
            resolution_strategy,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        index::{Index, IndexType},
        query::{q, QueryBuilder},
        serialization::DataFormat,
        test::*,
        OperationTarget,
    };
    use rstest::rstest;
    use std::cmp::Ordering::*;

    use super::ResolutionStrategy;

    #[rstest]
    #[case(DataFormat::Json)]
    #[case(DataFormat::Yaml)]
    #[case(DataFormat::Pot)]
    fn test_simple_query(#[case] data_format: DataFormat) {
        let (db, _td) = create_db(data_format);
        db.set(
            "a",
            SampleDbStruct {
                str_val: String::from("value"),
            },
            OperationTarget::Main,
        )
        .unwrap();
        db.set(
            "b",
            SampleDbStruct {
                str_val: String::from("other value"),
            },
            OperationTarget::Main,
        )
        .unwrap();
        let query_result =
            QueryBuilder::query(q("str_val", Equal, "value") | q("non_existing_val", Equal, "a"))
                .execute(&db)
                .unwrap();
        assert_eq!(query_result.count, 1);
        let oid = query_result.results.iter().next().unwrap();
        let obj = db.get_by_oid::<SampleDbStruct>(*oid);
        assert_eq!(obj.unwrap().unwrap().str_val, "value");
    }

    #[rstest]
    #[case(DataFormat::Json)]
    #[case(DataFormat::Yaml)]
    #[case(DataFormat::Pot)]
    fn test_complex_query(#[case] data_format: DataFormat) {
        let (db, _td) = create_db(data_format);
        db.set(
            "a",
            ComplexDbStruct::new(String::from("value"), 22, 1.0),
            OperationTarget::Main,
        )
        .unwrap();
        db.set(
            "b",
            ComplexDbStruct::new(String::from("value"), 4, 1.0),
            OperationTarget::Main,
        )
        .unwrap();
        db.set(
            "c",
            ComplexDbStruct::new(String::from("different"), 22, 1.0),
            OperationTarget::Main,
        )
        .unwrap();
        let query_result = QueryBuilder::query(
            q("str_val", Equal, "different")
                | (q("usize_val", Less, 10) & q("str_val", Equal, "value")),
        )
        .execute(&db)
        .unwrap();
        assert_eq!(query_result.count, 2);
    }

    #[rstest]
    #[case(DataFormat::Json)]
    #[case(DataFormat::Yaml)]
    #[case(DataFormat::Pot)]
    fn test_float_number_query(#[case] data_format: DataFormat) {
        let (db, _td) = create_db(data_format);
        db.set(
            "a",
            ComplexDbStruct::new(String::from("value"), 22, 4.20),
            OperationTarget::Main,
        )
        .unwrap();
        let query_result = QueryBuilder::query(q("float_val", Less, 22.1))
            .execute(&db)
            .unwrap();
        assert_eq!(query_result.count, 1);
    }

    #[rstest]
    #[case(DataFormat::Json)]
    #[case(DataFormat::Yaml)]
    #[case(DataFormat::Pot)]
    fn test_resolution_strategy_and_index(#[case] data_format: DataFormat) {
        let (db, _td) = create_db(data_format);
        db.add_index("usize_val", IndexType::Numeric);
        let result = QueryBuilder::query(q("usize_val", Equal, 22) & q("str_val", Equal, "qwerty"))
            .execute(&db)
            .unwrap();
        assert_eq!(
            result.resolution_strategy,
            ResolutionStrategy::UseIndexes(vec![Index::new(
                "usize_val#numeric.index",
                "usize_val",
                IndexType::Numeric
            )])
        )
    }

    #[rstest]
    #[case(DataFormat::Json)]
    #[case(DataFormat::Yaml)]
    #[case(DataFormat::Pot)]
    fn test_resolution_strategy_or_no_index(#[case] data_format: DataFormat) {
        let (db, _td) = create_db(data_format);
        db.add_index("usize_val", IndexType::Numeric);
        let result = QueryBuilder::query(q("usize_val", Equal, 22) | q("str_val", Equal, "qwerty"))
            .execute(&db)
            .unwrap();
        assert_eq!(result.resolution_strategy, ResolutionStrategy::Scan)
    }

    #[rstest]
    #[case(DataFormat::Json)]
    #[case(DataFormat::Yaml)]
    #[case(DataFormat::Pot)]
    fn test_query_results_with_index(#[case] data_format: DataFormat) {
        let (db, _td) = create_db(data_format);
        db.add_index("usize_val", IndexType::Numeric);
        let result = QueryBuilder::query(q("usize_val", Greater, 22))
            .execute(&db)
            .unwrap();
        db.set(
            "a",
            ComplexDbStruct::new(String::from("value"), 200, 4.20),
            OperationTarget::Main,
        )
        .unwrap();
        db.set(
            "b",
            ComplexDbStruct::new(String::from("value"), 22, 4.20),
            OperationTarget::Main,
        )
        .unwrap();
        db.set(
            "c",
            ComplexDbStruct::new(String::from("value"), 0, 4.20),
            OperationTarget::Main,
        )
        .unwrap();
        assert_eq!(
            result.resolution_strategy,
            ResolutionStrategy::UseIndexes(vec![Index::new(
                "usize_val#numeric.index",
                "usize_val",
                IndexType::Numeric
            )])
        )
    }

    #[rstest]
    #[case(DataFormat::Json)]
    #[case(DataFormat::Yaml)]
    #[case(DataFormat::Pot)]
    fn test_query_results_every_ordering(#[case] data_format: DataFormat) {
        let (db, _td) = create_db(data_format);
        db.add_index("usize_val", IndexType::Numeric);
        const INIT_DB_SIZE: usize = 1_000;
        let hm: [usize; INIT_DB_SIZE] = core::array::from_fn(|i| i + 1);
        let hm2 = hm.iter().map(|x| {
            (
                format!("key-{}", x),
                ComplexDbStruct::new(String::from("test value"), *x, *x as f64),
            )
        });
        db.set_batch(hm2, OperationTarget::Main).unwrap();
        let query_result = QueryBuilder::query(
            q("usize_val", Less, 100) | q("usize_val", Equal, 500) | q("usize_val", Greater, 900),
        )
        .execute(&db)
        .unwrap();
        assert_eq!(query_result.count, 200);
        let index = Index::new("usize_val#numeric.index", "usize_val", IndexType::Numeric);
        assert_eq!(
            query_result.resolution_strategy,
            ResolutionStrategy::UseIndexes(vec![index.clone(), index.clone(), index])
        )
    }

    #[rstest]
    #[case(DataFormat::Json)]
    #[case(DataFormat::Yaml)]
    #[case(DataFormat::Pot)]
    fn test_query_with_limit(#[case] data_format: DataFormat) {
        let (db, _td) = create_db(data_format);
        db.set(
            "a",
            ComplexDbStruct::new(String::from("value"), 22, 1.0),
            OperationTarget::Main,
        )
        .unwrap();
        db.set(
            "b",
            ComplexDbStruct::new(String::from("value"), 4, 1.0),
            OperationTarget::Main,
        )
        .unwrap();
        db.set(
            "c",
            ComplexDbStruct::new(String::from("different"), 22, 1.0),
            OperationTarget::Main,
        )
        .unwrap();
        let query_result = QueryBuilder::all().maybe_limit(2).execute(&db).unwrap();
        assert_eq!(query_result.count, 2);
    }
}
