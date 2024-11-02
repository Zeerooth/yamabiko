use chrono::Utc;
use core::str;
use git2::build::CheckoutBuilder;
use git2::{
    BranchType, Commit, ErrorCode, FileFavor, Index, MergeOptions, ObjectType, Oid, RebaseOptions,
    Repository, RepositoryInitOptions, Signature, Time, Tree, TreeBuilder, TreeWalkResult,
};
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::{collections::HashMap, path::Path};

use crate::field::Field;

pub mod error;
pub mod field;
pub mod index;
pub mod logging;
pub mod query;
pub mod replica;
pub mod serialization;

pub enum OperationTarget<'a> {
    Main,
    Transaction(&'a str),
}

impl<'a> OperationTarget<'a> {
    pub fn to_git_branch(&self) -> &str {
        match self {
            OperationTarget::Main => "main",
            OperationTarget::Transaction(t) => t,
        }
    }
}

pub enum ConflictResolution {
    Overwrite,
    DiscardChanges,
    Abort,
}

trait RepositoryAbstraction {
    fn init_new_repo(path: &Path) -> Result<Repository, git2::Error> {
        let repo = Repository::init_opts(
            path,
            RepositoryInitOptions::new().bare(true).initial_head("main"),
        )?;
        {
            let mut cfg = repo.config()?;
            cfg.set_str("user.name", "yamabiko")?;
            cfg.set_str("user.email", "yamabiko@localhost")?;
            let index = &mut repo.index()?;
            let id = index.write_tree()?;
            let tree = repo.find_tree(id)?;
            let sig = repo.signature()?;
            repo.commit(Some("HEAD"), &sig, &sig, "init", &tree, &[])?;
            // HEAD has to exist and point at something
            let head = repo.head().unwrap().target().unwrap();
            let head_commit = repo.find_commit(head)?;
            repo.branch("main", &head_commit, true)?;
        }
        Ok(repo)
    }

    fn load_existing_repo(path: &Path) -> Result<Repository, git2::Error> {
        Repository::open_bare(path)
    }

    fn load_or_create_repo(path: &Path) -> Result<Repository, git2::Error> {
        match Self::load_existing_repo(path) {
            Ok(repo) => Ok(repo),
            Err(error) => match error.code() {
                ErrorCode::NotFound => Self::init_new_repo(path),
                _ => Err(error),
            },
        }
    }

    fn current_commit<'a>(repo: &'a Repository, branch: &str) -> Result<Commit<'a>, git2::Error> {
        let reference = repo
            .find_branch(branch.as_ref(), BranchType::Local)?
            .into_reference();
        let commit = reference.peel_to_commit()?;
        Ok(commit)
    }

    fn signature<'a>() -> Signature<'a> {
        let current_time = &Time::new(chrono::Utc::now().timestamp(), 0);
        // unwrap: this signature has to be valid
        Signature::new("yamabiko", "yamabiko@localhost", current_time).unwrap()
    }
}

pub struct Collection {
    repository: Repository,
    data_format: serialization::DataFormat,
}

impl RepositoryAbstraction for Collection {}

impl Collection {
    pub fn initialize(
        path: &Path,
        data_format: serialization::DataFormat,
    ) -> Result<Self, error::InitializationError> {
        let repo = Self::load_or_create_repo(path)?;
        Ok(Self {
            repository: repo,
            data_format,
        })
    }

    pub fn repository(&self) -> &Repository {
        &self.repository
    }

    fn get_tree_key(
        &self,
        key: &str,
        target: OperationTarget,
    ) -> Result<Option<git2::TreeEntry<'_>>, error::GetObjectError> {
        let path = Self::construct_path_to_key(key)?;
        let branch = match target {
            OperationTarget::Main => "main",
            OperationTarget::Transaction(t) => t,
        };
        let repo = &self.repository;
        let tree_path = Collection::current_commit(repo, branch)
            .map_err(|e| match e.code() {
                ErrorCode::NotFound => error::GetObjectError::InvalidOperationTarget,
                _ => e.into(),
            })?
            .tree()?
            .get_path(Path::new(&path))
            .ok();
        Ok(tree_path)
    }

    pub fn get_raw(
        &self,
        key: &str,
        target: OperationTarget,
    ) -> Result<Option<String>, error::GetObjectError> {
        if let Some(tree_entry) = self.get_tree_key(key, target)? {
            let obj = tree_entry.to_object(&self.repository)?;
            let blob = obj
                .as_blob()
                .ok_or_else(|| error::GetObjectError::CorruptedObject)?;
            let blob_content = blob.content().to_owned();
            let parsed = String::from_utf8(blob_content)?;
            return Ok(Some(parsed));
        };
        Ok(None)
    }

    pub fn get<D>(
        &self,
        key: &str,
        target: OperationTarget,
    ) -> Result<Option<D>, error::GetObjectError>
    where
        D: DeserializeOwned,
    {
        if let Some(tree_entry) = self.get_tree_key(key, target)? {
            let obj = tree_entry.to_object(&self.repository)?;
            let blob = obj
                .as_blob()
                .ok_or_else(|| error::GetObjectError::CorruptedObject)?;
            let blob_content = blob.content().to_owned();
            return Ok(Some(
                self.data_format.deserialize(str::from_utf8(&blob_content)?),
            ));
        };
        Ok(None)
    }

    /// Beware that this method only works on the main branch
    /// Should be faster than the normal get by key if the blob is in cache
    pub fn get_by_oid<D>(&self, oid: Oid) -> Result<Option<D>, error::GetObjectError>
    where
        D: DeserializeOwned,
    {
        debug!("Looking up oid {}", oid);
        let repo = &self.repository;
        let blob = repo.find_blob(oid);
        if let Ok(blob) = blob {
            let blob_content = blob.content().to_owned();
            return Ok(Some(
                self.data_format.deserialize(str::from_utf8(&blob_content)?),
            ));
        };
        Ok(None)
    }

    pub fn set_batch<S, I, T>(
        &self,
        items: I,
        target: OperationTarget,
    ) -> Result<(), error::SetObjectError>
    where
        S: Serialize,
        I: IntoIterator<Item = (T, S)>,
        T: AsRef<str>,
    {
        let indexes = self.index_list();
        let repo = &self.repository;
        let branch = match target {
            OperationTarget::Main => "main",
            OperationTarget::Transaction(t) => t,
        };
        let commit = Collection::current_commit(repo, branch)?;
        {
            let mut root_tree = commit.tree()?;
            let mut counter = 0;
            for (key, value) in items {
                counter += 1;
                debug!("set #{} key '{}'", counter, key.as_ref());
                let mut index_values = HashMap::new();
                for index in indexes.iter() {
                    index_values.insert(index, None);
                }
                let blob = repo.blob(
                    self.data_format
                        .serialize_with_indexes(value, &mut index_values)
                        .as_bytes(),
                )?;
                let hash = Oid::hash_object(ObjectType::Blob, key.as_ref().as_bytes())?;
                let trees =
                    Collection::make_tree(repo, hash.as_bytes(), &root_tree, key.as_ref(), blob)?;
                root_tree = repo.find_tree(trees)?;
                for (index, value) in index_values {
                    if let Some(val) = value {
                        index.create_entry(repo, hash, &val);
                    } else {
                        index.delete_entry(repo, hash);
                    }
                }
            }
            let signature = Self::signature();
            let commit_msg = format!("set {} items on {}", counter, branch);
            let new_commit = repo.commit_create_buffer(
                &signature,
                &signature,
                &commit_msg,
                &root_tree,
                &[&commit],
            )?;
            // unwrap: commit_create_buffer should never create an invalid UTF-8
            let commit_obj = repo.commit_signed(str::from_utf8(&new_commit).unwrap(), "", None)?;
            let mut branch_ref = repo
                .find_branch(branch, BranchType::Local)
                .map_err(|_| error::SetObjectError::InvalidOperationTarget)?;
            branch_ref.get_mut().set_target(commit_obj, &commit_msg)?;
        }
        Ok(())
    }

    pub fn set<S>(
        &self,
        key: &str,
        value: S,
        target: OperationTarget,
    ) -> Result<(), error::SetObjectError>
    where
        S: Serialize,
    {
        self.set_batch([(key, value)], target)
    }

    pub fn new_transaction(&self, name: Option<&str>) -> Result<String, git2::Error> {
        let repo = &self.repository;
        // unwrap: HEAD has to exist and point at something
        let head = repo.head().unwrap().target().unwrap();
        let head_commit = repo.find_commit(head)?;
        let transaction_name = name.map(|n| n.to_string()).unwrap_or_else(|| {
            format!(
                "t-{}",
                rand::thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(8)
                    .map(char::from)
                    .collect::<String>()
            )
        });
        repo.branch(&transaction_name, &head_commit, false)?;
        Ok(transaction_name)
    }

    pub fn apply_transaction(
        &self,
        name: &str,
        conflict_resolution: ConflictResolution,
    ) -> Result<(), error::TransactionError> {
        let repo = &self.repository;
        let main_branch = repo
            .find_annotated_commit(Collection::current_commit(repo, "main")?.id())
            .unwrap();
        let transaction =
            Collection::current_commit(repo, name).map_err(|err| match err.code() {
                ErrorCode::NotFound => error::TransactionError::TransactionNotFound,
                _ => err.into(),
            })?;
        let target_branch = repo.find_annotated_commit(transaction.id())?;
        let mut checkout_options = CheckoutBuilder::new();
        checkout_options.force();
        checkout_options.allow_conflicts(true);
        let mut merge_options = MergeOptions::new();
        match conflict_resolution {
            ConflictResolution::DiscardChanges => {
                checkout_options.use_ours(true);
                merge_options.file_favor(FileFavor::Ours);
            }
            ConflictResolution::Overwrite => {
                checkout_options.use_theirs(true);
                merge_options.file_favor(FileFavor::Theirs);
            }
            ConflictResolution::Abort => {
                // merge_options.fail_on_conflict(true);
            }
        }
        let mut rebase_options = RebaseOptions::new();
        let mut rebase_opts = rebase_options
            .inmemory(true)
            .checkout_options(checkout_options)
            .merge_options(merge_options);
        let mut rebase = repo
            .rebase(
                Some(&target_branch),
                Some(&main_branch),
                None,
                Some(&mut rebase_opts),
            )
            .unwrap();
        let mut current_commit: Option<Oid> = None;
        loop {
            let change = rebase.next();
            if change.is_none() {
                rebase.finish(None).unwrap();
                if let Some(commit) = current_commit {
                    let mut branch_ref = repo.find_branch("main", BranchType::Local).unwrap();
                    branch_ref
                        .get_mut()
                        .set_target(commit, format!("apply transaction {}", name).as_str())
                        .unwrap();
                };
                break;
            }
            match rebase.commit(None, &Self::signature(), None) {
                Ok(com) => current_commit = Some(com),
                Err(err) => match err.code() {
                    ErrorCode::Applied => {}
                    ErrorCode::MergeConflict | ErrorCode::Unmerged => match conflict_resolution {
                        ConflictResolution::Abort => {
                            rebase.abort()?;
                            return Err(error::TransactionError::Aborted);
                        }
                        _ => return Err(err.into()),
                    },
                    _ => return Err(err.into()),
                },
            }
        }
        repo.find_branch(name, BranchType::Local)
            .unwrap()
            .delete()
            .unwrap();
        Ok(())
    }

    pub fn add_index(&self, field: &str, kind: index::IndexType) -> index::Index {
        let branch = "main";
        let repo = &self.repository;
        let commit = Collection::current_commit(repo, branch).unwrap();
        let index_tree = commit.tree().unwrap();
        let index_name = format!("{}#{}.index", &field, kind);
        let existing_index = index_tree.get_name(&index_name);
        let index_obj = index::Index::from_name(&index_name).unwrap();
        if existing_index.is_none() {
            {
                let mut tb = repo.treebuilder(Some(&index_tree)).unwrap();
                Self::ensure_index_dir_exists(repo);
                let mut index =
                    Index::open(Path::new(&repo.path().join(".index").join(&index_name))).unwrap();
                let obj = index.write_tree_to(repo).unwrap();
                tb.insert(&index_name, obj, 0o040000).unwrap();
                let new_root = tb.write().unwrap();
                let root_tree = repo.find_tree(new_root).unwrap();
                let signature = Self::signature();
                let new_commit = repo
                    .commit_create_buffer(
                        &signature,
                        &signature,
                        format!("add index: {}", index_name).as_str(),
                        &root_tree,
                        &[&commit],
                    )
                    .unwrap();
                let commit_obj = repo
                    .commit_signed(str::from_utf8(&new_commit).unwrap(), "", None)
                    .unwrap();
                let mut branch_ref = repo.find_branch(branch, BranchType::Local).unwrap();
                branch_ref
                    .get_mut()
                    .set_target(commit_obj, format!("add index: {}", index_name).as_str())
                    .unwrap();
            }
        }
        self.populate_index(repo, &index_obj);
        index_obj
    }

    fn populate_index(&self, repo: &Repository, index: &index::Index) {
        let current_commit = Collection::current_commit(repo, "main").unwrap();
        current_commit
            .tree()
            .unwrap()
            .walk(git2::TreeWalkMode::PostOrder, |_, entry| {
                if entry.kind() != Some(ObjectType::Blob)
                    || entry.name().unwrap().ends_with(".index")
                {
                    return TreeWalkResult::Skip;
                }
                let mut index_values: HashMap<&index::Index, Option<Field>> = HashMap::new();
                index_values.insert(index, None);
                let oid = entry.id();
                let blob = entry.to_object(repo).unwrap();
                let blob_content = blob.as_blob().unwrap().content();
                self.data_format
                    .serialize_with_indexes_raw(blob_content, &mut index_values);
                if let Some(v) = index_values.get(index).unwrap() {
                    index.create_entry(repo, oid, v);
                }
                TreeWalkResult::Ok
            })
            .unwrap();
    }

    pub fn index_list(&self) -> Vec<index::Index> {
        let repo = &self.repository;
        let index_tree = Self::current_commit(repo, "main").unwrap().tree().unwrap();
        let mut indexes = Vec::new();
        for index in index_tree.iter() {
            if index.name().unwrap().ends_with(".index") {
                indexes.push(index::Index::from_name(index.name().unwrap()).unwrap());
            }
        }
        indexes
    }

    fn index_field_map(repo: &Repository) -> HashMap<String, index::Index> {
        let index_tree = Self::current_commit(repo, "main").unwrap().tree().unwrap();
        let mut indexes = HashMap::new();
        for index in index_tree.iter() {
            if index.name().unwrap().ends_with(".index") {
                let ind = index::Index::from_name(index.name().unwrap()).unwrap();
                indexes.insert(ind.indexed_field().to_string(), ind);
            }
        }
        indexes
    }

    fn ensure_index_dir_exists(repo: &Repository) {
        std::fs::create_dir_all(repo.path().join(".index")).unwrap();
    }

    fn make_tree<'a>(
        repo: &'a Repository,
        oid: &[u8],
        root_tree: &'a Tree,
        key: &str,
        blob: Oid,
    ) -> Result<Oid, git2::Error> {
        let mut trees: Vec<(String, TreeBuilder)> =
            vec![("".to_string(), repo.treebuilder(Some(root_tree))?)];
        let natural_tree = key.contains("/");
        if natural_tree {
            let mut iterator = key.split("/").peekable();
            while let Some(part) = iterator.next() {
                let (parent_name, mut parent_tree) = trees.pop().unwrap();
                if iterator.peek().is_none() {
                    parent_tree.insert(part, blob, 0o100644)?;
                    trees.push((parent_name, parent_tree));
                } else {
                    let tree_builder = parent_tree
                        .get(part)
                        .unwrap()
                        .map(|x| {
                            repo.treebuilder(Some(&x.to_object(repo).unwrap().into_tree().unwrap()))
                        })
                        .unwrap_or_else(|| repo.treebuilder(None))?;
                    trees.push((parent_name, parent_tree));
                    trees.push((part.to_string(), tree_builder));
                }
            }
        } else {
            for part in 0..2 {
                let (parent_name, parent_tree) = trees.pop().unwrap();
                let hex_part = oid[part];
                let name = format!("{hex_part:x}");
                let mut tree_builder = parent_tree
                    .get(&name)
                    .unwrap()
                    .map(|x| {
                        repo.treebuilder(Some(&x.to_object(repo).unwrap().into_tree().unwrap()))
                    })
                    .unwrap_or_else(|| repo.treebuilder(None))?;
                if part == 1 {
                    tree_builder.insert(key, blob, 0o100644)?;
                }
                trees.push((parent_name, parent_tree));
                trees.push((name, tree_builder));
            }
        }

        loop {
            let (self_name, self_tree) = trees.pop().unwrap();
            if let Some((parent_name, mut parent_tree)) = trees.pop() {
                let tree_id = self_tree.write()?;
                parent_tree.insert(&self_name, tree_id, 0o040000)?;
                trees.push((parent_name, parent_tree));
            } else {
                return self_tree.write();
            }
        }
    }

    fn prepare_remote_push_tags(&self, head: Oid, target: Oid) -> Result<(), git2::Error> {
        let remotes = self.repository.remotes()?;
        let current_time = Utc::now();
        let tag_name = format!(
            "revert-{}-{}-{}",
            &head.to_string()[0..7],
            &target.to_string()[0..7],
            current_time.timestamp()
        );
        for remote in remotes.iter().flatten() {
            let ref_name = format!("refs/history_tags/{}/{}", remote, tag_name);
            self.repository.reference(&ref_name, head, true, "")?;
        }
        Ok(())
    }

    pub fn revert_main_to_commit(
        &self,
        commit: Oid,
        keep_history: bool,
    ) -> Result<(), error::RevertError> {
        let repo = &self.repository;
        let target_commit = repo
            .find_commit(commit)
            .map_err(|_| error::RevertError::TargetCommitNotFound(commit))?;
        if keep_history {
            let current_commit = Self::current_commit(repo, OperationTarget::Main.to_git_branch())
                .map_err(|e| match e.code() {
                    ErrorCode::NotFound => error::RevertError::InvalidOperationTarget,
                    _ => e.into(),
                })?;
            self.prepare_remote_push_tags(current_commit.id(), target_commit.id())?;
        }
        repo.reset(target_commit.as_object(), git2::ResetType::Soft, None)?;
        Ok(())
    }

    pub fn revert_n_commits(
        &self,
        n: usize,
        target: OperationTarget,
        keep_history: bool,
    ) -> Result<(), error::RevertError> {
        debug!("Reverting {} commits", n);
        if n == 0 {
            return Ok(());
        }
        let repo = &self.repository;
        let current_commit =
            Self::current_commit(repo, target.to_git_branch()).map_err(|e| match e.code() {
                ErrorCode::NotFound => error::RevertError::InvalidOperationTarget,
                _ => e.into(),
            })?;
        let mut target_commit = current_commit.clone();
        for _ in 0..n {
            let parent_count = target_commit.parent_count();
            if parent_count > 1 {
                return Err(error::RevertError::BranchingHistory(target_commit.id()));
            } else if parent_count == 0 {
                debug!("No more parents to check");
                break;
            }
            target_commit = target_commit.parent(0)?;
            debug!("Current commit to revert: {:?}", target_commit.as_object());
        }
        if keep_history {
            self.prepare_remote_push_tags(current_commit.id(), target_commit.id())?;
        }
        repo.reset(target_commit.as_object(), git2::ResetType::Soft, None)?;
        Ok(())
    }

    fn construct_path_to_key(key: &str) -> Result<String, error::KeyError> {
        if key.contains("/") {
            return Ok(key.to_string());
        }
        let hash = Oid::hash_object(ObjectType::Blob, key.as_bytes())
            .map_err(error::KeyError::NotHashable)?;
        let hash_bytes = hash.as_bytes();
        let mut path = String::new();
        (0..2).for_each(|x| {
            let val = &hash_bytes[x];
            path.push_str(format!("{val:x}").as_ref());
            path.push('/');
        });
        path.push_str(key);
        Ok(path)
    }

    pub fn prefix_from_oid(oid: &Oid) -> String {
        let hash_bytes = oid.as_bytes();
        let mut path = String::new();
        (0..2).for_each(|x| {
            let val = &hash_bytes[x];
            path.push_str(format!("{val:x}").as_ref());
            path.push('/');
        });
        debug!("Constructed prefix {}", path);
        path
    }

    pub fn construct_oid_from_path(path: &str) -> Oid {
        Oid::from_str(&path[path.len() - 22..].replace("/", "")).unwrap()
    }
}

pub mod test;

#[cfg(test)]
mod tests {
    use std::cmp::Ordering::*;
    use std::collections::HashMap;

    use git2::{BranchType, Repository};

    use crate::{
        error,
        index::{Index, IndexType},
        query::{q, QueryBuilder},
        OperationTarget,
    };

    use super::test::*;

    #[test]
    fn set_and_get() {
        let (db, _td) = create_db();
        db.set(
            "key",
            SampleDbStruct {
                str_val: String::from("value"),
            },
            OperationTarget::Main,
        )
        .unwrap();
        assert_eq!(
            db.get::<SampleDbStruct>("key", OperationTarget::Main)
                .unwrap()
                .unwrap(),
            SampleDbStruct {
                str_val: String::from("value")
            }
        );
    }

    #[test]
    fn batch_set_and_get() {
        let (db, _td) = create_db();
        let mut hm = HashMap::new();
        hm.insert(
            "pref/a",
            SampleDbStruct {
                str_val: String::from("initial a value"),
            },
        );
        hm.insert(
            "pref/b",
            SampleDbStruct {
                str_val: String::from("initial b value"),
            },
        );
        hm.insert(
            "pref/c",
            SampleDbStruct {
                str_val: String::from("initial c value"),
            },
        );
        let mut hm2 = hm.clone();
        db.set_batch(hm, OperationTarget::Main).unwrap();
        assert_eq!(
            db.get::<SampleDbStruct>("pref/a", OperationTarget::Main)
                .unwrap()
                .unwrap(),
            SampleDbStruct::new(String::from("initial a value"))
        );
        assert_eq!(
            db.get::<SampleDbStruct>("pref/b", OperationTarget::Main)
                .unwrap()
                .unwrap(),
            SampleDbStruct::new(String::from("initial b value"))
        );
        assert_eq!(
            db.get::<SampleDbStruct>("pref/c", OperationTarget::Main)
                .unwrap()
                .unwrap(),
            SampleDbStruct::new(String::from("initial c value"))
        );
        hm2.insert(
            "pref/a",
            SampleDbStruct::new(String::from("changed a value")),
        );
        db.set_batch(hm2, OperationTarget::Main).unwrap();
        assert_eq!(
            db.get::<SampleDbStruct>("pref/a", OperationTarget::Main)
                .unwrap()
                .unwrap(),
            SampleDbStruct::new(String::from("changed a value"))
        );
    }

    #[test]
    fn test_get_non_existent_value() {
        let (db, _td) = create_db();
        assert_eq!(
            db.get::<SampleDbStruct>("key", OperationTarget::Main)
                .unwrap(),
            None
        );
    }

    #[test]
    fn test_revert_n_commits() {
        let (db, _td) = create_db();
        db.set(
            "a",
            SampleDbStruct::new(String::from("initial a value")),
            OperationTarget::Main,
        )
        .unwrap();
        db.set(
            "b",
            SampleDbStruct::new(String::from("initial b value")),
            OperationTarget::Main,
        )
        .unwrap();
        db.set(
            "b",
            SampleDbStruct::new(String::from("changed b value")),
            OperationTarget::Main,
        )
        .unwrap();
        assert_eq!(
            db.get::<SampleDbStruct>("b", OperationTarget::Main)
                .unwrap()
                .unwrap(),
            SampleDbStruct {
                str_val: String::from("changed b value")
            }
        );
        db.revert_n_commits(1, OperationTarget::Main, false)
            .unwrap();
        assert_eq!(
            db.get::<SampleDbStruct>("b", OperationTarget::Main)
                .unwrap()
                .unwrap(),
            SampleDbStruct {
                str_val: String::from("initial b value")
            }
        );
    }

    #[test]
    fn test_revert_to_commit() {
        let (db, td) = create_db();
        db.set(
            "a",
            SampleDbStruct::new(String::from("initial a value")),
            OperationTarget::Main,
        )
        .unwrap();
        db.set(
            "a",
            SampleDbStruct::new(String::from("change #1")),
            OperationTarget::Main,
        )
        .unwrap();
        db.set(
            "a",
            SampleDbStruct::new(String::from("change #2")),
            OperationTarget::Main,
        )
        .unwrap();
        assert_eq!(
            db.get::<SampleDbStruct>("a", OperationTarget::Main)
                .unwrap()
                .unwrap(),
            SampleDbStruct {
                str_val: String::from("change #2")
            }
        );
        let repo = Repository::open(td.path()).unwrap();
        let reference = repo
            .find_branch("main", BranchType::Local)
            .unwrap()
            .into_reference();
        let head_commit = reference.peel_to_commit().unwrap();
        let first_commit = head_commit.parent(0).unwrap().parent(0).unwrap().clone();
        db.revert_main_to_commit(first_commit.id(), false).unwrap();
        assert_eq!(
            db.get::<SampleDbStruct>("a", OperationTarget::Main)
                .unwrap()
                .unwrap(),
            SampleDbStruct {
                str_val: String::from("initial a value")
            }
        );
    }

    #[test]
    fn test_simple_transaction() {
        let (db, _td) = create_db();
        db.set(
            "a",
            SampleDbStruct::new(String::from("a val")),
            OperationTarget::Main,
        )
        .unwrap();
        let t = db.new_transaction(None).unwrap();
        db.set(
            "b",
            SampleDbStruct::new(String::from("b val")),
            OperationTarget::Transaction(&t),
        )
        .unwrap();
        assert_eq!(
            db.get::<SampleDbStruct>("b", OperationTarget::Main)
                .unwrap(),
            None
        );
        assert_eq!(
            db.get::<SampleDbStruct>("b", OperationTarget::Transaction(&t))
                .unwrap()
                .unwrap(),
            SampleDbStruct {
                str_val: String::from("b val")
            }
        );
        db.apply_transaction(&t, crate::ConflictResolution::Overwrite)
            .unwrap();
        assert_eq!(
            db.get::<SampleDbStruct>("b", OperationTarget::Main)
                .unwrap()
                .unwrap(),
            SampleDbStruct {
                str_val: String::from("b val")
            }
        );
    }

    #[test]
    fn test_transaction_overwrite() {
        let (db, _td) = create_db();
        db.set(
            "a",
            SampleDbStruct::new(String::from("INIT\nline2")),
            OperationTarget::Main,
        )
        .unwrap();
        let t = db.new_transaction(None).unwrap();
        db.set(
            "a",
            SampleDbStruct::new(String::from("TRAN\nline2")),
            OperationTarget::Transaction(&t),
        )
        .unwrap();
        db.set(
            "a",
            SampleDbStruct::new(String::from("MAIN\nline2")),
            OperationTarget::Main,
        )
        .unwrap();
        assert_eq!(
            db.get::<SampleDbStruct>("a", OperationTarget::Main)
                .unwrap()
                .unwrap(),
            SampleDbStruct {
                str_val: String::from("MAIN\nline2")
            }
        );
        assert_eq!(
            db.get::<SampleDbStruct>("a", OperationTarget::Transaction(&t))
                .unwrap()
                .unwrap(),
            SampleDbStruct {
                str_val: String::from("TRAN\nline2")
            }
        );
        db.apply_transaction(&t, crate::ConflictResolution::Overwrite)
            .unwrap();
        assert_eq!(
            db.get::<SampleDbStruct>("a", OperationTarget::Main)
                .unwrap()
                .unwrap(),
            SampleDbStruct {
                str_val: String::from("TRAN\nline2")
            }
        );
    }

    #[test]
    fn test_transaction_discard() {
        let (db, _td) = create_db();
        db.set(
            "a",
            SampleDbStruct::new(String::from("INIT\nline2")),
            OperationTarget::Main,
        )
        .unwrap();
        let t = db.new_transaction(None).unwrap();
        db.set(
            "a",
            SampleDbStruct::new(String::from("TRAN\nline2")),
            OperationTarget::Transaction(&t),
        )
        .unwrap();
        db.set(
            "a",
            SampleDbStruct::new(String::from("MAIN\nline2")),
            OperationTarget::Main,
        )
        .unwrap();
        assert_eq!(
            db.get::<SampleDbStruct>("a", OperationTarget::Main)
                .unwrap()
                .unwrap(),
            SampleDbStruct {
                str_val: String::from("MAIN\nline2")
            }
        );
        assert_eq!(
            db.get::<SampleDbStruct>("a", OperationTarget::Transaction(&t))
                .unwrap()
                .unwrap(),
            SampleDbStruct {
                str_val: String::from("TRAN\nline2")
            }
        );
        db.apply_transaction(&t, crate::ConflictResolution::DiscardChanges)
            .unwrap();
        assert_eq!(
            db.get::<SampleDbStruct>("a", OperationTarget::Main)
                .unwrap()
                .unwrap(),
            SampleDbStruct {
                str_val: String::from("MAIN\nline2")
            }
        );
    }

    #[test]
    fn test_transaction_abort() {
        let (db, _td) = create_db();
        db.set(
            "a",
            SampleDbStruct::new(String::from("INIT\nline2")),
            OperationTarget::Main,
        )
        .unwrap();
        let t = db.new_transaction(None).unwrap();
        db.set(
            "a",
            SampleDbStruct::new(String::from("TRAN\nline2")),
            OperationTarget::Transaction(&t),
        )
        .unwrap();
        db.set(
            "a",
            SampleDbStruct::new(String::from("MAIN\nline2")),
            OperationTarget::Main,
        )
        .unwrap();
        assert_eq!(
            db.get::<SampleDbStruct>("a", OperationTarget::Main)
                .unwrap()
                .unwrap(),
            SampleDbStruct {
                str_val: String::from("MAIN\nline2")
            }
        );
        assert_eq!(
            db.get::<SampleDbStruct>("a", OperationTarget::Transaction(&t))
                .unwrap()
                .unwrap(),
            SampleDbStruct {
                str_val: String::from("TRAN\nline2")
            }
        );
        assert_eq!(
            db.apply_transaction(&t, crate::ConflictResolution::Abort)
                .unwrap_err(),
            error::TransactionError::Aborted
        );
        assert_eq!(
            db.get::<SampleDbStruct>("a", OperationTarget::Main)
                .unwrap()
                .unwrap(),
            SampleDbStruct {
                str_val: String::from("MAIN\nline2")
            }
        );
    }

    #[test]
    fn test_adding_index() {
        let (db, _td) = create_db();
        db.add_index("str_val", IndexType::Sequential);
        db.add_index("str_val", IndexType::Sequential);
        db.set(
            "a",
            SampleDbStruct::new(String::from("test value")),
            OperationTarget::Main,
        )
        .unwrap();
        let index_list = db.index_list();
        assert_eq!(index_list.len(), 1);
        assert_eq!(
            index_list[0],
            Index::new("str_val#sequential.index", "str_val", IndexType::Sequential)
        );
    }

    #[test]
    fn test_index_content() {
        let (db, _td) = create_db();
        db.add_index("str_val", IndexType::Sequential);
        db.set(
            "a",
            SampleDbStruct::new(String::from("1val")),
            OperationTarget::Main,
        )
        .unwrap();
        db.set(
            "b",
            SampleDbStruct::new(String::from("1val")),
            OperationTarget::Main,
        )
        .unwrap();
        db.set(
            "c",
            SampleDbStruct::new(String::from("2val")),
            OperationTarget::Main,
        )
        .unwrap();
        let index_values: Vec<git2::IndexEntry> = db.index_list()[0]
            .git_index(&db.repository)
            .iter()
            .collect();
        assert_eq!(index_values.len(), 3);
        assert_eq!(index_values[0].path, "1val/fffffffffffffffe".as_bytes());
        assert_eq!(index_values[1].path, "1val/ffffffffffffffff".as_bytes());
        assert_eq!(index_values[2].path, "2val/ffffffffffffffff".as_bytes());
    }

    #[test]
    fn test_index_content_numeric() {
        let (db, _td) = create_db();
        db.add_index("num_val", IndexType::Numeric);
        db.set(
            "b",
            InterigentDbStruct { num_val: 20 },
            OperationTarget::Main,
        )
        .unwrap();
        db.set(
            "c",
            InterigentDbStruct { num_val: 2 },
            OperationTarget::Main,
        )
        .unwrap();
        db.set(
            "d",
            InterigentDbStruct { num_val: 3 },
            OperationTarget::Main,
        )
        .unwrap();
        db.set(
            "e",
            FloatyDbStruct { num_val: -11.0 },
            OperationTarget::Main,
        )
        .unwrap();
        db.set("a", FloatyDbStruct { num_val: 2.11 }, OperationTarget::Main)
            .unwrap();
        let index_values: Vec<git2::IndexEntry> = db.index_list()[0]
            .git_index(&db.repository)
            .iter()
            .collect();
        assert_eq!(index_values.len(), 5);
        assert_eq!(
            String::from_utf8(index_values[0].path.clone()).unwrap(),
            format!("0/{:16x}/ffffffffffffffff", (-11 as f64).to_bits())
        );
        assert_eq!(
            String::from_utf8(index_values[1].path.clone()).unwrap(),
            format!("1/{:16x}/ffffffffffffffff", 2.0_f64.to_bits())
        );
        assert_eq!(
            String::from_utf8(index_values[2].path.clone()).unwrap(),
            format!("1/{:16x}/ffffffffffffffff", 2.11_f64.to_bits())
        );
        assert_eq!(
            String::from_utf8(index_values[3].path.clone()).unwrap(),
            format!("1/{:16x}/ffffffffffffffff", 3.0_f64.to_bits())
        );
        assert_eq!(
            String::from_utf8(index_values[4].path.clone()).unwrap(),
            format!("1/{:16x}/ffffffffffffffff", 20.0_f64.to_bits())
        );
    }

    #[test]
    fn test_writing_to_correct_index() {
        let (db, _td) = create_db();
        db.add_index("str_val", IndexType::Numeric);
        db.set(
            "a",
            SampleDbStruct::new(String::from("test")),
            OperationTarget::Main,
        )
        .unwrap();
        let index_values: Vec<git2::IndexEntry> = db.index_list()[0]
            .git_index(&db.repository)
            .iter()
            .collect();
        assert_eq!(index_values.len(), 0);
    }

    #[test]
    fn test_index_population() {
        let (db, _td) = create_db();
        db.set(
            "a",
            SampleDbStruct::new(String::from("test")),
            OperationTarget::Main,
        )
        .unwrap();
        db.add_index("str_val", IndexType::Sequential);
        let index_values: Vec<git2::IndexEntry> = db.index_list()[0]
            .git_index(&db.repository)
            .iter()
            .collect();
        assert_eq!(index_values.len(), 1);
    }

    #[test]
    fn test_index_removes_entries_on_update() {
        let (db, _td) = create_db();
        db.add_index("str_val", IndexType::Sequential);
        let query = QueryBuilder::new().query(q("str_val", Equal, "test"));
        db.set(
            "a",
            SampleDbStruct::new(String::from("test")),
            OperationTarget::Main,
        )
        .unwrap();
        assert_eq!(query.execute(&db).count, 1);
        db.set("a", FloatyDbStruct { num_val: 69.0 }, OperationTarget::Main)
            .unwrap();
        assert_eq!(query.execute(&db).count, 0);
    }

    #[test]
    fn test_index_entry_update() {
        let (db, _td) = create_db();
        db.add_index("str_val", IndexType::Sequential);
        let query = QueryBuilder::new().query(q("str_val", Equal, "test"));
        db.set(
            "a",
            SampleDbStruct::new(String::from("test")),
            OperationTarget::Main,
        )
        .unwrap();
        assert_eq!(query.execute(&db).count, 1);
        db.set(
            "a",
            SampleDbStruct::new(String::from("test2")),
            OperationTarget::Main,
        )
        .unwrap();
        assert_eq!(query.execute(&db).count, 1);
    }
}
