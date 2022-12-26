use git2::build::CheckoutBuilder;
use git2::{
    BranchType, Commit, ErrorCode, FileFavor, Index, MergeOptions, ObjectType, Oid, PushOptions,
    RebaseOptions, Repository, Signature, Time, Tree, TreeBuilder, TreeWalkResult,
};
use parking_lot::{Mutex, MutexGuard};
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::str;
use std::sync::Arc;
use std::{collections::HashMap, path::Path};
use tokio::runtime::{Handle, Runtime};

pub mod error;
pub mod index;
pub mod query;
pub mod replica;
pub mod serialization;

pub enum OperationTarget<'a> {
    Main,
    Transaction(&'a str),
}

pub enum ConflictResolution {
    Overwrite,
    DiscardChanges,
    Abort,
}

pub struct Collection<'c> {
    repository: Arc<Mutex<Repository>>,
    replicas: Vec<replica::Replica<'c>>,
    handle: Handle,
    data_format: serialization::DataFormat,
}

impl<'c> Collection<'c> {
    pub fn load(
        path: &Path,
        data_format: serialization::DataFormat,
    ) -> Result<Self, error::CollectionInitError> {
        Ok(Self {
            repository: Arc::new(Mutex::new(Repository::open(path)?)),
            replicas: Vec::new(),
            handle: Collection::get_runtime_handle().0,
            data_format,
        })
    }

    pub fn create(
        path: &Path,
        data_format: serialization::DataFormat,
    ) -> Result<Self, error::CollectionInitError> {
        let repo = Repository::init_bare(path).unwrap();
        {
            let index = &mut repo.index()?;
            let id = index.write_tree()?;
            let tree = repo.find_tree(id)?;
            let sig = repo.signature()?;
            repo.commit(Some("HEAD"), &sig, &sig, "init", &tree, &[])?;
            let head = repo.head()?.target().unwrap();
            let head_commit = repo.find_commit(head)?;
            repo.branch("main", &head_commit, true)?;
        }
        Ok(Self {
            repository: Arc::new(Mutex::new(repo)),
            replicas: Vec::new(),
            handle: Collection::get_runtime_handle().0,
            data_format,
        })
    }

    pub fn repository(&self) -> MutexGuard<Repository> {
        self.repository.lock()
    }

    pub fn add_replica(
        &mut self,
        name: &str,
        url: &str,
        replication_method: replica::ReplicationMethod,
        push_options: Option<PushOptions<'c>>,
    ) {
        if self.replicas.iter().any(|x| x.remote.as_str() == name) {
            return;
        }
        let repo = self.repository.lock();
        let remote = repo
            .find_remote(name.as_ref())
            .unwrap_or_else(|_| repo.remote(name, url).unwrap());
        self.replicas.push(replica::Replica {
            remote: remote.name().unwrap().to_string(),
            replication_method,
            push_options,
        });
    }

    pub fn get<D>(
        &self,
        key: &str,
        target: OperationTarget,
    ) -> Result<Option<D>, error::GetObjectError>
    where
        D: DeserializeOwned,
    {
        let path = Self::construct_path_to_key(key);
        let branch = match target {
            OperationTarget::Main => "main",
            OperationTarget::Transaction(t) => t,
        };
        let repo = self.repository.lock();
        let tree_path = Collection::current_commit(&repo, branch)
            .map_err(|e| match e.code() {
                ErrorCode::NotFound => error::GetObjectError::InvalidOperationTarget,
                _ => e.into(),
            })?
            .tree()?
            .get_path(Path::new(&path))
            .ok();
        if let Some(tree_entry) = tree_path {
            let obj = tree_entry.to_object(&repo)?;
            let blob = obj
                .as_blob()
                .ok_or_else(|| error::GetObjectError::CorruptedObject)?;
            let blob_content = blob.content().to_owned();
            return Ok(Some(
                self.data_format
                    .deserialize(str::from_utf8(&blob_content).unwrap()),
            ));
        };
        Ok(None)
    }

    pub fn set_batch<'a, S, I, T>(
        &self,
        items: I,
        target: OperationTarget,
    ) -> HashMap<String, tokio::task::JoinHandle<Result<(), git2::Error>>>
    where
        S: Serialize,
        I: IntoIterator<Item = (T, S)>,
        T: AsRef<str>,
    {
        let indexes = self.list_indexes();
        let repo = self.repository.lock();
        let branch = match target {
            OperationTarget::Main => "main",
            OperationTarget::Transaction(t) => t,
        };
        let commit = Collection::current_commit(&repo, branch).unwrap();
        {
            let mut root_tree = commit.tree().unwrap();
            for (key, value) in items {
                let mut index_values = HashMap::new();
                for index in indexes.iter() {
                    index_values.insert(index, None);
                }
                let blob = repo
                    .blob(
                        self.data_format
                            .serialize_with_indexes(value, &mut index_values)
                            .as_bytes(),
                    )
                    .unwrap();
                let hash = Oid::hash_object(ObjectType::Blob, key.as_ref().as_bytes()).unwrap();
                let trees =
                    Collection::make_tree(&repo, hash.as_bytes(), &root_tree, key.as_ref(), blob)
                        .unwrap();
                root_tree = repo.find_tree(trees).unwrap();
                for (index, value) in index_values {
                    if let Some(val) = value {
                        index.create_entry(&repo, hash, &val);
                    }
                }
            }
            let signature = Self::signature();
            let new_commit = repo
                .commit_create_buffer(&signature, &signature, "update db", &root_tree, &[&commit])
                .unwrap();
            let commit_obj = repo
                .commit_signed(str::from_utf8(&new_commit).unwrap(), "", None)
                .unwrap();
            let mut branch_ref = repo.find_branch(branch, BranchType::Local).unwrap();
            branch_ref
                .get_mut()
                .set_target(commit_obj, "update db")
                .unwrap();
        }
        drop(commit);
        drop(repo);
        self.replicate()
    }

    pub fn set<S>(
        &self,
        key: &str,
        value: S,
        target: OperationTarget,
    ) -> HashMap<String, tokio::task::JoinHandle<Result<(), git2::Error>>>
    where
        S: Serialize,
    {
        self.set_batch([(key, value)], target)
    }

    pub fn new_transaction(&self, name: Option<&str>) -> Result<String, git2::Error> {
        let repo = self.repository.lock();
        let head = repo.head()?.target().unwrap();
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
        let repo = self.repository.lock();
        let main_branch = repo
            .find_annotated_commit(Collection::current_commit(&repo, "main")?.id())
            .unwrap();
        let transaction =
            Collection::current_commit(&repo, name).map_err(|err| match err.code() {
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
                        .set_target(commit, "update db")
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

    pub fn add_index(
        &self,
        field: &str,
        kind: index::IndexType,
        target: OperationTarget,
    ) -> (
        index::Index,
        HashMap<String, tokio::task::JoinHandle<Result<(), git2::Error>>>,
    ) {
        let branch = match target {
            OperationTarget::Main => "main",
            OperationTarget::Transaction(t) => t,
        };
        let repo = self.repository.lock();
        let commit = Collection::current_commit(&repo, branch).unwrap();
        let index_tree = commit.tree().unwrap();
        let index_name = format!("{}#{}.index", &field, kind);
        let existing_index = index_tree.get_name(&index_name);
        let index_obj = index::Index::from_name(&index_name).unwrap();
        if let None = existing_index {
            {
                let mut tb = repo.treebuilder(Some(&index_tree)).unwrap();
                Self::ensure_index_dir_exists(&repo);
                let mut index =
                    Index::open(Path::new(&repo.path().join(".index").join(&index_name)).into())
                        .unwrap();
                let obj = index.write_tree_to(&repo).unwrap();
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
        self.populate_index(&repo, &index_obj);
        (index_obj, self.replicate())
    }

    fn populate_index<'a>(&self, repo: &'a MutexGuard<Repository>, index: &index::Index) {
        let mut index_values: HashMap<&index::Index, Option<String>> = HashMap::new();
        let current_commit = Collection::current_commit(repo, "main").unwrap();
        current_commit
            .tree()
            .unwrap()
            .walk(git2::TreeWalkMode::PreOrder, |_, entry| {
                if entry.kind() != Some(ObjectType::Blob) {
                    return TreeWalkResult::Skip;
                }
                index_values.insert(&index, None);
                let oid = entry.id();
                let tree_path = Collection::current_commit(&repo, "main")
                    .map_err(|e| match e.code() {
                        ErrorCode::NotFound => error::GetObjectError::InvalidOperationTarget,
                        _ => e.into(),
                    })
                    .unwrap()
                    .tree()
                    .unwrap();
                let obj = tree_path.get_id(oid).unwrap();
                let blob = obj.to_object(&repo).unwrap();
                let blob_content = blob.as_blob().unwrap().content();
                let index_val = self
                    .data_format
                    .serialize_with_indexes(blob_content, &mut index_values);
                index.create_entry(&repo, oid, &index_val);
                TreeWalkResult::Ok
            })
            .unwrap();
    }

    fn list_indexes(&self) -> Vec<index::Index> {
        let repo = self.repository.lock();
        let index_tree = Self::current_commit(&repo, "main").unwrap().tree().unwrap();
        let mut indexes = Vec::new();
        for index in index_tree.iter() {
            if index.name().unwrap().ends_with(".index") {
                indexes.push(index::Index::from_name(index.name().unwrap()).unwrap());
            }
        }
        indexes
    }

    fn ensure_index_dir_exists<'a>(repo: &'a MutexGuard<Repository>) {
        std::fs::create_dir_all(repo.path().join(".index")).unwrap();
    }

    fn replicate(&self) -> HashMap<String, tokio::task::JoinHandle<Result<(), git2::Error>>> {
        let mut remote_push_results = HashMap::new();
        let rand_res: f64 = rand::thread_rng().gen();
        for replica in &self.replicas {
            let replicate = match replica.replication_method {
                replica::ReplicationMethod::All => true,
                replica::ReplicationMethod::Random(chance) => rand_res > chance,
                _ => true,
            };
            if !replicate {
                continue;
            }
            let data = Arc::clone(&self.repository);
            let replica_remote = replica.remote.clone();
            let task = self.handle.spawn(async move {
                let repo = data.lock();
                let mut remote = repo.find_remote(&replica_remote)?;
                remote.push(&["refs/heads/main"], None)
            });
            remote_push_results.insert(replica.remote.clone(), task);
        }
        remote_push_results
    }

    fn make_tree<'a>(
        repo: &'a MutexGuard<Repository>,
        oid: &[u8],
        root_tree: &'a Tree,
        key: &str,
        blob: Oid,
    ) -> Result<Oid, git2::Error> {
        let mut trees: Vec<TreeBuilder> = vec![repo.treebuilder(Some(root_tree))?];
        for part in 0..2 {
            let parent_tree = trees.pop().unwrap();
            let hex_part = oid[part];
            let mut tree_builder = parent_tree
                .get(format!("{hex_part:x}"))
                .unwrap()
                .map(|x| repo.treebuilder(Some(&x.to_object(&repo).unwrap().into_tree().unwrap())))
                .unwrap_or_else(|| repo.treebuilder(None))?;
            if part == 1 {
                tree_builder.insert(key, blob, 0o100644)?;
            }
            trees.push(parent_tree);
            trees.push(tree_builder);
        }
        let mut index: usize = 2;
        loop {
            let self_tree = trees.pop().unwrap();
            if let Some(mut parent_tree) = trees.pop() {
                let tree_id = self_tree.write()?;
                index -= 1;
                let hex_part = oid[index];
                parent_tree.insert(format!("{hex_part:x}"), tree_id, 0o040000)?;
                trees.push(parent_tree);
            } else {
                return Ok(self_tree.write()?);
            }
        }
    }

    pub fn revert_to_commit(&self, commit: Oid) -> Result<(), error::RevertError> {
        let repo = self.repository.lock();
        let target_commit = repo
            .find_commit(commit)
            .map_err(|_| error::RevertError::TargetCommitNotFound(commit))?;
        repo.reset(target_commit.as_object(), git2::ResetType::Soft, None)?;
        Ok(())
    }

    pub fn revert_n_commits(&self, n: usize) -> Result<(), error::RevertError> {
        if n == 0 {
            return Ok(());
        }
        let repo = self.repository.lock();
        let head = repo.head()?.target().unwrap();
        let mut target_commit = repo
            .find_commit(head)
            .map_err(|_| error::RevertError::TargetCommitNotFound(head))?;
        for _ in 0..n {
            if target_commit.parent_count() > 1 {
                return Err(error::RevertError::BranchingHistory(head));
            } else if target_commit.parent_count() == 0 {
                break;
            }
            target_commit = target_commit.parent(0)?;
        }
        repo.reset(target_commit.as_object(), git2::ResetType::Soft, None)?;
        Ok(())
    }

    fn current_commit<'a>(
        repo: &'a MutexGuard<Repository>,
        branch: &str,
    ) -> Result<Commit<'a>, git2::Error> {
        let reference = repo
            .find_branch(branch.as_ref(), BranchType::Local)?
            .into_reference();
        let commit = reference.peel_to_commit()?;
        Ok(commit)
    }

    fn construct_path_to_key(key: &str) -> String {
        let hash = Oid::hash_object(ObjectType::Blob, key.as_bytes()).unwrap();
        let hash_bytes = hash.as_bytes();
        let mut path = String::new();
        for x in 0..2 {
            let val = &hash_bytes[x];
            path.push_str(format!("{val:x}").as_ref());
            path.push('/');
        }
        path.push_str(key);
        path
    }

    fn construct_oid_from_path(path: &str) -> Oid {
        Oid::from_str(&path[path.len() - 22..].replace("/", "")).unwrap()
    }

    fn get_runtime_handle() -> (Handle, Option<Runtime>) {
        match Handle::try_current() {
            Ok(h) => (h, None),
            Err(_) => {
                let rt = Runtime::new().unwrap();
                (rt.handle().clone(), Some(rt))
            }
        }
    }

    fn signature<'a>() -> Signature<'a> {
        let current_time = &Time::new(chrono::Utc::now().timestamp(), 0);
        Signature::new("yamabiko", "yamabiko", current_time).unwrap()
    }
}

pub mod test;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use git2::{BranchType, Repository};

    use crate::{
        error,
        index::{Index, IndexType},
        replica::ReplicationMethod,
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
        );
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
            "a",
            SampleDbStruct {
                str_val: String::from("initial a value"),
            },
        );
        hm.insert(
            "b",
            SampleDbStruct {
                str_val: String::from("initial b value"),
            },
        );
        hm.insert(
            "c",
            SampleDbStruct {
                str_val: String::from("initial c value"),
            },
        );
        let mut hm2 = hm.clone();
        db.set_batch(hm, OperationTarget::Main);
        assert_eq!(
            db.get::<SampleDbStruct>("a", OperationTarget::Main)
                .unwrap()
                .unwrap(),
            SampleDbStruct::new(String::from("initial a value"))
        );
        assert_eq!(
            db.get::<SampleDbStruct>("b", OperationTarget::Main)
                .unwrap()
                .unwrap(),
            SampleDbStruct::new(String::from("initial b value"))
        );
        assert_eq!(
            db.get::<SampleDbStruct>("c", OperationTarget::Main)
                .unwrap()
                .unwrap(),
            SampleDbStruct::new(String::from("initial c value"))
        );
        hm2.insert("a", SampleDbStruct::new(String::from("changed a value")));
        db.set_batch(hm2, OperationTarget::Main);
        assert_eq!(
            db.get::<SampleDbStruct>("a", OperationTarget::Main)
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
        );
        db.set(
            "b",
            SampleDbStruct::new(String::from("initial b value")),
            OperationTarget::Main,
        );
        db.set(
            "b",
            SampleDbStruct::new(String::from("changed b value")),
            OperationTarget::Main,
        );
        assert_eq!(
            db.get::<SampleDbStruct>("b", OperationTarget::Main)
                .unwrap()
                .unwrap(),
            SampleDbStruct {
                str_val: String::from("changed b value")
            }
        );
        db.revert_n_commits(1).unwrap();
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
        );
        db.set(
            "a",
            SampleDbStruct::new(String::from("change #1")),
            OperationTarget::Main,
        );
        db.set(
            "a",
            SampleDbStruct::new(String::from("change #2")),
            OperationTarget::Main,
        );
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
        db.revert_to_commit(first_commit.id()).unwrap();
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
    fn test_replica_same_name() {
        let (mut db, _td) = create_db();
        let (_, _td_backup) = create_db();
        db.add_replica(
            "test",
            _td_backup.path().to_str().unwrap(),
            ReplicationMethod::All,
            None,
        );
        db.add_replica(
            "test",
            _td_backup.path().to_str().unwrap(),
            ReplicationMethod::All,
            None,
        );
        assert_eq!(db.replicas.len(), 1);
    }

    #[test]
    fn test_replica_already_in_git() {
        let (mut db, _td) = create_db();
        let (_, _td_backup) = create_db();
        db.repository
            .lock()
            .remote("test", _td_backup.path().to_str().unwrap())
            .unwrap();
        db.add_replica(
            "test",
            _td_backup.path().to_str().unwrap(),
            ReplicationMethod::All,
            None,
        );
        assert_eq!(db.replicas.len(), 1);
    }

    #[tokio::test]
    async fn test_replica_sync() {
        let (mut db, _td) = create_db();
        let (db_backup, _td_backup) = create_db();
        db.add_replica(
            "test",
            _td_backup.path().to_str().unwrap(),
            ReplicationMethod::All,
            None,
        );
        assert_eq!(db.replicas.len(), 1);
        let result = db.set(
            "a",
            SampleDbStruct::new(String::from("a value")),
            OperationTarget::Main,
        );
        for (_, value) in result {
            value.await.unwrap().unwrap();
        }
        assert_eq!(
            db_backup
                .get::<SampleDbStruct>("a", OperationTarget::Main)
                .unwrap()
                .unwrap(),
            SampleDbStruct {
                str_val: String::from("a value")
            }
        );
    }

    #[tokio::test]
    async fn test_replica_non_existing_repo() {
        let (mut db, _td) = create_db();
        db.add_replica(
            "test",
            "https://800.800.800.800/git.git",
            ReplicationMethod::All,
            None,
        );
        assert_eq!(db.replicas.len(), 1);
        let result = db.set(
            "a",
            SampleDbStruct::new(String::from("a value")),
            OperationTarget::Main,
        );
        for (_, value) in result {
            assert!(value.await.unwrap().is_err());
        }
    }

    #[test]
    fn test_simple_transaction() {
        let (db, _td) = create_db();
        db.set(
            "a",
            SampleDbStruct::new(String::from("a val")),
            OperationTarget::Main,
        );
        let t = db.new_transaction(None).unwrap();
        db.set(
            "b",
            SampleDbStruct::new(String::from("b val")),
            OperationTarget::Transaction(&t),
        );
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
        );
        let t = db.new_transaction(None).unwrap();
        db.set(
            "a",
            SampleDbStruct::new(String::from("TRAN\nline2")),
            OperationTarget::Transaction(&t),
        );
        db.set(
            "a",
            SampleDbStruct::new(String::from("MAIN\nline2")),
            OperationTarget::Main,
        );
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
        );
        let t = db.new_transaction(None).unwrap();
        db.set(
            "a",
            SampleDbStruct::new(String::from("TRAN\nline2")),
            OperationTarget::Transaction(&t),
        );
        db.set(
            "a",
            SampleDbStruct::new(String::from("MAIN\nline2")),
            OperationTarget::Main,
        );
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
        );
        let t = db.new_transaction(None).unwrap();
        db.set(
            "a",
            SampleDbStruct::new(String::from("TRAN\nline2")),
            OperationTarget::Transaction(&t),
        );
        db.set(
            "a",
            SampleDbStruct::new(String::from("MAIN\nline2")),
            OperationTarget::Main,
        );
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
        db.add_index("str_val", IndexType::Single, OperationTarget::Main);
        db.add_index("str_val", IndexType::Single, OperationTarget::Main);
        db.set(
            "a",
            SampleDbStruct::new(String::from("test value")),
            OperationTarget::Main,
        );
        let index_list = db.list_indexes();
        assert_eq!(index_list.len(), 1);
        assert_eq!(
            index_list[0],
            Index::new("str_val#single.index", "str_val", IndexType::Single)
        );
    }

    #[test]
    fn test_index_content() {
        let (db, _td) = create_db();
        db.add_index("str_val", IndexType::Single, OperationTarget::Main);
        db.set(
            "a",
            SampleDbStruct::new(String::from("1val")),
            OperationTarget::Main,
        );
        db.set(
            "b",
            SampleDbStruct::new(String::from("1val")),
            OperationTarget::Main,
        );
        db.set(
            "c",
            SampleDbStruct::new(String::from("2val")),
            OperationTarget::Main,
        );

        let index_values: Vec<git2::IndexEntry> = db.list_indexes()[0]
            .git_index(&db.repository.lock())
            .iter()
            .collect();
        assert_eq!(index_values.len(), 3);
        assert_eq!(index_values[0].path, "1val/fffffffffffffffe".as_bytes());
        assert_eq!(index_values[1].path, "1val/ffffffffffffffff".as_bytes());
        assert_eq!(index_values[2].path, "2val/ffffffffffffffff".as_bytes());
    }
}
