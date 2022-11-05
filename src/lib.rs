use std::str;
use std::sync::{Arc, Mutex, MutexGuard};
use std::{collections::HashMap, path::Path};

use blake3;
use git2::{BranchType, Commit, Oid, Repository, Signature, Time, Tree, TreeBuilder};
use tokio::runtime::{Handle, Runtime};

pub mod error;
pub mod replica;

pub struct Collection {
    repository: Arc<Mutex<Repository>>,
    replicas: Vec<replica::Replica>,
    handle: Handle,
}

impl Collection {
    pub fn load(path: &Path) -> Self {
        Self {
            repository: Arc::new(Mutex::new(Repository::open(path).unwrap())),
            replicas: Vec::new(),
            handle: Collection::get_runtime_handle().0,
        }
    }

    pub fn create(path: &Path) -> Self {
        let repo = Repository::init_bare(path).unwrap();
        {
            let index = &mut repo.index().unwrap();
            let id = index.write_tree().unwrap();
            let tree = repo.find_tree(id).unwrap();
            let sig = repo.signature().unwrap();
            repo.commit(Some("HEAD"), &sig, &sig, "init", &tree, &[])
                .unwrap();
            let head = repo.head().unwrap().target().unwrap();
            let head_commit = repo.find_commit(head).unwrap();
            repo.branch("main", &head_commit, true).unwrap();
        }
        Self {
            repository: Arc::new(Mutex::new(repo)),
            replicas: Vec::new(),
            handle: Collection::get_runtime_handle().0,
        }
    }

    pub fn add_replica<S>(&mut self, name: S, url: S)
    where
        S: AsRef<str>,
    {
        if self
            .replicas
            .iter()
            .any(|x| x.remote.as_str() == name.as_ref())
        {
            return;
        }
        let repo = self.repository.lock().unwrap();
        let remote = repo
            .find_remote(name.as_ref())
            .unwrap_or_else(|_| repo.remote(name.as_ref(), url.as_ref()).unwrap());
        self.replicas.push(replica::Replica {
            remote: remote.name().unwrap().to_string(),
            replication_method: replica::ReplicationMethod::All,
        });
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let path = Self::construct_path_to_key(key);
        let repo = self.repository.lock().unwrap();
        let tree_entry = Collection::current_commit(&repo)
            .tree()
            .unwrap()
            .get_path(Path::new(&path))
            .ok()?
            .to_object(&repo)
            .unwrap();
        let blob = tree_entry.as_blob().unwrap();
        let blob_content = blob.content();
        Some(blob_content.to_vec())
    }

    pub fn set_batch<'a, I, T>(
        &self,
        items: I,
    ) -> HashMap<String, tokio::task::JoinHandle<Result<(), git2::Error>>>
    where
        I: IntoIterator<Item = (T, &'a [u8])>,
        T: AsRef<str>,
    {
        let repo = self.repository.lock().unwrap();
        let commit = Collection::current_commit(&repo);
        {
            let mut root_tree = commit.tree().unwrap();
            for (key, value) in items {
                let blob = repo.blob(value).unwrap();
                let hash = blake3::hash(key.as_ref().as_bytes());
                let trees =
                    Collection::make_tree(&repo, hash.as_bytes(), &root_tree, key.as_ref(), blob);
                root_tree = repo.find_tree(trees).unwrap();
            }
            let current_time = &Time::new(chrono::Utc::now().timestamp(), 0);
            let signature = Signature::new("test", "test", current_time).unwrap();
            let new_commit = repo
                .commit_create_buffer(&signature, &signature, "update db", &root_tree, &[&commit])
                .unwrap();
            let commit_obj = repo
                .commit_signed(str::from_utf8(&new_commit).unwrap(), "", None)
                .unwrap();
            repo.head()
                .unwrap()
                .set_target(commit_obj, "update db")
                .unwrap();
        }
        drop(commit);
        drop(repo);
        let mut remote_push_results = HashMap::new();
        for replica in &self.replicas {
            let data = Arc::clone(&self.repository);
            let replica_remote = replica.remote.clone();
            let task = self.handle.spawn(async move {
                let repo = data.lock().unwrap();
                let mut remote = repo.find_remote(&replica_remote).unwrap().clone();
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
    ) -> Oid {
        let mut trees: Vec<TreeBuilder> = vec![repo.treebuilder(Some(root_tree)).unwrap()];
        for part in 0..2 {
            let parent_tree = trees.pop().unwrap();
            let octal_part = oid[part];
            let mut tree_builder = parent_tree
                .get(format!("{octal_part:o}"))
                .unwrap()
                .map(|x| {
                    repo.treebuilder(Some(&x.to_object(&repo).unwrap().into_tree().unwrap()))
                        .unwrap()
                })
                .unwrap_or_else(|| repo.treebuilder(None).unwrap());
            if part == 1 {
                tree_builder.insert(key, blob, 0o100644).unwrap();
            }
            trees.push(parent_tree);
            trees.push(tree_builder);
        }
        let mut index: usize = 2;
        loop {
            if let Some(self_tree) = trees.pop() {
                if let Some(mut parent_tree) = trees.pop() {
                    let tree_id = self_tree.write().unwrap();
                    index -= 1;
                    let octal_part = oid[index];
                    parent_tree
                        .insert(format!("{octal_part:o}"), tree_id, 0o040000)
                        .unwrap();
                    trees.push(parent_tree);
                } else {
                    return self_tree.write().unwrap();
                }
            } else {
                panic!("This shouldn't have happened");
            }
        }
    }

    pub fn set(
        &self,
        key: &str,
        value: &[u8],
    ) -> HashMap<String, tokio::task::JoinHandle<Result<(), git2::Error>>> {
        self.set_batch([(key, value)])
    }

    pub fn revert_to_commit(&self, _commit: &str) {}

    pub fn revert_n_commits(&self, n: usize) -> Result<(), error::RevertError> {
        if n == 0 {
            return Ok(());
        }
        let repo = self.repository.lock().unwrap();
        let head = repo.head().unwrap().target().unwrap();
        let mut target_commit = repo.find_commit(head).unwrap();
        for _ in 0..n {
            if target_commit.parent_count() > 1 {
                return Err(error::RevertError::BranchingHistory { commit: head });
            }
            target_commit = target_commit.parent(0).unwrap();
        }
        repo.reset(target_commit.as_object(), git2::ResetType::Soft, None)
            .unwrap();
        Ok(())
    }

    fn current_commit<'a>(repo: &'a MutexGuard<Repository>) -> Commit<'a> {
        let reference = repo
            .find_branch("main", BranchType::Local)
            .unwrap()
            .into_reference();
        let commit = reference.peel_to_commit().unwrap();
        commit
    }

    fn construct_path_to_key(key: &str) -> String {
        let hash = blake3::hash(key.as_bytes());
        let hash_bytes = hash.as_bytes();
        let mut path = String::new();
        for x in 0..2 {
            let val = &hash_bytes[x];
            path.push_str(format!("{val:o}").as_ref());
            path.push('/');
        }
        path.push_str(key);
        path
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
}

pub mod test;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::test::*;

    #[test]
    fn set_and_get() {
        let (db, _td) = create_db();
        db.set("key", "value".as_bytes());
        assert_eq!(db.get("key").unwrap(), "value".as_bytes());
    }

    #[test]
    fn batch_set_and_get() {
        let (db, _td) = create_db();
        let mut hm = HashMap::new();
        hm.insert("a", "initial a value".as_bytes());
        hm.insert("b", "initial b value".as_bytes());
        hm.insert("c", "initial c value".as_bytes());
        let mut hm2 = hm.clone();
        db.set_batch(hm);
        assert_eq!(db.get("a").unwrap(), "initial a value".as_bytes());
        assert_eq!(db.get("b").unwrap(), "initial b value".as_bytes());
        assert_eq!(db.get("c").unwrap(), "initial c value".as_bytes());
        hm2.insert("a", "changed a value".as_bytes());
        db.set_batch(hm2);
        assert_eq!(db.get("a").unwrap(), "changed a value".as_bytes());
    }

    #[test]
    fn get_non_existent_value() {
        let (db, _td) = create_db();
        assert_eq!(db.get("key"), None);
    }

    #[test]
    fn test_revert_n_commits() {
        let (db, _td) = create_db();
        db.set("a", b"initial a value");
        db.set("b", b"initial b value");
        db.set("b", b"changed b value");
        assert_eq!(db.get("b").unwrap(), b"changed b value");
        db.revert_n_commits(1).unwrap();
        assert_eq!(db.get("b").unwrap(), b"initial b value");
    }

    #[test]
    fn test_replica_same_name() {
        let (mut db, _td) = create_db();
        let (_, _td_backup) = create_db();
        db.add_replica("test", _td_backup.path().to_str().unwrap());
        db.add_replica("test", _td_backup.path().to_str().unwrap());
        assert_eq!(db.replicas.len(), 1);
    }

    #[test]
    fn test_replica_already_in_git() {
        let (mut db, _td) = create_db();
        let (_, _td_backup) = create_db();
        db.repository
            .lock()
            .unwrap()
            .remote("test", _td_backup.path().to_str().unwrap())
            .unwrap();
        db.add_replica("test", _td_backup.path().to_str().unwrap());
        assert_eq!(db.replicas.len(), 1);
    }

    #[tokio::test]
    async fn test_replica_sync() {
        let (mut db, _td) = create_db();
        let (db_backup, _td_backup) = create_db();
        db.add_replica("test", _td_backup.path().to_str().unwrap());
        assert_eq!(db.replicas.len(), 1);
        let result = db.set("a", b"a value");
        for (_, value) in result {
            value.await.unwrap().unwrap();
        }
        assert_eq!(db_backup.get("a").unwrap(), b"a value");
    }

    #[test]
    fn test_replica_non_existing_repo() {
        let (mut db, _td) = create_db();
        db.add_replica("test", "https://example.com/git.git");
        assert_eq!(db.replicas.len(), 1);
        db.set("a", b"a value");
    }
}
