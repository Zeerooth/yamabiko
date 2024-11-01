use std::path::{Path, PathBuf};

use git2::{Cred, PushOptions, Remote, RemoteCallbacks, Repository};
use rand::Rng;

use crate::{error, RepositoryAbstraction};

#[derive(Clone)]
pub enum ReplicationMethod {
    All,
    Partial(usize),
    Random(f64),
}

pub struct Replicator {
    repository: Repository,
    remote_name: String,
    remote_url: String,
    replication_method: ReplicationMethod,
    credentials: Option<RemoteCredentials>,
}

impl RepositoryAbstraction for Replicator {}

impl Replicator {
    pub fn initialize(
        repo_path: &Path,
        remote_name: &str,
        remote_url: &str,
        replication_method: ReplicationMethod,
        credentials: Option<RemoteCredentials>,
    ) -> Result<Self, error::InitializationError> {
        let repo = Self::load_or_create_repo(repo_path)?;
        let remote_name_formatted = format!("_yamabiko_replica_{}", remote_name);
        Self::ensure_remote(&repo, remote_name, remote_url)?;
        Ok(Self {
            repository: repo,
            remote_name: remote_name_formatted,
            remote_url: remote_url.to_string(),
            replication_method,
            credentials,
        })
    }

    fn ensure_remote<'a>(
        repo: &'a Repository,
        remote_name: &str,
        remote_url: &str,
    ) -> Result<Remote<'a>, git2::Error> {
        let remote = repo.find_remote(remote_name);
        match remote {
            Err(_) => repo.remote(remote_name, remote_url),
            Ok(remote) => Ok(remote),
        }
    }

    pub fn replicate(&self) -> Result<bool, git2::Error> {
        let rand_res: f64 = rand::thread_rng().gen();
        let replicate = match self.replication_method {
            ReplicationMethod::All => true,
            ReplicationMethod::Random(chance) => rand_res < chance,
            _ => true,
        };
        if !replicate {
            return Ok(false);
        }
        let mut remote = Self::ensure_remote(
            &self.repository,
            self.remote_name.as_str(),
            self.remote_url.as_str(),
        )?;
        let mut callbacks = RemoteCallbacks::new();
        if let Some(ref cred) = self.credentials {
            callbacks.credentials(|_, username_from_url, _| {
                Cred::ssh_key(
                    cred.username
                        .as_deref()
                        .unwrap_or(username_from_url.unwrap_or("git")),
                    cred.publickey.as_deref(),
                    cred.privatekey.as_path(),
                    cred.passphrase.as_deref(),
                )
            });
        }
        let mut push_options = PushOptions::new();
        push_options.remote_callbacks(callbacks);
        remote.push(&["refs/heads/main"], Some(&mut push_options))?;
        Ok(true)
    }
}

#[derive(Clone)]
pub struct RemoteCredentials {
    pub username: Option<String>,
    pub publickey: Option<PathBuf>,
    pub privatekey: PathBuf,
    pub passphrase: Option<String>,
}

#[cfg(test)]
mod tests {
    use crate::{
        replica::{ReplicationMethod, Replicator},
        test::{create_db, SampleDbStruct},
        OperationTarget,
    };

    #[test]
    fn test_replica_same_name() {
        let (_, td) = create_db();
        Replicator::initialize(td.path(), "test", "test", ReplicationMethod::All, None).unwrap();
        Replicator::initialize(td.path(), "test", "test", ReplicationMethod::All, None).unwrap();
    }

    #[test]
    fn test_replica_sync() {
        let (db, _td) = create_db();
        let (db_backup, _td_backup) = create_db();
        let repl = Replicator::initialize(
            _td.path(),
            "test",
            _td_backup.path().to_str().unwrap(),
            ReplicationMethod::All,
            None,
        )
        .unwrap();
        db.set(
            "a",
            SampleDbStruct::new(String::from("a value")),
            OperationTarget::Main,
        )
        .unwrap();
        let result = repl.replicate().unwrap();
        assert!(result);
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

    #[test]
    fn test_replica_non_existing_repo() {
        let (db, _td) = create_db();
        let repl = Replicator::initialize(
            _td.path(),
            "test",
            "https://800.800.800.800/git.git",
            ReplicationMethod::All,
            None,
        )
        .unwrap();
        db.set(
            "a",
            SampleDbStruct::new(String::from("a value")),
            OperationTarget::Main,
        )
        .unwrap();
        let result = repl.replicate();
        assert!(result.is_err());
    }
}
