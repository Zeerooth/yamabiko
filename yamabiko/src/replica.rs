use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use git2::{Cred, ErrorCode, PushOptions, Reference, Remote, RemoteCallbacks, Repository};
use rand::Rng;

use crate::{debug, error, RepositoryAbstraction};

#[derive(Clone)]
pub enum ReplicationMethod {
    All,
    Periodic(i64),
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
        let remote_name_formatted = format!("_repl_{}", remote_name);
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

    fn last_push_ref(remote_name: &str) -> String {
        format!("refs/replicas/{}_last_push", remote_name)
    }

    fn resolve_periodic_ref<'a>(
        repo: &'a Repository,
        remote_name: &str,
    ) -> Result<Reference<'a>, git2::Error> {
        let ref_name = Self::last_push_ref(remote_name);
        let reference = repo.find_reference(&ref_name);
        match reference {
            Ok(reference) => Ok(reference),
            Err(err) => {
                if err.code() != ErrorCode::NotFound {
                    return Err(err);
                }
                let reference = repo.reference_symbolic(ref_name.as_str(), "HEAD", false, "")?;
                repo.reference_ensure_log(&ref_name).unwrap();
                let mut reflog = repo.reflog(&ref_name)?;
                let head = repo.head().unwrap();
                reflog.append(
                    head.target().unwrap(),
                    &Self::signature(),
                    Some(0.to_string().as_str()),
                )?;
                reflog.write()?;
                Ok(reference)
            }
        }
    }

    fn tags_to_push(&self) -> Result<Vec<String>, git2::Error> {
        let glob = format!("refs/history_tags/{}/*", self.remote_name);
        let refs = self.repository.references_glob(glob.as_str())?;
        let mut to_push = Vec::new();
        to_push.push(String::from("+refs/heads/main"));
        for reference in refs.flatten() {
            let ref_name = reference.name().unwrap();
            let last_part = ref_name.split('/').last().unwrap();
            let tag_name = format!("refs/tags/{}__{}", self.remote_name, last_part);
            self.repository.tag_lightweight(
                tag_name.as_str(),
                reference.peel_to_commit()?.as_object(),
                true,
            )?;
            to_push.push(tag_name);
        }
        Ok(to_push)
    }

    fn remove_old_tags(&self, list: Vec<String>) -> Result<(), git2::Error> {
        for tag in list {
            if tag == "+refs/heads/main" {
                continue;
            }
            let history_tag = tag.replace(format!("refs/tags/{}__", self.remote_name).as_str(), "");
            let reference = self.repository.find_reference(&format!(
                "refs/history_tags/{}/{}",
                self.remote_name, history_tag
            ));
            match reference {
                Ok(mut reference) => reference.delete()?,
                Err(err) => {
                    if err.code() != ErrorCode::NotFound {
                        return Err(err);
                    }
                }
            }
        }
        Ok(())
    }

    pub fn replicate(&self) -> Result<bool, git2::Error> {
        let rand_res: f64 = rand::thread_rng().gen();
        let replicate = match self.replication_method {
            ReplicationMethod::All => true,
            ReplicationMethod::Random(chance) => rand_res < chance,
            ReplicationMethod::Periodic(peroid) => {
                Self::resolve_periodic_ref(&self.repository, &self.remote_name)?;
                let reflog = &self
                    .repository
                    .reflog(Self::last_push_ref(self.remote_name.as_str()).as_str())?;
                debug!("Reflog has {} entries", reflog.len());
                let last_push = reflog.get(0).unwrap().message().unwrap().parse().unwrap();
                let next_push_timestamp = DateTime::from_timestamp(last_push, 0).unwrap();
                next_push_timestamp.timestamp() + peroid < Utc::now().timestamp()
            }
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
        let tags_to_push = self.tags_to_push()?;
        remote.push(tags_to_push.as_ref(), Some(&mut push_options))?;
        self.remove_old_tags(tags_to_push)?;
        if let ReplicationMethod::Periodic(_) = self.replication_method {
            let current_time = Utc::now().timestamp();
            let mut reflog = self
                .repository
                .reflog(&Self::last_push_ref(self.remote_name.as_str()))?;
            let head = self.repository.head().unwrap();
            reflog.append(
                head.target().unwrap(),
                &Self::signature(),
                Some(current_time.to_string().as_str()),
            )?;
            reflog.write()?;
        }
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
    fn test_replica_periodic() {
        let (db, _td) = create_db();
        let (db_backup, _td_backup) = create_db();
        let repl = Replicator::initialize(
            _td.path(),
            "test",
            _td_backup.path().to_str().unwrap(),
            ReplicationMethod::Periodic(0),
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
