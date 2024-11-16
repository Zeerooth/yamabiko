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
        Self::ensure_remote(&repo, &remote_name_formatted, remote_url)?;
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
                repo.reference_ensure_log(&ref_name)?;
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
            let tag_name = format!("refs/tags/{}", last_part);
            self.repository.tag_lightweight(
                last_part,
                reference.peel_to_commit()?.as_object(),
                true,
            )?;
            to_push.push(tag_name);
        }
        let glob_rm = format!("refs/history_rm/{}/*", self.remote_name);
        let refs_rm = self.repository.references_glob(glob_rm.as_str())?;
        for reference in refs_rm.flatten() {
            let ref_name = reference.name().unwrap();
            let last_part = ref_name.split('/').last().unwrap();
            let tag_name = format!(":refs/tags/{}", last_part);
            to_push.push(tag_name);
        }
        Ok(to_push)
    }

    fn remove_old_tags(&self, list: &Vec<String>) -> Result<(), git2::Error> {
        for tag in list {
            if tag == "+refs/heads/main" {
                continue;
            }
            let history_tag = tag.replace(format!("refs/tags/{}__", self.remote_name).as_str(), "");
            let reference_name = match history_tag.starts_with(":") {
                true => format!("refs/history_rm/{}/{}", self.remote_name, &history_tag[1..]),
                false => format!("refs/history_tags/{}/{}", self.remote_name, history_tag),
            };
            let reference = self.repository.find_reference(&reference_name);
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

    /// Try to replicate data to the remote specified during Replicator::initialize.
    /// Depending on the chosen ReplicationMethod, it may or may not actually happen.
    /// That's why a bool is returned -> true indicates successful replication, while false means
    /// that the replication was not even attempted (this result might be different when called
    /// again in the future)
    pub fn replicate(&self) -> Result<bool, error::ReplicationError> {
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
        let mut tags_to_remove = Vec::new();
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
        callbacks.push_update_reference(|reference, result| {
            if let Some(_result) = result {
                debug!("Pushing {} failed: {}", reference, _result);
                return Ok(());
            }
            debug!("Pushing {} to {} succeeded", reference, self.remote_name);
            tags_to_remove.push(reference.to_string());
            Ok(())
        });
        let mut push_options = PushOptions::new();
        push_options.remote_callbacks(callbacks);
        let tags_to_push = self.tags_to_push()?;
        remote.push(tags_to_push.as_ref(), Some(&mut push_options))?;
        drop(push_options);
        self.remove_old_tags(&tags_to_remove)?;
        if let ReplicationMethod::Periodic(_) = self.replication_method {
            let current_time = Utc::now().timestamp();
            let mut reflog = self
                .repository
                .reflog(&Self::last_push_ref(self.remote_name.as_str()))?;

            // unwrap: head has to exist and point at something
            let head_target = self.repository.head().unwrap().target().unwrap();

            reflog.append(
                head_target,
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
    use git2::Reference;

    use crate::{
        replica::{ReplicationMethod, Replicator},
        serialization::DataFormat,
        test::{create_db, SampleDbStruct},
        OperationTarget,
    };

    use rstest::rstest;

    #[rstest]
    #[case(DataFormat::Json)]
    #[case(DataFormat::Yaml)]
    #[case(DataFormat::Pot)]
    fn test_replica_same_name(#[case] data_format: DataFormat) {
        let (_, td) = create_db(data_format);
        Replicator::initialize(td.path(), "test", "test", ReplicationMethod::All, None).unwrap();
        Replicator::initialize(td.path(), "test", "test", ReplicationMethod::All, None).unwrap();
    }

    #[rstest]
    #[case(DataFormat::Json)]
    #[case(DataFormat::Yaml)]
    #[case(DataFormat::Pot)]
    fn test_replica_sync(#[case] data_format: DataFormat) {
        let (db, _td) = create_db(data_format);
        let (db_backup, _td_backup) = create_db(data_format);
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

    #[rstest]
    #[case(DataFormat::Json)]
    #[case(DataFormat::Yaml)]
    #[case(DataFormat::Pot)]
    fn test_replica_periodic(#[case] data_format: DataFormat) {
        let (db, _td) = create_db(data_format);
        let (db_backup, _td_backup) = create_db(data_format);
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

    #[rstest]
    #[case(DataFormat::Json)]
    #[case(DataFormat::Yaml)]
    #[case(DataFormat::Pot)]
    fn test_replica_non_existing_repo(#[case] data_format: DataFormat) {
        let (db, _td) = create_db(data_format);
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

    #[rstest]
    #[case(DataFormat::Json)]
    #[case(DataFormat::Yaml)]
    #[case(DataFormat::Pot)]
    fn test_replica_add_and_remove_history_tags(#[case] data_format: DataFormat) {
        let (db, _td) = create_db(data_format);
        let (db_backup, _td_backup) = create_db(data_format);
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
            SampleDbStruct::new(String::from("initial a value")),
            OperationTarget::Main,
        )
        .unwrap();
        db.set(
            "a",
            SampleDbStruct::new(String::from("new a value")),
            OperationTarget::Main,
        )
        .unwrap();
        db.revert_n_commits(1, OperationTarget::Main, true).unwrap();
        repl.replicate().unwrap();

        let db_tags: Vec<Reference> = db
            .repository()
            .references_glob("refs/tags/*")
            .unwrap()
            .map(|x| x.unwrap())
            .collect();
        assert_eq!(db_tags.len(), 1);
        let tag = db_tags.first().unwrap();
        assert!(tag.name().unwrap().starts_with("refs/tags/revert"));

        let db_tags: Vec<Reference> = db_backup
            .repository()
            .references_glob("refs/tags/*")
            .unwrap()
            .map(|x| x.unwrap())
            .collect();
        assert_eq!(db_tags.len(), 1);
        let backup_tag = db_tags.first().unwrap();
        assert_eq!(backup_tag.name().unwrap(), tag.name().unwrap());
    }
}
