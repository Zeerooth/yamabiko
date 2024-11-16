use core::str;
use std::path::Path;

use chrono::DateTime;
use git2::{
    build::CheckoutBuilder, BranchType, IndexEntry, MergeOptions, Oid, RebaseOptions, Repository,
};

use crate::{debug, error, RepositoryAbstraction};

pub struct Squasher {
    repository: Repository,
}

impl RepositoryAbstraction for Squasher {}

impl Squasher {
    pub fn initialize(path: &Path) -> Result<Self, error::InitializationError> {
        let repo = Self::load_or_create_repo(path)?;
        Ok(Self { repository: repo })
    }

    pub fn cleanup_revert_history_tags(
        &self,
        timestamp_before: i64,
        stage_tag_cleanup_for_remotes_as_well: bool,
    ) -> Result<(), git2::Error> {
        let timestamp_before = DateTime::from_timestamp(timestamp_before, 0).unwrap();
        let pattern = "revert-*";
        let tags = self.repository.tag_names(Some(pattern))?;
        for tag in tags.iter() {
            let Some(tag) = tag else { continue };
            let tag_last_part = tag.split("/").last().unwrap();
            if let Some(ts) = tag.split("-").last() {
                if let Ok(ts) = ts.parse::<i64>() {
                    let timestamp = DateTime::from_timestamp(ts, 0).unwrap();
                    if timestamp < timestamp_before {
                        self.repository.tag_delete(tag)?;
                        if stage_tag_cleanup_for_remotes_as_well {
                            for remote in self.repository.remotes()?.iter().flatten() {
                                let ref_name =
                                    format!("refs/history_rm/{}/{}", remote, tag_last_part);
                                let head = self.repository.head()?.target().unwrap();
                                self.repository.reference(&ref_name, head, true, "")?;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn squash_before_commit(&self, commit: Oid) -> Result<(), git2::Error> {
        let annotated_commit = self.repository.find_annotated_commit(commit)?;
        let mut checkout_options = CheckoutBuilder::default();
        checkout_options.force();
        checkout_options.allow_conflicts(true);
        let mut merge_options = MergeOptions::default();
        merge_options.fail_on_conflict(false);
        let mut rebase_options = RebaseOptions::default();
        rebase_options.inmemory(true);
        rebase_options.merge_options(merge_options);
        rebase_options.checkout_options(checkout_options);

        let signature = &Self::signature();

        let treebuilder = self.repository.treebuilder(None)?;
        let new_root_tree_id = treebuilder.write()?;
        let new_root_tree = self.repository.find_tree(new_root_tree_id)?;
        let new_root_commit_id = self.repository.commit(
            None,
            &Self::signature(),
            &Self::signature(),
            "squash old commits",
            &new_root_tree,
            &[],
        )?;
        let new_root_commit = self.repository.find_annotated_commit(new_root_commit_id)?;
        let new_root_commit_normal = self.repository.find_commit(new_root_commit_id)?;
        debug!("New orphan commit id is {}", new_root_tree.id());

        let reference = self.repository.find_branch("main", BranchType::Local)?;
        let main_commit = self
            .repository
            .reference_to_annotated_commit(reference.get())?;
        let mut rebase = self.repository.rebase(
            Some(&new_root_commit),
            Some(&annotated_commit),
            Some(&main_commit),
            Some(&mut rebase_options),
        )?;
        while let Some(operation) = rebase.next() {
            let _op = operation?;
            debug!(
                "Performing rebase operation {:?} on commit {}",
                _op.kind(),
                _op.id()
            );
            let mut index = rebase.inmemory_index()?;
            let mut to_remove: Vec<(Vec<u8>, i32)> = Vec::new();
            let mut to_keep: Vec<IndexEntry> = Vec::new();
            for conflict in index.conflicts()?.by_ref() {
                let conflict = conflict?;
                if let Some(our) = conflict.our {
                    to_remove.push((our.path.clone(), 2));
                    to_keep.push(our);
                }
                if let Some(their) = conflict.their {
                    to_remove.push((their.path.clone(), 3));
                }
                if let Some(ancestor) = conflict.ancestor {
                    to_remove.push((ancestor.path, 1));
                }
            }
            for (path, stage) in to_remove {
                let parsed_path = str::from_utf8(path.as_ref()).unwrap();
                debug!("Removing entry {} with stage {}", parsed_path, stage);
                index.remove(Path::new(parsed_path), stage)?;
            }
            for mut entry in to_keep {
                debug!(
                    "Adding entry {} for stage 0",
                    str::from_utf8(entry.path.clone().as_ref()).unwrap()
                );
                entry.flags = 0;
                index.add(&entry)?;
            }
            debug!(
                "Conflicts resolved. Has conflicts? {}",
                index.has_conflicts()
            );
        }
        let mut index = rebase.inmemory_index()?;
        let final_tree_id = index.write_tree_to(&self.repository)?;
        let final_tree = self.repository.find_tree(final_tree_id)?;
        debug!("New tree is {}", final_tree_id);
        let final_commit = self.repository.commit(
            None,
            signature,
            signature,
            "",
            &final_tree,
            &[&new_root_commit_normal],
        )?;
        debug!("New tip is {}", final_commit);
        self.repository
            .reference("refs/heads/main", final_commit, true, "")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use git2::{BranchType, Repository};

    use crate::{serialization::DataFormat, squash::Squasher, test::*, OperationTarget};

    use rstest::rstest;

    #[rstest]
    #[case(DataFormat::Json)]
    #[case(DataFormat::Yaml)]
    fn test_simple_squash_of_3_commits(#[case] data_format: DataFormat) {
        let (db, td) = create_db(data_format);
        let squasher = Squasher::initialize(td.path()).unwrap();
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
        squasher
            .squash_before_commit(head_commit.parent(0).unwrap().id())
            .expect("Squash failed");
        assert_eq!(
            db.get::<SampleDbStruct>("a", OperationTarget::Main)
                .unwrap()
                .unwrap(),
            SampleDbStruct {
                str_val: String::from("change #2")
            }
        );
        let new_reference = repo
            .find_branch("main", BranchType::Local)
            .unwrap()
            .into_reference();
        let new_head_commit = new_reference.peel_to_commit().unwrap();
        assert_ne!(new_head_commit.id(), head_commit.id());
        assert_eq!(new_head_commit.parent_count(), 1);
        assert_eq!(new_head_commit.parent(0).unwrap().parent_count(), 0);
    }

    #[rstest]
    #[case(DataFormat::Json)]
    #[case(DataFormat::Yaml)]
    fn test_squash_with_revert_and_saved_history(#[case] data_format: DataFormat) {
        let (db, td) = create_db(data_format);
        let squasher = Squasher::initialize(td.path()).unwrap();
        db.set(
            "pref1/a",
            SampleDbStruct::new(String::from("initial a value")),
            OperationTarget::Main,
        )
        .unwrap();
        db.set(
            "pref1/b",
            SampleDbStruct::new(String::from("initial b value")),
            OperationTarget::Main,
        )
        .unwrap();
        db.set(
            "pref2/c",
            SampleDbStruct::new(String::from("initial c value")),
            OperationTarget::Main,
        )
        .unwrap();
        db.set(
            "pref1/b",
            SampleDbStruct::new(String::from("new b value")),
            OperationTarget::Main,
        )
        .unwrap();
        db.revert_n_commits(2, OperationTarget::Main, true).unwrap();
        db.set(
            "pref2/d",
            SampleDbStruct::new(String::from("initial d value")),
            OperationTarget::Main,
        )
        .unwrap();

        let repo = Repository::open(td.path()).unwrap();
        let head_commit = repo.head().unwrap().peel_to_commit().unwrap();
        squasher
            .squash_before_commit(head_commit.parent(0).unwrap().id())
            .expect("Squash failed");
        assert_eq!(
            db.get::<SampleDbStruct>("pref1/b", OperationTarget::Main)
                .unwrap()
                .unwrap(),
            SampleDbStruct {
                str_val: String::from("initial b value")
            }
        );
        assert!(db
            .get::<SampleDbStruct>("pref2/c", OperationTarget::Main)
            .unwrap()
            .is_none());
        let new_head_commit = repo.head().unwrap().peel_to_commit().unwrap();
        assert_ne!(new_head_commit.id(), head_commit.id());
        assert_eq!(new_head_commit.parent_count(), 1);
        assert_eq!(new_head_commit.parent(0).unwrap().parent_count(), 0);
    }

    #[rstest]
    #[case(DataFormat::Json)]
    #[case(DataFormat::Yaml)]
    fn test_squash_of_5_commits_with_multiple_keys_modified(#[case] data_format: DataFormat) {
        let (db, td) = create_db(data_format);
        let squasher = Squasher::initialize(td.path()).unwrap();
        db.set(
            "pref1/a",
            SampleDbStruct::new(String::from("initial a value")),
            OperationTarget::Main,
        )
        .unwrap();
        db.set(
            "pref1/b",
            SampleDbStruct::new(String::from("initial b value")),
            OperationTarget::Main,
        )
        .unwrap();
        db.set(
            "pref2/c",
            SampleDbStruct::new(String::from("initial c value")),
            OperationTarget::Main,
        )
        .unwrap();
        db.set(
            "pref1/b",
            SampleDbStruct::new(String::from("new b value")),
            OperationTarget::Main,
        )
        .unwrap();
        db.set(
            "pref2/d",
            SampleDbStruct::new(String::from("initial d value")),
            OperationTarget::Main,
        )
        .unwrap();

        let repo = Repository::open(td.path()).unwrap();
        let head_commit = repo.head().unwrap().peel_to_commit().unwrap();
        squasher
            .squash_before_commit(head_commit.parent(0).unwrap().id())
            .expect("Squash failed");
        assert_eq!(
            db.get::<SampleDbStruct>("pref1/b", OperationTarget::Main)
                .unwrap()
                .unwrap(),
            SampleDbStruct {
                str_val: String::from("new b value")
            }
        );
        assert_eq!(
            db.get::<SampleDbStruct>("pref2/c", OperationTarget::Main)
                .unwrap()
                .unwrap(),
            SampleDbStruct {
                str_val: String::from("initial c value")
            }
        );
        let new_head_commit = repo.head().unwrap().peel_to_commit().unwrap();
        assert_ne!(new_head_commit.id(), head_commit.id());
        assert_eq!(new_head_commit.parent_count(), 1);
        assert_eq!(new_head_commit.parent(0).unwrap().parent_count(), 0);
    }

    #[tokio::test]
    async fn test_no_discarded_changes_while_squashing() {
        let (db, td) = create_db(DataFormat::Json);
        let squasher = Squasher::initialize(td.path()).unwrap();
        for i in 0..1000 {
            db.set(
                "a",
                ComplexDbStruct::new(format!("change #{}", i), i, 4.20),
                OperationTarget::Main,
            )
            .unwrap();
        }
        let repo = Repository::open(td.path()).unwrap();
        let reference = repo
            .find_branch("main", BranchType::Local)
            .unwrap()
            .into_reference();
        let head_commit = reference.peel_to_commit().unwrap();
        let head_commit_parent_id = head_commit.parent(0).unwrap().id();
        let squash_task = tokio::spawn(async move {
            squasher
                .squash_before_commit(head_commit_parent_id)
                .unwrap();
        });
        for i in 1000..2001 {
            let t = db
                .get::<ComplexDbStruct>("a", OperationTarget::Main)
                .unwrap()
                .unwrap();
            db.set(
                "a",
                ComplexDbStruct::new(format!("change #{}", i), t.usize_val + 1, t.float_val),
                OperationTarget::Main,
            )
            .unwrap();
        }
        squash_task.await.unwrap();
        assert_eq!(
            db.get::<ComplexDbStruct>("a", OperationTarget::Main)
                .unwrap()
                .unwrap(),
            ComplexDbStruct {
                str_val: String::from("change #2000"),
                usize_val: 2000,
                float_val: 4.20
            }
        );
    }
}
