use git2::{Commit, Oid};

#[derive(Debug)]
pub enum RevertError {
    BranchingHistory { commit: Oid },
}
