use git2::Oid;

#[derive(Debug)]
pub enum RevertError {
    BranchingHistory { commit: Oid },
}
