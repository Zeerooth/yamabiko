use git2::Error as GitErr;
use git2::Oid;

#[derive(Debug)]
pub enum CollectionInitError {
    InternalGitError(GitErr),
}

impl From<GitErr> for CollectionInitError {
    fn from(err: GitErr) -> Self {
        Self::InternalGitError(err)
    }
}

#[derive(Debug)]
pub enum RevertError {
    BranchingHistory { commit: Oid },
    InternalGitError(GitErr),
}

impl From<GitErr> for RevertError {
    fn from(err: GitErr) -> Self {
        Self::InternalGitError(err)
    }
}
