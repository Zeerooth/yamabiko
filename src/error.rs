use git2::Error as GitErr;
use git2::Oid;

#[derive(Debug, PartialEq)]
pub enum InitializationError {
    /// Unknown error caused by git.
    InternalGitError(GitErr),
}

impl From<GitErr> for InitializationError {
    fn from(err: GitErr) -> Self {
        Self::InternalGitError(err)
    }
}

#[derive(Debug, PartialEq)]
pub enum RevertError {
    /// Unable to execute the revert operation - one of the commits in history
    /// has multiple parents and yamabiko doesn't know which one to pick.
    /// Contains the said commit as an argument.
    BranchingHistory(Oid),
    /// There is no such commit with specified Oid.
    TargetCommitNotFound(Oid),
    InvalidOperationTarget,
    /// Unknown error caused by git.
    InternalGitError(GitErr),
}

impl From<GitErr> for RevertError {
    fn from(err: GitErr) -> Self {
        Self::InternalGitError(err)
    }
}

#[derive(Debug, PartialEq)]
pub enum GetObjectError {
    InvalidOperationTarget,
    CorruptedObject,
    InvalidPathToKey(GitErr),
    /// Unknown error caused by git.
    InternalGitError(GitErr),
}

impl From<GitErr> for GetObjectError {
    fn from(err: GitErr) -> Self {
        Self::InternalGitError(err)
    }
}

#[derive(Debug, PartialEq)]
pub enum TransactionError {
    /// Transaction was aborted - only applicable when using ConflictResolution::Abort.
    Aborted,
    /// Transaction (more specifically, a branch with that name) wasn't found among git objects.
    TransactionNotFound,
    /// Unknown error caused by git.
    InternalGitError(GitErr),
}

impl From<GitErr> for TransactionError {
    fn from(err: GitErr) -> Self {
        Self::InternalGitError(err)
    }
}
