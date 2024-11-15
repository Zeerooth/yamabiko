use std::str::Utf8Error;
use std::string::FromUtf8Error;

use git2::Error as GitErr;
use git2::Oid;

#[derive(Debug, PartialEq)]
pub enum InitializationError {
    /// Unknown error caused by git.
    InternalGitError(GitErr),
}

#[derive(Debug, PartialEq)]
pub enum RevertError {
    /// Unable to execute the revert operation - one of the commits in history
    /// has multiple parents and yamabiko doesn't know which one to pick.
    /// Contains the said commit as an argument.
    BranchingHistory(Oid),
    /// There is no such commit with specified Oid.
    TargetCommitNotFound(Oid),
    /// OperationTarget the function was invoked with does not exist.
    InvalidOperationTarget,
    /// Unknown error caused by git.
    InternalGitError(GitErr),
}

#[derive(Debug, PartialEq)]
pub enum SetObjectError {
    /// OperationTarget the function was invoked with does not exist.
    InvalidOperationTarget,
    /// Unknown error caused by git.
    InternalGitError(GitErr),
}

#[derive(Debug, PartialEq)]
pub enum GetObjectError {
    InvalidOperationTarget,
    CorruptedObject,
    ValueIsNotValidUTF8(Utf8Error),
    InvalidKey(KeyError),
    /// Unknown error caused by git.
    InternalGitError(GitErr),
}

impl From<KeyError> for GetObjectError {
    fn from(err: KeyError) -> Self {
        Self::InvalidKey(err)
    }
}

impl From<Utf8Error> for GetObjectError {
    fn from(err: Utf8Error) -> Self {
        Self::ValueIsNotValidUTF8(err)
    }
}

impl From<FromUtf8Error> for GetObjectError {
    fn from(err: FromUtf8Error) -> Self {
        Self::ValueIsNotValidUTF8(err.utf8_error())
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

#[derive(Debug, PartialEq)]
pub enum KeyError {
    NotHashable(GitErr),
}

#[derive(Debug, PartialEq, Eq)]
pub struct InvalidDataFormatError;

#[derive(Debug, PartialEq)]
pub enum ReplicationError {
    /// Unknown error caused by git.
    InternalGitError(GitErr),
}

#[derive(Debug, PartialEq)]
pub enum QueryError {
    /// Unknown error caused by git.
    InternalGitError(GitErr),
}

macro_rules! impl_GitErr {
    ($($t:ty),+) => {
        $(impl From<GitErr> for $t {
            fn from(err: GitErr) -> Self {
                Self::InternalGitError(err)
            }
        })*
    }
}

impl_GitErr!(
    InitializationError,
    RevertError,
    SetObjectError,
    GetObjectError,
    TransactionError,
    ReplicationError,
    QueryError
);
