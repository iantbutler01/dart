use std::fmt::Display;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Errors {
    #[error("The data for key `{0}` does not exist in the tree.")]
    NonExistantError(String),
    #[error("There was an error attempting to remove the key: {0}")]
    RemoveError(String),
    #[error("Attempted an operation on an empty tree.")]
    EmptyTreeError,
    #[error("There was an error attempting to insert the key: {0}")]
    InsertError(String),
    #[error("There was an error attempting to get the key: {0}")]
    GetError(String),
    #[error("Locking error.")]
    LockingError(OptimisticLockCouplingErrorType),
}

impl From<OptimisticLockCouplingErrorType> for Errors {
    fn from(value: OptimisticLockCouplingErrorType) -> Self {
        Errors::LockingError(value)
    }
}

#[derive(Error, Debug)]
pub enum InsertErrors {
    #[error("Locking error.")]
    LockingError(OptimisticLockCouplingErrorType),
}

impl From<OptimisticLockCouplingErrorType> for InsertErrors {
    fn from(value: OptimisticLockCouplingErrorType) -> Self {
        InsertErrors::LockingError(value)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Error)]
pub enum OptimisticLockCouplingErrorType {
    /// writer thread panics without release the lock
    Poisoned,
    /// writer thread set this data is outdated
    Outdated,
    /// writer thread blocks the reader thread
    Blocked,
    /// reader thead try to sync after writer thread write things into lock
    VersionUpdated,
}

impl Display for OptimisticLockCouplingErrorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}
