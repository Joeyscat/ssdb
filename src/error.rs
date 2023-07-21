use std::fmt::Display;

use serde_derive::{Deserialize, Serialize};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Error {
    Abort,
    Config(String),
    Internal(String),
    Parse(String),
    ReadOnly,
    Serialization,
    Value(String),
}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Abort => write!(f, "Operation aborted"),
            Error::Config(s) | Error::Internal(s) | Error::Parse(s) | Error::Value(s) => {
                write!(f, "{}", s)
            }
            Error::Serialization => write!(f, "Serialization failure, retry transaction"),
            Error::ReadOnly => write!(f, "Read-only transaction"),
        }
    }
}

