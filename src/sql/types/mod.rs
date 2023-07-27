mod expression;
pub use expression::Expression;

use crate::error::{Error, Result};

use serde_derive::{Deserialize, Serialize};
use std::{borrow::Cow, cmp::Ordering, hash::Hash};

/// A data type
#[derive(Debug, Clone, PartialEq, Hash, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    Integer,
    Float,
    String,
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Boolean => "BOOLEAN",
            Self::Integer => "INTEGER",
            Self::Float => "FLOAT",
            Self::String => "STRING",
        })
    }
}

/// A specific value of a data type
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

impl std::cmp::Eq for Value {}

#[allow(clippy::derive_hash_xor_eq)]
impl Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.data_type().hash(state);
        match self {
            Self::Null => self.hash(state),
            Self::Boolean(v) => v.hash(state),
            Self::Integer(v) => v.hash(state),
            Self::Float(v) => v.to_be_bytes().hash(state),
            Self::String(v) => v.hash(state),
        }
    }
}

impl<'a> From<Value> for Cow<'a, Value> {
    fn from(value: Value) -> Self {
        Cow::Owned(value)
    }
}

impl<'a> From<&'a Value> for Cow<'a, Value> {
    fn from(value: &'a Value) -> Self {
        Cow::Borrowed(value)
    }
}

impl Value {
    /// Returns the value's data type, or None if it is null
    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Self::Null => None,
            Self::Boolean(_) => Some(DataType::Boolean),
            Self::Integer(_) => Some(DataType::Integer),
            Self::Float(_) => Some(DataType::Float),
            Self::String(_) => Some(DataType::String),
        }
    }

    /// Returns the inner boolean, or an error if the value is not a boolean
    pub fn boolean(self) -> Result<bool> {
        match self {
            Self::Boolean(v) => Ok(v),
            v => Err(Error::Value(format!("Not a boolean: {:?}", v))),
        }
    }

    /// Returns the inner integer, or an error if the value is not an integer
    pub fn integer(self) -> Result<i64> {
        match self {
            Self::Integer(v) => Ok(v),
            v => Err(Error::Value(format!("Not an integer: {:?}", v))),
        }
    }

    /// Returns the inner float, or an error if the value is not a float
    pub fn float(self) -> Result<f64> {
        match self {
            Self::Float(v) => Ok(v),
            v => Err(Error::Value(format!("Not a float: {:?}", v))),
        }
    }

    /// Returns the inner string, or an error if the value is not a string
    pub fn string(self) -> Result<String> {
        match self {
            Self::String(v) => Ok(v),
            v => Err(Error::Value(format!("Not a string: {:?}", v))),
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            match self {
                Self::Null => "NULL".to_string(),
                Self::Boolean(v) if *v => "TRUE".to_string(),
                Self::Boolean(_) => "FALSE".to_string(),
                Self::Integer(v) => v.to_string(),
                Self::Float(v) => v.to_string(),
                Self::String(v) => v.clone(),
            }
            .as_ref(),
        )
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Null, Self::Null) => Some(Ordering::Equal),
            (Self::Null, _) => Some(Ordering::Less),
            (_, Self::Null) => Some(Ordering::Greater),
            (Self::Boolean(a), Self::Boolean(b)) => a.partial_cmp(b),
            (Self::Integer(a), Self::Integer(b)) => a.partial_cmp(b),
            (Self::Integer(a), Self::Float(b)) => (*a as f64).partial_cmp(b),
            (Self::Float(a), Self::Float(b)) => a.partial_cmp(b),
            (Self::Float(a), Self::Integer(b)) => a.partial_cmp(&(*b as f64)),
            (Self::String(a), Self::String(b)) => a.partial_cmp(b),
            (_, _) => None,
        }
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

/// A row of values
pub type Row = Vec<Value>;

/// A row iterator
pub type Rows = Box<dyn Iterator<Item = Result<Row>> + Send>;

/// A column (in a result set, see schema::Column for table columns)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: Option<String>,
}

/// A set of columns
pub type Columns = Vec<Column>;
