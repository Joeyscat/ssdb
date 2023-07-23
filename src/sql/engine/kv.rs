use super::super::schema::{Catalog, Table, Tables};
use super::super::types::{Expression, Row, Value};
use super::Transaction as _;
use crate::error::{Error, Result};
use crate::storage::kv;

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::borrow::Cow;
use std::clone::Clone;


/// A SQL engine based on an underlying MVCC key-value store
pub struct KV {
    /// The underlying key-value store
    pub(super)kv: kv::MVCC,
}

