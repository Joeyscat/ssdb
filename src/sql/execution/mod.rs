use derivative::Derivative;
use serde_derive::{Deserialize, Serialize};

use crate::error::{Error, Result};

use super::{
    engine::{Mode, Transaction},
    plan::Node,
    types::{Columns, Row, Rows, Value},
};

/// 执行器
pub trait Executor<T: Transaction> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet>;
}

impl<T: Transaction + 'static> dyn Executor<T> {
    pub fn build(_node: Node) -> Box<dyn Executor<T>> {
        unimplemented!()
    }
}

#[derive(Derivative, Serialize, Deserialize)]
#[derivative(Debug, PartialEq)]
pub enum ResultSet {
    Begin {
        id: u64,
        mode: Mode,
    },

    Commit {
        id: u64,
    },

    Rollback {
        id: u64,
    },

    CreateTable {
        name: String,
    },

    DropTable {
        name: String,
    },

    Create {
        count: u64,
    },

    Delete {
        count: u64,
    },

    Update {
        count: u64,
    },

    Query {
        columns: Columns,
        #[derivative(Debug = "ignore")]
        #[derivative(PartialEq = "ignore")]
        #[serde(skip, default = "ResultSet::empty_rows")]
        rows: Rows,
    },

    Explain(Node),
}

impl ResultSet {
    fn empty_rows() -> Rows {
        Box::new(std::iter::empty())
    }

    pub fn into_row(self) -> Result<Row> {
        if let ResultSet::Query { mut rows, .. } = self {
            rows.next()
                .transpose()?
                .ok_or_else(|| Error::Value("No rows returned".into()))
        } else {
            Err(Error::Value(format!("Not a query result: {:?}", self)))
        }
    }

    pub fn into_value(self) -> Result<Value> {
        self.into_row()?
            .into_iter()
            .next()
            .ok_or_else(|| Error::Value("No value returned".into()))
    }
}
