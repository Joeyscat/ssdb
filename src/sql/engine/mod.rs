mod kv;
pub use kv::KV;
use sqlparser::ast::Statement;
use sqlparser::dialect::{GenericDialect};
use sqlparser::parser::Parser;

use super::execution::ResultSet;
// use super::parser::{ast, Parser};
use super::plan::Planner;
use super::schema::Catalog;
use super::types::{Expression, Row, Value};
use crate::error::{Error, Result};

use std::collections::HashSet;

/// The SQL engine interface.
pub trait Engine: Clone {
    /// The transaction type
    type Transaction: Transaction;

    /// Begin a new transaction in the given mode
    fn begin(&self, mode: Mode) -> Result<Self::Transaction>;

    /// Begin a session for executing individual statements
    fn session(&self) -> Result<Session<Self>> {
        Ok(Session {
            engine: self.clone(),
            txn: None,
        })
    }

    /// Resume an active transaction with the given ID
    fn resume(&self, id: u64) -> Result<Self::Transaction>;
}

/// An SQL Transaction
pub trait Transaction: Catalog {
    /// The transaction ID
    fn id(&self) -> u64;
    /// The transaction mode
    fn mode(&self) -> Mode;
    /// Commit the transaction
    fn commit(self) -> Result<()>;
    /// Rollback the transaction
    fn rollback(self) -> Result<()>;

    /// Create a new table row
    fn create_row(&mut self, table: &str, row: Row) -> Result<()>;
    /// Delete a table row
    fn delete_row(&mut self, table: &str, id: &Value) -> Result<()>;
    /// Read a table row, if it exists
    fn read_row(&self, table: &str, id: &Value) -> Result<Option<Row>>;
    /// Update a table row
    fn update_row(&mut self, table: &str, id: &Value, row: Row) -> Result<()>;
    /// Read an index entry, if it exists
    fn read_index(&self, table: &str, column: &str, id: &Value) -> Result<HashSet<Value>>;
    /// Scan a table
    fn scan(&self, table: &str, filter: Option<Expression>) -> Result<Scan>;
    /// Scan a column's index entries
    fn scan_index(&self, table: &str, column: &str) -> Result<IndexScan>;
}

/// An SQL session, which handles transaction management, and simplified query execution
pub struct Session<E: Engine> {
    /// The underlying engine
    engine: E,
    /// The active transaction
    txn: Option<E::Transaction>,
}

impl<E: Engine + 'static> Session<E> {
    /// Execute a single SQL statement, managing the transaction state for the session
    pub fn execute(&mut self, query: &str) -> Result<ResultSet> {
        let statement = Parser::new(&GenericDialect {})
            .try_with_sql(query)?
            .parse_statement()?;

        match statement {
            Statement::Query(query) => {
                let mut txn = self.engine.begin(Mode::ReadOnly)?;
                let result = Planner::build(statement, &mut txn)?
                    .optimize(&mut txn)?
                    .execute(&mut txn);
                txn.rollback()?;
                result
            }
            statement => Err(Error::Parse(format!(
                "Unsupported statement: {:?}",
                statement
            ))),
        }
    }
}

/// The transaction mode
pub type Mode = crate::storage::kv::mvcc::Mode;

/// A row scan iterator
pub type Scan = Box<dyn DoubleEndedIterator<Item = Result<Row>> + Send>;

/// An index scan iterator
pub type IndexScan = Box<dyn DoubleEndedIterator<Item = Result<(Value, HashSet<Value>)>> + Send>;
