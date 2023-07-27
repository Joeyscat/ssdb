//! SQL引擎, 提供了基本的CRUD存储操作。
mod kv;
pub use kv::KV;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use super::execution::ResultSet;
use super::plan::Plan;
use super::schema::Catalog;
use super::types::{Expression, Row, Value};
use crate::error::{Error, Result};

use std::collections::HashSet;

/// SQL引擎接口
pub trait Engine: Clone {
    /// 事务类型
    type Transaction: Transaction;

    /// 开始一个新的事务
    fn begin(&self, mode: Mode) -> Result<Self::Transaction>;

    /// 开始一个会话，用于执行单个语句
    fn session(&self) -> Result<Session<Self>> {
        Ok(Session {
            engine: self.clone(),
            txn: None,
        })
    }

    /// 用给定的ID恢复一个活动的事务
    fn resume(&self, id: u64) -> Result<Self::Transaction>;
}

/// SQL事务
pub trait Transaction: Catalog {
    /// 事务ID
    fn id(&self) -> u64;
    /// 事务模式
    fn mode(&self) -> Mode;
    /// 提交事务
    fn commit(self) -> Result<()>;
    /// 回滚事务
    fn rollback(self) -> Result<()>;

    /// 创建新行
    fn create_row(&mut self, table: &str, row: Row) -> Result<()>;
    /// 删除行
    fn delete_row(&mut self, table: &str, id: &Value) -> Result<()>;
    /// 读取行
    fn read_row(&self, table: &str, id: &Value) -> Result<Option<Row>>;
    /// 更新行
    fn update_row(&mut self, table: &str, id: &Value, row: Row) -> Result<()>;
    /// 读取索引
    fn read_index(&self, table: &str, column: &str, value: &Value) -> Result<HashSet<Value>>;
    /// 扫描表
    fn scan(&self, table: &str, filter: Option<Expression>) -> Result<Scan>;
    /// 扫描索引
    fn scan_index(&self, table: &str, column: &str) -> Result<IndexScan>;
}

/// SQL会话，处理事务管理和简化的查询执行
pub struct Session<E: Engine> {
    /// 底层引擎
    engine: E,
    /// 活动事务
    txn: Option<E::Transaction>,
}

impl<E: Engine + 'static> Session<E> {
    /// 执行单个SQL语句，管理会话的事务状态
    pub fn execute(&mut self, query: &str) -> Result<ResultSet> {
        let statement = Parser::new(&GenericDialect {})
            .try_with_sql(query)?
            .parse_statement()?;

        match statement {
            statement @ Statement::Query(..) => {
                let mut txn = self.engine.begin(Mode::ReadOnly)?;
                let result = Plan::build(statement.clone(), &mut txn)?
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

    pub fn with_txn<R, F>(&mut self, mode: Mode, f: F) -> Result<R>
    where
        F: FnOnce(&mut E::Transaction) -> Result<R>,
    {
        if let Some(ref mut txn) = self.txn {
            if !txn.mode().satisfies(&mode) {
                return Err(Error::Value(
                    "The operation cannot be performed in the current transaction mode".into(),
                ));
            }
            return f(txn);
        }
        let mut txn = self.engine.begin(mode)?;
        let result = f(&mut txn);
        txn.rollback()?;
        result
    }
}

/// 事务模式
pub type Mode = crate::storage::kv::mvcc::Mode;

/// 行扫描迭代器
pub type Scan = Box<dyn DoubleEndedIterator<Item = Result<Row>> + Send>;

/// 索引扫描迭代器
pub type IndexScan = Box<dyn DoubleEndedIterator<Item = Result<(Value, HashSet<Value>)>> + Send>;
