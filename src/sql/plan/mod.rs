mod optimizer;
mod planner;
use std::fmt::Display;

use optimizer::Optimizer as _;
use planner::Planner;

use super::engine::Transaction;
use super::execution::{Executor, ResultSet};
use super::schema::{Catalog, Table};
use super::types::{Expression, Value};
use crate::error::Result;

use serde_derive::{Deserialize, Serialize};
use sqlparser::ast;

/// 查询计划
#[derive(Debug)]
pub struct Plan(pub Node);

impl Plan {
    /// 根据语句构建计划
    pub fn build<C: Catalog>(statement: ast::Statement, catalog: &mut C) -> Result<Self> {
        Planner::new(catalog).build(statement)
    }

    /// 执行计划
    pub fn execute<T: Transaction + 'static>(self, txn: &mut T) -> Result<ResultSet> {
        <dyn Executor<T>>::build(self.0).execute(txn)
    }

    /// 优化计划
    pub fn optimize<C: Catalog>(self, _catalog: &mut C) -> Result<Self> {
        unimplemented!()
    }
}

impl Display for Plan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.to_string())
    }
}

/// 计划节点
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Node {
    Aggregate {
        source: Box<Node>,
        aggregates: Vec<Aggregate>,
    },
    CreateTable {
        schema: Table,
    },
    Delete {
        table: String,
        source: Box<Node>,
    },
    DropTable {
        table: String,
    },
    Filter {
        source: Box<Node>,
        predicate: Expression,
    },
    HashJoin {
        left: Box<Node>,
        left_field: (usize, Option<(Option<String>, String)>),
        right: Box<Node>,
        right_field: (usize, Option<(Option<String>, String)>),
        outer: bool,
    },
    IndexLookup {
        table: String,
        alias: Option<String>,
        column: String,
        values: Vec<Value>,
    },
    Insert {
        table: String,
        columns: Vec<String>,
        expressions: Vec<Vec<Expression>>,
    },
    KeyLookup {
        table: String,
        alias: Option<String>,
        keys: Vec<Value>,
    },
    Limit {
        source: Box<Node>,
        limit: usize,
    },
    NestedLoopJoin {
        left: Box<Node>,
        left_size: usize,
        right: Box<Node>,
        predicate: Option<Expression>,
        outer: bool,
    },
    Nothing,
    Offset {
        source: Box<Node>,
        offset: usize,
    },
    Order {
        source: Box<Node>,
        orders: Vec<(Expression, Direction)>,
    },
    Projection {
        source: Box<Node>,
        expressions: Vec<(Expression, Option<String>)>,
    },
    Scan {
        table: String,
        alias: Option<String>,
        filter: Option<Expression>,
    },
    Update {
        table: String,
        source: Box<Node>,
        assignments: Vec<(usize, Option<String>, Expression)>,
    },
}

impl Node {
    /// Recursively transforms nodes by applying functions `before` and `after` descending.
    pub fn transform<B, A>(mut self, before: &B, after: &A) -> Result<Self>
    where
        B: Fn(Self) -> Result<Self>,
        A: Fn(Self) -> Result<Self>,
    {
        self = before(self)?;

        self = match self {
            n @ Self::CreateTable { .. }
            | n @ Self::DropTable { .. }
            | n @ Self::IndexLookup { .. }
            | n @ Self::Insert { .. }
            | n @ Self::KeyLookup { .. }
            | n @ Self::Nothing
            | n @ Self::Scan { .. } => n,

            Self::Aggregate { source, aggregates } => Self::Aggregate {
                source: Box::new(source.transform(before, after)?).into(),
                aggregates,
            },
            Self::Delete { table, source } => Self::Delete {
                table,
                source: source.transform(before, after)?.into(),
            },
            Self::Filter { source, predicate } => Self::Filter {
                source: source.transform(before, after)?.into(),
                predicate,
            },
            Self::HashJoin {
                left,
                left_field,
                right,
                right_field,
                outer,
            } => Self::HashJoin {
                left: left.transform(before, after)?.into(),
                left_field,
                right: right.transform(before, after)?.into(),
                right_field,
                outer,
            },
            Self::Limit { source, limit } => Self::Limit {
                source: source.transform(before, after)?.into(),
                limit,
            },
            Self::NestedLoopJoin {
                left,
                left_size,
                right,
                predicate,
                outer,
            } => Self::NestedLoopJoin {
                left: left.transform(before, after)?.into(),
                left_size,
                right: right.transform(before, after)?.into(),
                predicate,
                outer,
            },
            Self::Offset { source, offset } => Self::Offset {
                source: source.transform(before, after)?.into(),
                offset,
            },
            Self::Order { source, orders } => Self::Order {
                source: source.transform(before, after)?.into(),
                orders,
            },
            Self::Projection {
                source,
                expressions,
            } => Self::Projection {
                source: source.transform(before, after)?.into(),
                expressions,
            },
            Self::Update {
                table,
                source,
                assignments,
            } => Self::Update {
                table,
                source: source.transform(before, after)?.into(),
                assignments,
            },
        };

        after(self)
    }

    pub fn transform_expressions<B, A>(self, before: &B, after: &A) -> Result<Self>
    where
        B: Fn(Expression) -> Result<Expression>,
        A: Fn(Expression) -> Result<Expression>,
    {
        let node = match self {
            n @ Self::Aggregate { .. }
            | n @ Self::CreateTable { .. }
            | n @ Self::Delete { .. }
            | n @ Self::DropTable { .. }
            | n @ Self::HashJoin { .. }
            | n @ Self::IndexLookup { .. }
            | n @ Self::KeyLookup { .. }
            | n @ Self::Limit { .. }
            | n @ Self::NestedLoopJoin {
                predicate: None, ..
            }
            | n @ Self::Nothing
            | n @ Self::Offset { .. }
            | n @ Self::Scan { filter: None, .. } => n,

            Self::Filter { source, predicate } => Self::Filter {
                source,
                predicate: predicate.transform(before, after)?,
            },
            Self::Insert {
                table,
                columns,
                expressions,
            } => Self::Insert {
                table,
                columns,
                expressions: expressions
                    .into_iter()
                    .map(|e| e.into_iter().map(|e| e.transform(before, after)).collect())
                    .collect::<Result<_>>()?,
            },
            Self::Order { source, orders } => Self::Order {
                source,
                orders: orders
                    .into_iter()
                    .map(|(e, d)| e.transform(before, after).map(|e| (e, d)))
                    .collect::<Result<_>>()?,
            },
            Self::NestedLoopJoin {
                left,
                left_size,
                right,
                predicate: Some(predicate),
                outer,
            } => Self::NestedLoopJoin {
                left,
                left_size,
                right,
                predicate: Some(predicate.transform(before, after)?),
                outer,
            },
            Self::Projection {
                source,
                expressions,
            } => Self::Projection {
                source,
                expressions: expressions
                    .into_iter()
                    .map(|(e, a)| Ok((e.transform(before, after)?, a)))
                    .collect::<Result<_>>()?,
            },
            Self::Scan {
                table,
                alias,
                filter: Some(filter),
            } => Self::Scan {
                table,
                alias,
                filter: Some(filter.transform(before, after)?),
            },
            Self::Update {
                table,
                source,
                assignments,
            } => Self::Update {
                table,
                source,
                assignments: assignments
                    .into_iter()
                    .map(|(i, l, e)| e.transform(before, after).map(|e| (i, l, e)))
                    .collect::<Result<_>>()?,
            },
        };

        Ok(node)
    }

    /// 格式化输出
    pub fn format(&self, mut indent: String, root: bool, last: bool) -> String {
        let mut s = indent.clone();
        if !last {
            s.push_str("├─ ");
            indent.push_str("│  ");
        } else {
            s.push_str("└─ ");
            indent.push_str("   ");
        }
        match self {
            Self::Aggregate { source, aggregates } => {
                s.push_str(&format!(
                    "Aggregate({})\n",
                    aggregates
                        .iter()
                        .map(|a| a.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
                s.push_str(&source.format(indent, false, true));
            }
            Self::CreateTable { schema } => {
                s.push_str(&format!("CreateTable({})\n", schema.name));
            }
            Self::Delete { table, source } => {
                s.push_str(&format!("Delete({})\n", table));
                s.push_str(&source.format(indent, false, true));
            }
            Self::DropTable { table } => {
                s.push_str(&format!("DropTable({})\n", table));
            }
            Self::Filter { source, predicate } => {
                s.push_str(&format!("Filter({})\n", predicate));
                s.push_str(&source.format(indent, false, true));
            }
            Self::HashJoin {
                left,
                left_field,
                right,
                right_field,
                outer,
            } => {
                s.push_str(&format!(
                    "HashJoin({} on {} = {})\n",
                    if *outer { "outer" } else { "inner" },
                    match left_field {
                        (_, Some((Some(t), n))) => format!("{}.{}", t, n),
                        (_, Some((None, n))) => n.clone(),
                        (i, None) => format!("left #{}", i),
                    },
                    match right_field {
                        (_, Some((Some(t), n))) => format!("{}.{}", t, n),
                        (_, Some((None, n))) => n.clone(),
                        (i, None) => format!("right #{}", i),
                    },
                ));
                s.push_str(&left.format(indent.clone(), false, false));
                s.push_str(&right.format(indent, false, true));
            }
            Self::IndexLookup {
                table,
                alias,
                column,
                values,
            } => {
                s.push_str(&format!("IndexLookup({})", table));
                if let Some(alias) = alias {
                    s.push_str(&format!(" as {}", alias));
                }
                s.push_str(&format!(" column {}", column));
                if !values.is_empty() {
                    s.push_str(&format!(
                        " ({})",
                        values
                            .iter()
                            .map(|k| k.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                } else {
                    s.push_str(format!(" ({} values)", values.len()).as_str());
                }
                s.push('\n');
            }
            Self::Insert {
                table,
                columns: _,
                expressions,
            } => {
                s.push_str(format!("Insert({}, {} rows)", table, expressions.len()).as_str());
            }
            Self::KeyLookup { table, alias, keys } => {
                s.push_str(&format!("KeyLookup({})", table));
                if let Some(alias) = alias {
                    s.push_str(&format!(" as {}", alias));
                }
                if !keys.is_empty() && keys.len() < 10 {
                    s.push_str(&format!(
                        " ({})",
                        keys.iter()
                            .map(|k| k.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                } else {
                    s.push_str(format!(" ({} keys)", keys.len()).as_str());
                }
                s.push('\n');
            }
            Self::Limit { source, limit } => {
                s.push_str(&format!("Limit({})\n", limit));
                s.push_str(&source.format(indent, false, true));
            }
            Self::NestedLoopJoin {
                left,
                left_size: _,
                right,
                predicate,
                outer,
            } => {
                s.push_str(
                    format!("NestedLoopJoin({}", if *outer { "outer" } else { "inner" }).as_str(),
                );
                if let Some(predicate) = predicate {
                    s.push_str(&format!(" on {})\n", predicate));
                } else {
                    s.push_str(")\n");
                }
                s.push_str(&left.format(indent.clone(), false, false));
                s.push_str(&right.format(indent, false, true));
            }
            Self::Nothing => {
                s.push_str("Nothing\n");
            }
            Self::Offset { source, offset } => {
                s.push_str(&format!("Offset({})\n", offset));
                s.push_str(&source.format(indent, false, true));
            }
            Self::Order { source, orders } => {
                s.push_str(&format!(
                    "Order({})\n",
                    orders
                        .iter()
                        .map(|(e, d)| format!("{} {}", e, d))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
                s.push_str(&source.format(indent, false, true));
            }
            Self::Projection {
                source,
                expressions,
            } => {
                s.push_str(&format!(
                    "Projection({})\n",
                    expressions
                        .iter()
                        .map(|(e, _)| e.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
                s.push_str(&source.format(indent, false, true));
            }
            Self::Scan {
                table,
                alias,
                filter,
            } => {
                s.push_str(&format!("Scan({})", table));
                if let Some(alias) = alias {
                    s.push_str(&format!(" as {}", alias));
                }
                if let Some(filter) = filter {
                    s.push_str(&format!(" ({})", filter));
                }
                s.push('\n');
            }
            Self::Update {
                table,
                source,
                assignments,
            } => {
                s.push_str(&format!("Update({})", table));
                if !assignments.is_empty() {
                    s.push_str(&format!(
                        " ({})",
                        assignments
                            .iter()
                            .map(|(i, l, e)| format!(
                                "{} = {}",
                                l.clone().unwrap_or_else(|| format!("#{}", i)),
                                e
                            ))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                }
                s.push('\n');
                s.push_str(&source.format(indent, false, true));
            }
        }

        if root {
            s = s.trim_end().to_string();
        }
        s
    }
}

impl Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.format("".into(), true, true))
    }
}

/// 聚合操作
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Aggregate {
    Average,
    Count,
    Max,
    Min,
    Sum,
}

impl Display for Aggregate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Average => "AVG",
            Self::Count => "COUNT",
            Self::Max => "MAX",
            Self::Min => "MIN",
            Self::Sum => "SUM",
        };
        write!(f, "{}", s)
    }
}

pub type Aggregates = Vec<Aggregate>;

/// 排序方向
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Direction {
    Ascending,
    Descending,
}

impl Display for Direction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Ascending => "ASC",
            Self::Descending => "DESC",
        };
        write!(f, "{}", s)
    }
}

#[cfg(test)]
mod sqlparser_tests {
    use sqlparser::{dialect::GenericDialect, parser::Parser};

    use crate::error::Result;

    #[test]
    fn test_select() -> Result<()> {
        let stmt = Parser::new(&GenericDialect {})
            .try_with_sql("SELECT * FROM t1 ORDER BY a DESC")?
            .parse_statement()?;
        println!("{:#?}", stmt);

        Ok(())
    }
}
