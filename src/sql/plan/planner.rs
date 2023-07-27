use std::collections::{HashMap, HashSet};

use crate::{
    error::{Error, Result},
    sql::{
        schema::{Catalog, Table},
        types::{Expression, Value},
    },
};

use sqlparser::ast::{self, Query};

use super::{Node, Plan};

/// 计划器
pub struct Planner<'a, C: Catalog> {
    _catalog: &'a mut C,
}

impl<'a, C: Catalog> Planner<'a, C> {
    /// 创建一个计划器
    pub fn new(catalog: &'a mut C) -> Self {
        Self { _catalog: catalog }
    }

    /// 根据语句构建计划
    pub fn build(self, statement: ast::Statement) -> Result<Plan> {
        Ok(Plan(self.build_statement(statement)?))
    }

    /// 根据语句构建计划节点
    fn build_statement(&self, statement: ast::Statement) -> Result<Node> {
        Ok(match statement {
            ast::Statement::Query(query) => {
                let Query {
                    with,
                    body,
                    order_by,
                    limit,
                    offset,
                    fetch,
                    locks,
                } = *query.clone();

                // body 必须是一个 Select 语句
                let select = match *body {
                    ast::SetExpr::Select(select) => select,
                    _ => {
                        return Err(Error::Value(format!(
                            "Unsupported query: {}",
                            query.to_string()
                        )))
                    }
                };

                Node::Nothing
            }

            _ => unimplemented!(),
        })
    }

    fn build_from_clause(&self, scope: &mut Scope, from: Vec<ast::TableWithJoins>) -> Result<Node> {
        let base_scope = scope.clone();
        let mut items = from.into_iter();
        let mut node = match items.next() {
            Some(item) => self.build_from_item(scope, item)?,
            None => return Err(Error::Value("Empty FROM clause".into())),
        };
        for item in items {
            let mut right_scope = base_scope.clone();
            let right = self.build_from_item(&mut right_scope, item)?;
            node = Node::NestedLoopJoin {
                left: Box::new(node),
                left_size: scope.len(),
                right: Box::new(right),
                predicate: None,
                outer: false,
            };
            scope.merge(right_scope)?;
        }

        Ok(node)
    }

    fn build_from_item(&self, scope: &mut Scope, item: ast::TableWithJoins) -> Result<Node> {
        let (table, alias) = table_alias_fram_factor(item.relation)?;
        let mut node = Node::Scan {
            table,
            alias,
            filter: None,
        };

        for join in item.joins {
            let (table, alias) = table_alias_fram_factor(join.relation)?;
            let right = Node::Scan {
                table,
                alias,
                filter: None,
            };
            let predicate = match join.join_operator {
                ast::JoinOperator::Inner(ast::JoinConstraint::On(expr))
                | ast::JoinOperator::LeftOuter(ast::JoinConstraint::On(expr))
                | ast::JoinOperator::RightOuter(ast::JoinConstraint::On(expr))
                | ast::JoinOperator::FullOuter(ast::JoinConstraint::On(expr)) => {
                    Some(self.build_expression(scope, expr)?)
                }
                ast::JoinOperator::CrossJoin => None,
                j => return Err(Error::Value(format!("Unsupported join: {:?}", j))),
            };
            node = Node::NestedLoopJoin {
                left: Box::new(node),
                left_size: scope.len(),
                right: Box::new(right),
                predicate,
                outer: false,
            };
        }

        Ok(node)
    }

    fn build_expression(&self, scope: &mut Scope, expr: ast::Expr) -> Result<Expression> {
        use Expression::*;
        Ok(match expr {
            ast::Expr::Value(value) => Constant(match value {
                ast::Value::Null => Value::Null,
                ast::Value::Boolean(b) => Value::Boolean(b),
                ast::Value::Number(s, _) => {
                    if s.contains('.') {
                        Value::Float(s.parse::<f64>()?)
                    } else {
                        Value::Integer(s.parse::<i64>()?)
                    }
                },
                ast::Value::SingleQuotedString(s) => Value::String(s),
                t => return Err(Error::Value(format!("Unsupported value: {:?}", t))),
            }),
            ast::Expr::Identifier(ident) => {
                let index = scope.resolve(None, &ident.value)?;
                Field(index, scope.get_label(index)?)
            }
                
            _ => unimplemented!(),
        })
    }
}

fn table_alias_fram_factor(table_factor: ast::TableFactor) -> Result<(String, Option<String>)> {
    Ok(match table_factor {
        ast::TableFactor::Table {
            name, alias: None, ..
        } => (name.0[0].value.clone(), None),
        ast::TableFactor::Table {
            name,
            alias: Some(alias),
            ..
        } => (name.0[0].value.clone(), Some(alias.name.value)),
        _ => return Err(Error::Value("Unsupported table factor".into())),
    })
}

/// 管理可用于表达式和执行器的名称，并将它们映射到列/字段
#[derive(Debug, Clone)]
pub struct Scope {
    /// 如果为 true，则 `scope` 是不变的，不能包含任何变量。
    constant: bool,
    ///
    tables: HashMap<String, Table>,

    columns: Vec<(Option<String>, Option<String>)>,

    qualified: HashMap<(String, String), usize>,

    unqualified: HashMap<String, usize>,

    ambiguous: HashSet<String>,
}

impl Scope {
    fn new() -> Self {
        Self {
            constant: false,
            tables: HashMap::new(),
            columns: Vec::new(),
            qualified: HashMap::new(),
            unqualified: HashMap::new(),
            ambiguous: HashSet::new(),
        }
    }

    fn constant() -> Self {
        let mut scope = Self::new();
        scope.constant = true;
        scope
    }

    fn from_table(table: Table) -> Result<Self> {
        let mut scope = Self::new();
        scope.add_table(table.name.clone(), table)?;
        Ok(scope)
    }

    fn add_column(&mut self, table: Option<String>, label: Option<String>) {
        unimplemented!()
    }

    fn add_table(&mut self, label: String, table: Table) -> Result<()> {
        unimplemented!()
    }

    fn get_column(&self, index: usize) -> Result<(Option<String>, Option<String>)> {
        unimplemented!()
    }

    fn get_label(&self, index: usize) -> Result<Option<(Option<String>, String)>> {
        Ok(match self.get_column(index)? {
            (table, Some(name)) => Some((table, name)),
            _ => None,
        })
    }

    fn merge(&mut self, scope: Scope) -> Result<()> {
        unimplemented!()
    }

    fn resolve(&self, table: Option<&str>, name: &str) -> Result<usize> {
        unimplemented!()
    }

    /// 列数
    fn len(&self) -> usize {
        self.columns.len()
    }

    fn project(&mut self, projection: &[(Expression, Option<String>)]) -> Result<()> {
        unimplemented!()
    }
}
