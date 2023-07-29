use std::{
    collections::{HashMap, HashSet},
    default,
};

use crate::{
    error::{Error, Result},
    sql::{
        schema::{Catalog, Column, Table},
        types::{DataType, Expression, Value},
    },
};

use sqlparser::ast::{self, Query, Select};

use super::{Node, Plan};

/// 计划器
pub struct Planner<'a, C: Catalog> {
    catalog: &'a mut C,
}

impl<'a, C: Catalog> Planner<'a, C> {
    /// 创建一个计划器
    pub fn new(catalog: &'a mut C) -> Self {
        Self { catalog }
    }

    /// 根据语句构建计划
    pub fn build(self, statement: ast::Statement) -> Result<Plan> {
        Ok(Plan(self.build_statement(statement)?))
    }

    /// 根据语句构建计划节点
    fn build_statement(&self, statement: ast::Statement) -> Result<Node> {
        Ok(match statement {
            ast::Statement::CreateTable { name, columns, .. } => Node::CreateTable {
                schema: Table::new(
                    name.0[0].value.clone(),
                    columns
                        .into_iter()
                        .map(|c| self.build_column(c))
                        .collect::<Result<_>>()?,
                )?,
            },
            ast::Statement::Drop {
                object_type, names, ..
            } => match object_type {
                ast::ObjectType::Table => {
                    if names.len() != 1 {
                        return Err(Error::Value(format!(
                            "Unsupported drop multiple table once: {:?}",
                            names
                        )));
                    }
                    Node::DropTable {
                        table: names[0].0[0].value.clone(),
                    }
                }
                ot => return Err(Error::Value(format!("Unsupported drop: {:?}", ot))),
            },

            ast::Statement::Delete {
                from,
                selection,
                using: None,
                returning: None,
                ..
            } => {
                if from.len() != 1 {
                    return Err(Error::Value(format!(
                        "Unsupported delete from multiple table: {:?}",
                        from
                    )));
                }
                let table = table_alias_from_factor(from[0].clone().relation)?.0;
                let ctx = &mut Context::from_table(self.catalog.must_read_table(&table)?)?;

                Node::Delete {
                    table: table.clone(),
                    source: Box::new(Node::Scan {
                        table,
                        alias: None,
                        filter: selection
                            .map(|expr| self.build_expression(ctx, expr))
                            .transpose()?,
                    }),
                }
            }
            ast::Statement::Insert {
                table_name,
                columns,
                source,
                on: None,
                returning: None,
                ..
            } => {
                let table = table_name.0[0].value.clone();
                let columns = columns
                    .into_iter()
                    .map(|ident| ident.value)
                    .collect::<Vec<_>>();
                let expressions = match *source.body {
                    ast::SetExpr::Values(values) => {
                        let mut expressions = Vec::new();
                        for row in values.rows {
                            let mut row_expressions = Vec::new();
                            for expr in row {
                                row_expressions
                                    .push(self.build_expression(&mut Context::constant(), expr)?);
                            }
                            expressions.push(row_expressions);
                        }
                        expressions
                    }
                    s => return Err(Error::Value(format!("Unsupported source: {:?}", s))),
                };
                Node::Insert {
                    table,
                    columns,
                    expressions,
                }
            }
            ast::Statement::Query(query) => {
                let Query {
                    body,
                    limit,
                    offset,
                    ..
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
                let Select {
                    distinct,
                    projection,
                    from,
                    selection,
                    group_by,
                    having,
                    ..
                } = *select.clone();

                let ctx = &mut Context::new();

                // 处理 FROM 子句
                let mut node = self.build_from_clause(ctx, from)?;

                // 处理 WHERE 子句
                if let Some(expr) = selection {
                    node = Node::Filter {
                        source: Box::new(node),
                        predicate: self.build_expression(ctx, expr)?,
                    };
                };

                // 处理 SELECT 子句
                if !projection.is_empty() {}

                unimplemented!()
            }

            stmt => return Err(Error::Value(format!("Unsupported statement: {:?}", stmt))),
        })
    }

    fn build_from_clause(&self, ctx: &mut Context, from: Vec<ast::TableWithJoins>) -> Result<Node> {
        if from.len() > 1 {
            return Err(Error::Value("Unsupported multiple JOIN".into()));
        }

        // let base_ctx = ctx.clone();
        let mut items = from.into_iter();
        let node = match items.next() {
            Some(item) => self.build_from_item(ctx, item)?,
            None => return Err(Error::Value("Empty FROM clause".into())),
        };
        // for item in items {
        //     let mut right_ctx = base_ctx.clone();
        //     let right = self.build_from_item(&mut right_ctx, item)?;
        //     node = Node::NestedLoopJoin {
        //         left: Box::new(node),
        //         left_size: ctx.len(),
        //         right: Box::new(right),
        //         predicate: None,
        //         outer: false,
        //     };
        //     ctx.merge(right_ctx)?;
        // }

        Ok(node)
    }

    fn build_from_item(&self, ctx: &mut Context, item: ast::TableWithJoins) -> Result<Node> {
        let (name, alias) = table_alias_from_factor(item.relation)?;
        let table = self.catalog.must_read_table(&name)?;
        ctx.add_table(alias.clone().unwrap_or_else(|| name.clone()), table)?;
        let mut node = Node::Scan {
            table: name,
            alias,
            filter: None,
        };

        for join in item.joins {
            let (table, alias) = table_alias_from_factor(join.relation)?;
            let right = Node::Scan {
                table,
                alias,
                filter: None,
            };
            let predicate = match join.join_operator {
                ast::JoinOperator::Inner(ast::JoinConstraint::On(expr))
                | ast::JoinOperator::LeftOuter(ast::JoinConstraint::On(expr))
                // | ast::JoinOperator::RightOuter(ast::JoinConstraint::On(expr))
                | ast::JoinOperator::FullOuter(ast::JoinConstraint::On(expr)) => {
                    Some(self.build_expression(ctx, expr)?)
                }
                ast::JoinOperator::CrossJoin => None,
                j => return Err(Error::Value(format!("Unsupported join: {:?}", j))),
            };
            node = Node::NestedLoopJoin {
                left: Box::new(node),
                left_size: ctx.len(),
                right: Box::new(right),
                predicate,
                outer: false,
            };
        }

        Ok(node)
    }

    fn build_expression(&self, ctx: &mut Context, expr: ast::Expr) -> Result<Expression> {
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
                }
                ast::Value::SingleQuotedString(s) => Value::String(s),
                t => return Err(Error::Value(format!("Unsupported value: {:?}", t))),
            }),
            ast::Expr::Identifier(ident) => {
                let index = ctx.resolve(None, &ident.value)?;
                let label = ctx.get_label(index)?;
                Field(index, label)
            }
            ast::Expr::CompoundIdentifier(idents) => {
                if idents.len() != 2 {
                    return Err(Error::Value(format!(
                        "Unsupported compound identifier: {:?}",
                        idents
                    )));
                }
                let index = ctx.resolve(
                    Some(idents.get(0).unwrap().value.as_str()),
                    idents.get(1).unwrap().value.as_str(),
                )?;
                let label = ctx.get_label(index)?;
                Field(index, label)
            }
            ast::Expr::Function(_func) => {
                return Err(Error::Value("Unsupported function".into()));
            }
            ast::Expr::BinaryOp { left, op, right } => match op {
                ast::BinaryOperator::And => And(
                    self.build_expression(ctx, *left)?.into(),
                    self.build_expression(ctx, *right)?.into(),
                ),
                ast::BinaryOperator::Or => Or(
                    self.build_expression(ctx, *left)?.into(),
                    self.build_expression(ctx, *right)?.into(),
                ),

                ast::BinaryOperator::Eq => Equal(
                    self.build_expression(ctx, *left)?.into(),
                    self.build_expression(ctx, *right)?.into(),
                ),
                ast::BinaryOperator::NotEq => Not(Equal(
                    self.build_expression(ctx, *left)?.into(),
                    self.build_expression(ctx, *right)?.into(),
                )
                .into()),
                ast::BinaryOperator::Gt => GreaterThan(
                    self.build_expression(ctx, *left)?.into(),
                    self.build_expression(ctx, *right)?.into(),
                ),
                ast::BinaryOperator::GtEq => And(
                    GreaterThan(
                        self.build_expression(ctx, *left.clone())?.into(),
                        self.build_expression(ctx, *right.clone())?.into(),
                    )
                    .into(),
                    Equal(
                        self.build_expression(ctx, *left)?.into(),
                        self.build_expression(ctx, *right)?.into(),
                    )
                    .into(),
                ),
                ast::BinaryOperator::Lt => LessThan(
                    self.build_expression(ctx, *left)?.into(),
                    self.build_expression(ctx, *right)?.into(),
                ),
                ast::BinaryOperator::LtEq => And(
                    LessThan(
                        self.build_expression(ctx, *left.clone())?.into(),
                        self.build_expression(ctx, *right.clone())?.into(),
                    )
                    .into(),
                    Equal(
                        self.build_expression(ctx, *left)?.into(),
                        self.build_expression(ctx, *right)?.into(),
                    )
                    .into(),
                ),

                ast::BinaryOperator::Plus => Plus(
                    self.build_expression(ctx, *left)?.into(),
                    self.build_expression(ctx, *right)?.into(),
                ),
                ast::BinaryOperator::Minus => Minus(
                    self.build_expression(ctx, *left)?.into(),
                    self.build_expression(ctx, *right)?.into(),
                ),
                ast::BinaryOperator::Multiply => Multiply(
                    self.build_expression(ctx, *left)?.into(),
                    self.build_expression(ctx, *right)?.into(),
                ),
                ast::BinaryOperator::Divide => Divide(
                    self.build_expression(ctx, *left)?.into(),
                    self.build_expression(ctx, *right)?.into(),
                ),
                ast::BinaryOperator::Modulo => Modulo(
                    self.build_expression(ctx, *left)?.into(),
                    self.build_expression(ctx, *right)?.into(),
                ),

                _ => {
                    return Err(Error::Value(format!(
                        "Unsupported binary operator: {:?}",
                        op
                    )))
                }
            },
            ast::Expr::IsNull(expr) => IsNull(self.build_expression(ctx, *expr)?.into()),
            ast::Expr::IsNotNull(expr) => {
                Not(IsNull(self.build_expression(ctx, *expr)?.into()).into())
            }
            // TODO like
            e => return Err(Error::Value(format!("Unsupported expression: {:?}", e))),
        })
    }

    fn build_column(&self, c: ast::ColumnDef) -> Result<Column> {
        let mut primary_key = false;
        let mut unique = false;
        let mut nullable = true;
        let mut default: Option<Value> = None;
        let mut references: Option<String> = None;
        for o in &c.options {
            match o.option.clone() {
                ast::ColumnOption::Unique { is_primary } => {
                    primary_key = is_primary || primary_key;
                    unique = true;
                }
                ast::ColumnOption::Default(expr) => {
                    default = Some(
                        self.build_expression(&mut Context::constant(), expr)?
                            .evaluate(None)?,
                    );
                }
                ast::ColumnOption::ForeignKey {
                    foreign_table,
                    referred_columns,
                    ..
                } => {
                    if !referred_columns.is_empty() {
                        return Err(Error::Value("Unsupported foreign key".into()));
                    }
                    references = Some(foreign_table.0[0].value.clone());
                }
                ast::ColumnOption::Null => {
                    if primary_key {
                        return Err(Error::Value("Primary key must be not null".into()));
                    }
                    nullable = true
                }
                ast::ColumnOption::NotNull => nullable = false,
                opt => return Err(Error::Value(format!("Unsupported option: {:?}", opt))),
            }
        }
        Ok(Column {
            name: c.name.value,
            data_type: data_type(c.data_type)?,
            primary_key,
            nullable,
            default,
            indexed: false,
            unique,
            references,
        })
    }
}

/// 管理可用于表达式和执行器的名称，并将它们映射到列/字段
#[derive(Debug, Clone)]
pub struct Context {
    /// 如果为 true，则 `context` 是不变的，不能包含任何变量。
    constant: bool,
    ///
    tables: HashMap<String, Table>,

    columns: Vec<(Option<String>, Option<String>)>,

    qualified: HashMap<(String, String), usize>,

    unqualified: HashMap<String, usize>,

    ambiguous: HashSet<String>,
}

impl Context {
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
        let mut ctx = Self::new();
        ctx.constant = true;
        ctx
    }

    fn from_table(table: Table) -> Result<Self> {
        let mut ctx = Self::new();
        ctx.add_table(table.name.clone(), table)?;
        Ok(ctx)
    }

    fn add_column(&mut self, table: Option<String>, label: Option<String>) {
        if let Some(l) = label.clone() {
            if let Some(t) = table.clone() {
                self.qualified.insert((t, l.clone()), self.columns.len());
            }
            if !self.ambiguous.contains(&l) {
                if !self.unqualified.contains_key(&l) {
                    self.unqualified.insert(l, self.columns.len());
                } else {
                    self.unqualified.remove(&l);
                    self.ambiguous.insert(l);
                }
            }
        }
        self.columns.push((table, label));
    }

    fn add_table(&mut self, label: String, table: Table) -> Result<()> {
        if self.constant {
            return Err(Error::Internal("Can't modify constant context".into()));
        }
        if self.tables.contains_key(&label) {
            return Err(Error::Value(format!("Duplicate table name '{}'", label)));
        }
        for column in &table.columns {
            self.add_column(Some(label.clone()), Some(column.name.clone()));
        }
        self.tables.insert(label, table);
        Ok(())
    }

    fn get_column(&self, index: usize) -> Result<(Option<String>, Option<String>)> {
        if self.constant {
            return Err(Error::Value(format!(
                "Expression must be constant, found column '{}'",
                index
            )));
        }
        self.columns
            .get(index)
            .cloned()
            .ok_or_else(|| Error::Value(format!("No column '{}'", index)))
    }

    fn get_label(&self, index: usize) -> Result<Option<(Option<String>, String)>> {
        Ok(match self.get_column(index)? {
            (table, Some(name)) => Some((table, name)),
            _ => None,
        })
    }

    fn merge(&mut self, ctx: Context) -> Result<()> {
        if self.constant {
            return Err(Error::Internal("Can't modify constant context".into()));
        }
        for (label, table) in ctx.tables {
            if self.tables.contains_key(&label) {
                return Err(Error::Value(format!("Duplicate table name '{}'", label)));
            }
            self.tables.insert(label, table);
        }
        for (table, label) in ctx.columns {
            self.add_column(table, label);
        }
        Ok(())
    }

    fn resolve(&self, table: Option<&str>, name: &str) -> Result<usize> {
        if self.constant {
            return Err(Error::Value(format!(
                "Expression must be constant, found field'{}'",
                if let Some(table) = table {
                    format!("{}.{}", table, name)
                } else {
                    name.into()
                }
            )));
        }
        if let Some(table) = table {
            if !self.tables.contains_key(table) {
                return Err(Error::Value(format!("Unknown table '{}'", table)));
            }
            self.qualified
                .get(&(table.into(), name.into()))
                .copied()
                .ok_or_else(|| Error::Value(format!("Unknown field '{}.{}'", table, name)))
        } else if self.ambiguous.contains(name) {
            Err(Error::Value(format!("Ambiguous field '{}'", name)))
        } else {
            self.unqualified
                .get(name)
                .copied()
                .ok_or_else(|| Error::Value(format!("Unknown field '{}'", name)))
        }
    }

    /// 列数
    fn len(&self) -> usize {
        self.columns.len()
    }

    fn project(&mut self, projection: &[(Expression, Option<String>)]) -> Result<()> {
        if self.constant {
            return Err(Error::Internal("Can't modify constant context".into()));
        }
        let mut new_ctx = Self::new();
        new_ctx.tables = self.tables.clone();
        for (expr, lable) in projection {
            match (expr, lable) {
                (_, Some(lable)) => new_ctx.add_column(None, Some(lable.clone())),
                (Expression::Field(_, Some((Some(table), name))), _) => {
                    new_ctx.add_column(Some(table.clone()), Some(name.clone()))
                }
                (Expression::Field(_, Some((None, name))), _) => {
                    if let Some(i) = self.unqualified.get(name) {
                        let (table, name) = self.columns[*i].clone();
                        new_ctx.add_column(table, name);
                    }
                }
                (Expression::Field(i, None), _) => {
                    let (table, label) = self.columns.get(*i).cloned().unwrap_or((None, None));
                    new_ctx.add_column(table, label);
                }

                _ => new_ctx.add_column(None, None),
            }
        }
        *self = new_ctx;
        Ok(())
    }
}

fn table_alias_from_factor(table_factor: ast::TableFactor) -> Result<(String, Option<String>)> {
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

fn data_type(t: ast::DataType) -> Result<DataType> {
    match t {
        ast::DataType::Boolean => Ok(DataType::Boolean),
        ast::DataType::Int(_) => Ok(DataType::Integer),
        ast::DataType::Integer(_) => Ok(DataType::Integer),
        ast::DataType::Float(_) => Ok(DataType::Float),
        ast::DataType::String => Ok(DataType::String),
        ast::DataType::Varchar(None) => Ok(DataType::String),

        t => Err(Error::Value(format!("Unsupported data type: {:?}", t))),
    }
}
