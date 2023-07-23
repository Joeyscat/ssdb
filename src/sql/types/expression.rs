use regex::Regex;
use serde_derive::{Deserialize, Serialize};

use crate::error::{Error, Result};

use super::{Row, Value};

/// An expression, made up of constants and operations
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    // Values
    Constant(Value),
    Field(usize, Option<(Option<String>, String)>),

    // Logical operations
    And(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),
    Or(Box<Expression>, Box<Expression>),

    // Comparison operations (EQ, GT, LT)
    Equal(Box<Expression>, Box<Expression>),
    GreaterThan(Box<Expression>, Box<Expression>),
    IsNull(Box<Expression>),
    LessThan(Box<Expression>, Box<Expression>),

    // Math operations
    Plus(Box<Expression>, Box<Expression>),
    Minus(Box<Expression>, Box<Expression>),
    Multiply(Box<Expression>, Box<Expression>),
    Divide(Box<Expression>, Box<Expression>),
    Modulo(Box<Expression>, Box<Expression>),
    Negate(Box<Expression>),
    Exponentiate(Box<Expression>, Box<Expression>),
    Factorial(Box<Expression>),
    Assert(Box<Expression>),

    // String operations
    Like(Box<Expression>, Box<Expression>),
}

impl Expression {
    /// Evaluate the expression, returning a value
    pub fn evaluate(&self, row: Option<&Row>) -> Result<Value> {
        use Value::*;
        Ok(match self {
            // Constant values
            Self::Constant(value) => value.clone(),
            Self::Field(index, _) => row.and_then(|row| row.get(*index).cloned()).unwrap_or(Null),

            // Logical operations
            Self::And(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Boolean(lhs), Boolean(rhs)) => Boolean(lhs && rhs),
                (Boolean(lhs), Null) if !lhs => Boolean(false),
                (Boolean(_), Null) => Null,
                (Null, Boolean(rhs)) if !rhs => Boolean(false),
                (Null, Boolean(_)) => Null,
                (Null, Null) => Null,

                (lhs, rhs) => return Err(Error::Value(format!("{} AND {}", lhs, rhs))),
            },
            Self::Not(expr) => match expr.evaluate(row)? {
                Boolean(v) => Boolean(!v),
                Null => Null,
                v => return Err(Error::Value(format!("NOT {}", v))),
            },
            Self::Or(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Boolean(lhs), Boolean(rhs)) => Boolean(lhs || rhs),
                (Boolean(lhs), Null) if lhs => Boolean(true),
                (Boolean(_), Null) => Null,
                (Null, Boolean(rhs)) if rhs => Boolean(true),
                (Null, Boolean(_)) => Null,
                (Null, Null) => Null,
                (lhs, rhs) => return Err(Error::Value(format!("{} OR {}", lhs, rhs))),
            },

            // Comparison operations
            #[allow(clippy::float_cmp)]
            Self::Equal(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Boolean(lhs), Boolean(rhs)) => Boolean(lhs == rhs),
                (Integer(lhs), Integer(rhs)) => Boolean(lhs == rhs),
                (Integer(lhs), Float(rhs)) => Boolean((lhs as f64) == rhs),
                (Float(lhs), Float(rhs)) => Boolean(lhs == rhs),
                (Float(lhs), Integer(rhs)) => Boolean(lhs == (rhs as f64)),
                (String(lhs), String(rhs)) => Boolean(lhs == rhs),
                (Null, _) | (_, Null) => Null,
                (lhs, rhs) => Err(Error::Value(format!("Compare {} and {}", lhs, rhs))),
            },
            Self::GreaterThan(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                #[allow(clippy::bool_comparison)]
                (Boolean(lhs), Boolean(rhs)) => Boolean(lhs > rhs),
                (Integer(lhs), Integer(rhs)) => Boolean(lhs > rhs),
                (Integer(lhs), Float(rhs)) => Boolean((lhs as f64) > rhs),
                (Float(lhs), Float(rhs)) => Boolean(lhs > rhs),
                (Float(lhs), Integer(rhs)) => Boolean(lhs > (rhs as f64)),
                (String(lhs), String(rhs)) => Boolean(lhs > rhs),
                (Null, _) | (_, Null) => Null,
                (lhs, rhs) => Err(Error::Value(format!("Compare {} and {}", lhs, rhs))),
            },
            Self::IsNull(expr) => match expr.evaluate(row)? {
                Null => Boolean(true),
                _ => Boolean(false),
            },
            Self::LessThan(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                #[allow(clippy::bool_comparison)]
                (Boolean(lhs), Boolean(rhs)) => Boolean(lhs < rhs),
                (Integer(lhs), Integer(rhs)) => Boolean(lhs < rhs),
                (Integer(lhs), Float(rhs)) => Boolean((lhs as f64) < rhs),
                (Float(lhs), Float(rhs)) => Boolean(lhs < rhs),
                (Float(lhs), Integer(rhs)) => Boolean(lhs < (rhs as f64)),
                (String(lhs), String(rhs)) => Boolean(lhs < rhs),
                (Null, _) | (_, Null) => Null,
                (lhs, rhs) => Err(Error::Value(format!("Compare {} and {}", lhs, rhs))),
            },

            // Math operations
            Self::Plus(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Integer(lhs), Integer(rhs)) => {
                    Integer(lhs.checked_add(rhs).ok_or_else(|| {
                        Error::Value(format!("Integer overflow: {} + {}", lhs, rhs))
                    })?)
                }
                (Integer(lhs), Float(rhs)) => Float((lhs as f64) + rhs),
                (Integer(_), Null) => Null,
                (Float(lhs), Float(rhs)) => Float(lhs + rhs),
                (Float(lhs), Integer(rhs)) => Float(lhs + (rhs as f64)),
                (Float(_), Null) => Null,
                (Null, Float(_)) => Null,
                (Null, Integer(_)) => Null,
                (Null, Null) => Null,
                (lhs, rhs) => return Err(Error::Value(format!("Plus {} and {}", lhs, rhs))),
            },
            Self::Assert(expr) => match expr.evaluate(row)? {
                Float(f) => Float(f),
                Integer(i) => Integer(i),
                Null => Null,
                expr => return Err(Error::Value(format!("Assert {}", expr))),
            },
            Self::Divide(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Integer(lhs), Integer(rhs)) => {
                    if rhs == 0 {
                        return Err(Error::Value(format!("Divide by zero: {} / {}", lhs, rhs)));
                    }
                    Integer(lhs / rhs)
                }
                (Integer(lhs), Float(rhs)) => {
                    if rhs == 0.0 {
                        return Err(Error::Value(format!("Divide by zero: {} / {}", lhs, rhs)));
                    }
                    Float((lhs as f64) / rhs)
                }
                (Integer(_), Null) => Null,
                (Float(lhs), Float(rhs)) => {
                    if rhs == 0.0 {
                        return Err(Error::Value(format!("Divide by zero: {} / {}", lhs, rhs)));
                    }
                    Float(lhs / rhs)
                }
                (Float(lhs), Integer(rhs)) => {
                    if rhs == 0 {
                        return Err(Error::Value(format!("Divide by zero: {} / {}", lhs, rhs)));
                    }
                    Float(lhs / (rhs as f64))
                }
                (Float(_), Null) => Null,
                (Null, Float(_)) => Null,
                (Null, Integer(_)) => Null,
                (Null, Null) => Null,
                (lhs, rhs) => return Err(Error::Value(format!("Divide {} and {}", lhs, rhs))),
            },
            Self::Exponentiate(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Integer(lhs), Integer(rhs)) if rhs >= 0 => Integer(
                    lhs.checked_pow(rhs as u32)
                        .ok_or_else(|| Error::Value("Integer overflow".into()))?,
                ),
                (Integer(lhs), Integer(rhs)) => Float((lhs as f64).powf(rhs as f64)),
                (Integer(lhs), Float(rhs)) => Float((lhs as f64).powf(rhs)),
                (Integer(_), Null) => Null,
                (Float(lhs), Float(rhs)) => Float(lhs.powf(rhs)),
                (Float(lhs), Integer(rhs)) => Float(lhs.powf(rhs as f64)),
                (Float(_), Null) => Null,
                (Null, Float(_)) => Null,
                (Null, Integer(_)) => Null,
                (Null, Null) => Null,
                (lhs, rhs) => {
                    return Err(Error::Value(format!("Exponentiate {} and {}", lhs, rhs)))
                }
            },
            Self::Factorial(expr) => match expr.evaluate(row)? {
                Integer(n) if n >= 0 => {
                    let mut result = 1;
                    for i in 1..=n {
                        result = result
                            .checked_mul(i)
                            .ok_or_else(|| Error::Value("Integer overflow".into()))?;
                    }
                    Integer(result)
                }
                Integer(_) => return Err(Error::Value("Factorial of negative number".into())),
                Null => Null,
                expr => return Err(Error::Value(format!("Factorial {}", expr))),
            },
            Self::Modulo(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Integer(lhs), Integer(rhs)) => {
                    if rhs == 0 {
                        return Err(Error::Value(format!("Modulo by zero: {} % {}", lhs, rhs)));
                    }
                    Integer(lhs % rhs)
                }
                (Integer(lhs), Float(rhs)) => {
                    if rhs == 0.0 {
                        return Err(Error::Value(format!("Modulo by zero: {} % {}", lhs, rhs)));
                    }
                    Float((lhs as f64) % rhs)
                }
                (Integer(_), Null) => Null,
                (Float(lhs), Float(rhs)) => {
                    if rhs == 0.0 {
                        return Err(Error::Value(format!("Modulo by zero: {} % {}", lhs, rhs)));
                    }
                    Float(lhs % rhs)
                }
                (Float(lhs), Integer(rhs)) => {
                    if rhs == 0 {
                        return Err(Error::Value(format!("Modulo by zero: {} % {}", lhs, rhs)));
                    }
                    Float(lhs % (rhs as f64))
                }
                (Float(_), Null) => Null,
                (Null, Float(_)) => Null,
                (Null, Integer(_)) => Null,
                (Null, Null) => Null,
                (lhs, rhs) => return Err(Error::Value(format!("Modulo {} and {}", lhs, rhs))),
            },
            Self::Multiply(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Integer(lhs), Integer(rhs)) => {
                    Integer(lhs.checked_mul(rhs).ok_or_else(|| {
                        Error::Value(format!("Integer overflow: {} * {}", lhs, rhs))
                    })?)
                }
                (Integer(lhs), Float(rhs)) => Float((lhs as f64) * rhs),
                (Integer(_), Null) => Null,
                (Float(lhs), Float(rhs)) => Float(lhs * rhs),
                (Float(lhs), Integer(rhs)) => Float(lhs * (rhs as f64)),
                (Float(_), Null) => Null,
                (Null, Float(_)) => Null,
                (Null, Integer(_)) => Null,
                (Null, Null) => Null,
                (lhs, rhs) => return Err(Error::Value(format!("Multiply {} and {}", lhs, rhs))),
            },
            Self::Negate(expr) => match expr.evaluate(row)? {
                Integer(i) => Integer(-i),
                Float(f) => Float(-f),
                Null => Null,
                expr => return Err(Error::Value(format!("Negate {}", expr))),
            },
            Self::Minus(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Integer(lhs), Integer(rhs)) => {
                    Integer(lhs.checked_sub(rhs).ok_or_else(|| {
                        Error::Value(format!("Integer overflow: {} - {}", lhs, rhs))
                    })?)
                }
                (Integer(lhs), Float(rhs)) => Float((lhs as f64) - rhs),
                (Integer(_), Null) => Null,
                (Float(lhs), Float(rhs)) => Float(lhs - rhs),
                (Float(lhs), Integer(rhs)) => Float(lhs - (rhs as f64)),
                (Float(_), Null) => Null,
                (Null, Float(_)) => Null,
                (Null, Integer(_)) => Null,
                (Null, Null) => Null,
                (lhs, rhs) => return Err(Error::Value(format!("Subtract {} and {}", lhs, rhs))),
            },

            // String operations
            Self::Like(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (String(lhs), String(rhs)) => Boolean(
                    Regex::new(&format!(
                        "^{}$",
                        regex::escape(&rhs)
                            .replace("%", ".*")
                            .replace(".*.*", "%")
                            .replace("_", ".")
                            .replace("..", "_")
                    ))?
                    .is_match(&lhs),
                ),
                (Null, String(_)) | (String(_), Null) => Null,
                (lhs, rhs) => return Err(Error::Value(format!("Like {} and {}", lhs, rhs))),
            },
        })
    }

    /// Walk the expression tree, calling the visitor function for each node.
    /// If the visitor returns true, the walk will stop.
    /// This is the inverse of walk()
    pub fn contains<F: Fn(&Expression) -> bool>(&self, visitor: &F) -> bool {
        !self.walk(&|e| !visitor(e))
    }

    /// Replaces the expression with the result of the closure. Helper function for transform().
    fn replace_with<F: Fn(Self) -> Result<Self>>(&mut self, f: F) -> Result<()> {
        // Temporary replace expression with null, in case closure panics, May consider
        // replace_with crate if this hampers performance
        let expr = std::mem::replace(self, Expression::Constant(Value::Null));
        *self = f(expr)?;
        Ok(())
    }

    /// Transform the expression tree by applying a closure before and after descending.
    pub fn transform<B, A>(mut self, before: &B, after: &A) -> Result<Self>
    where
        B: Fn(Self) -> Result<Self>,
        A: Fn(Self) -> Result<Self>,
    {
        self = before(self)?;
        match &mut self {
            Self::Plus(lhs, rhs)
            | Self::And(lhs, rhs)
            | Self::Divide(lhs, rhs)
            | Self::Equal(lhs, rhs)
            | Self::Exponentiate(lhs, rhs)
            | Self::GreaterThan(lhs, rhs)
            | Self::LessThan(lhs, rhs)
            | Self::Like(lhs, rhs)
            | Self::Modulo(lhs, rhs)
            | Self::Multiply(lhs, rhs)
            | Self::Or(lhs, rhs)
            | Self::Minus(lhs, rhs) => {
                Self::replace_with(lhs, |e| e.transform(before, after))?;
                Self::replace_with(rhs, |e| e.transform(before, after))?;
            }

            Self::Assert(expr)
            | Self::Factorial(expr)
            | Self::IsNull(expr)
            | Self::Negate(expr)
            | Self::Not(expr) => Self::replace_with(expr, |e| e.transform(before, after))?,

            Self::Constant(_) | Self::Field(_, _) => {}
        };

        after(self)
    }

    /// Walk the expression tree, calling the visitor function for each node. Halts if the visitor
    /// returns false.
    pub fn walk<F: Fn(&Expression) -> bool>(&self, visitor: &F) -> bool {
        visitor(self)
            && match self {
                Self::Plus(lhs, rhs)
                | Self::And(lhs, rhs)
                | Self::Divide(lhs, rhs)
                | Self::Equal(lhs, rhs)
                | Self::Exponentiate(lhs, rhs)
                | Self::GreaterThan(lhs, rhs)
                | Self::LessThan(lhs, rhs)
                | Self::Like(lhs, rhs)
                | Self::Modulo(lhs, rhs)
                | Self::Multiply(lhs, rhs)
                | Self::Or(lhs, rhs)
                | Self::Minus(lhs, rhs) => lhs.walk(visitor) && rhs.walk(visitor),

                Self::Assert(expr)
                | Self::Factorial(expr)
                | Self::IsNull(expr)
                | Self::Negate(expr)
                | Self::Not(expr) => expr.walk(visitor),

                Self::Constant(_) | Self::Field(_, _) => true,
            }
    }

    /// Converts the expression into its negation normal form. This pushes NOT operators info
    /// the tree using De Morgan's laws, such that they never occur before other logical operators.
    pub fn into_nnf(self) -> Self {
        use Expression::*;

        // FIXME This should use a single match, but it's not possible to match on the boxed
        // children without box pattern syntax, which is unstable.
        self.transform(
            &|e| match e {
                Not(expr) => match *expr {
                    And(lhs, rhs) => Ok(Or(Not(lhs).into(), Not(rhs).into())),
                    Or(lhs, rhs) => Ok(And(Not(lhs).into(), Not(rhs).into())),
                    Not(expr) => Ok(*expr),
                    _ => Ok(Not(expr)),
                },
                _ => Ok(e),
            },
            &|e| Ok(e),
        )
        .unwrap()
    }

    /// Converts the expression into conjunctive normal form. i.e. an AND of ORs. This is done by
    /// converting to negation normal form, then applying the distributive law:
    /// (x AND y) OR z = (x OR z) AND (y OR z).
    pub fn into_cnf(self) -> Self {
        use Expression::*;
        self.into_nnf()
            .transform(
                &|e| match e {
                    Or(lhs, rhs) => match (*lhs, *rhs) {
                        (And(ll, lr), r) => {
                            Ok(And(Or(ll, r.clone().into()), Or(lr, r.into()).into()))
                        }
                        (l, And(rl, rr)) => Ok(And(
                            Or(l.clone().into(), rl).into(),
                            Or(l.into(), rr).into(),
                        )),
                        (lhs, rhs) => Ok(Or(lhs.into(), rhs.into())),
                    },
                    e => Ok(e),
                },
                &|e| Ok(e),
            )
            .unwrap()
    }

    /// Convert the expression into conjunctive normal form as a vector.
    pub fn into_cnf_vec(self) -> Vec<Self> {
        let mut cnf = Vec::new();
        let mut stack = vec![self.into_cnf()];
        while let Some(expr) = stack.pop() {
            match expr {
                Self::And(lhs, rhs) => {
                    stack.push(*rhs);
                    stack.push(*lhs);
                }
                expr => cnf.push(expr),
            }
        }
        cnf
    }

    /// Convert the expression into disjunctive normal form. i.e. an OR of ANDs. This is done by
    /// converting to negation normal form, then applying the distributive law:
    /// (x OR y) AND z = (x AND z) OR (y AND z).
    pub fn into_dnf(self) -> Self {
        use Expression::*;
        self.into_nnf()
            .transform(
                &|e| match e {
                    And(lhs, rhs) => match (*lhs, *rhs) {
                        (Or(ll, lr), r) => {
                            Ok(Or(And(ll, r.clone().into()), And(lr, r.into()).into()))
                        }
                        (l, Or(rl, rr)) => Ok(Or(
                            And(l.clone().into(), rl).into(),
                            And(l.into(), rr).into(),
                        )),
                        (lhs, rhs) => Ok(And(lhs.into(), rhs.into())),
                    },
                    e => Ok(e),
                },
                &|e| Ok(e),
            )
            .unwrap()
    }

    /// Convert the expression into disjunctive normal form as a vector.
    pub fn into_dnf_vec(self) -> Vec<Self> {
        let mut dnf = Vec::new();
        let mut stack = vec![self.into_dnf()];
        while let Some(expr) = stack.pop() {
            match expr {
                Self::Or(lhs, rhs) => {
                    stack.push(*rhs);
                    stack.push(*lhs);
                }
                expr => dnf.push(expr),
            }
        }
        dnf
    }

    /// Create an expression by joining a vector in conjunctive normal form as an AND.
    pub fn from_cnf_vec(mut cnf: Vec<Self>) -> Option<Self> {
        if cnf.is_empty() {
            return None;
        }
        let mut expr = cnf.pop().unwrap();
        while let Some(next) = cnf.pop() {
            expr = Self::And(next.into(), expr.into());
        }
        Some(expr)
    }

    /// Create an expression by joining a vector in disjunctive normal form as an OR.
    pub fn from_dnf_vec(mut dnf: Vec<Self>) -> Option<Self> {
        if dnf.is_empty() {
            return None;
        }
        let mut expr = dnf.pop().unwrap();
        while let Some(next) = dnf.pop() {
            expr = Self::Or(next.into(), expr.into());
        }
        Some(expr)
    }

    /// Check if the expression is a field lookup, and return the list of values looked up.
    /// Expression must be a combination of =, IS NULL, OR to be converted.
    pub fn as_lookup(&self, field: usize) -> Option<Vec<Value>> {
        use Expression::*;

        // FIXME This should use a single match level, but it's not possible to match on the boxed
        // children without box pattern syntax, which is unstable.
        match &*self {
            Equal(lhs, rhs) => match (&**lhs, &**rhs) {
                (Field(index, _), Constant(value)) if index == &field => Some(vec![value.clone()]),
                (Constant(value), Field(index, _)) if index == &field => Some(vec![value.clone()]),
                _ => None,
            },
            IsNull(expr) => match &**expr {
                Field(index, _) if index == &field => Some(vec![Value::Null]),
                _ => None,
            },
            Or(lhs, rhs) => match (lhs.as_lookup(field), rhs.as_lookup(field)) {
                (Some(mut lvalues), Some(mut rvalues)) => {
                    lvalues.append(&mut rvalues);
                    Some(lvalues)
                }
                _ => None,
            },
            _ => None,
        }
    }

    /// Create an expression from a list of field lookup values.
    pub fn from_lookup(
        field: usize,
        label: Option<(Option<String>, String)>,
        values: Vec<Value>,
    ) -> Self {
        if values.is_empty() {
            return Self::Equal(
                Self::Field(field, label).into(),
                Self::Constant(Value::Null).into(),
            );
        }
        Self::from_dnf_vec(
            values
                .into_iter()
                .map(|v| {
                    Self::Equal(
                        Self::Field(field, label.clone()).into(),
                        Self::Constant(v).into(),
                    )
                })
                .collect(),
        )
        .unwrap()
    }
}

impl std::fmt::Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Constant(value) => value.to_string(),
            Self::Field(index, _) => format!("#{}", index),
            Self::Field(_, Some((None, name))) => name.to_string(),
            Self::Field(_, Some((Some(table), name))) => format!("{}.{}", table, name),

            Self::And(lhs, rhs) => format!("{} AND {}", lhs, rhs),
            Self::Not(expr) => format!("NOT {}", expr),
            Self::Or(lhs, rhs) => format!("{} OR {}", lhs, rhs),

            Self::Equal(lhs, rhs) => format!("{} = {}", lhs, rhs),
            Self::GreaterThan(lhs, rhs) => format!("{} > {}", lhs, rhs),
            Self::IsNull(expr) => format!("{} IS NULL", expr),
            Self::LessThan(lhs, rhs) => format!("{} < {}", lhs, rhs),

            Self::Plus(lhs, rhs) => format!("{} + {}", lhs, rhs),
            Self::Assert(expr) => expr.to_string(),
            Self::Divide(lhs, rhs) => format!("{} / {}", lhs, rhs),
            Self::Exponentiate(lhs, rhs) => format!("{} ^ {}", lhs, rhs),
            Self::Factorial(expr) => format!("!{}", expr),
            Self::Modulo(lhs, rhs) => format!("{} % {}", lhs, rhs),
            Self::Multiply(lhs, rhs) => format!("{} * {}", lhs, rhs),
            Self::Negate(expr) => format!("-{}", expr),
            Self::Minus(lhs, rhs) => format!("{} - {}", lhs, rhs),

            Self::Like(lhs, rhs) => format!("{} LIKE {}", lhs, rhs),
        };

        write!(f, "{}", s)
    }
}
