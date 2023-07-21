use serde_derive::{Deserialize, Serialize};

use crate::error::Error;

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
    Equals(Box<Expression>, Box<Expression>),
    GreaterThan(Box<Expression>, Box<Expression>),
    IsNull(Box<Expression>),
    LessThan(Box<Expression>, Box<Expression>),

    // Math operations
    Plus(Box<Expression>, Box<Expression>),
    Assert(Box<Expression>),
    Divide(Box<Expression>, Box<Expression>),
    Exponentiate(Box<Expression>, Box<Expression>),
    Factorial(Box<Expression>),
    Modulo(Box<Expression>, Box<Expression>),
    Multiply(Box<Expression>, Box<Expression>),
    Negate(Box<Expression>),
    Subtract(Box<Expression>, Box<Expression>),

    // String operations
    Like(Box<Expression>, Box<Expression>),
}

impl Expression {
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
            Self::Equals(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
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
        })
    }
}
