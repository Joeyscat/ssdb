use super::engine::Transaction;
use super::types::{DataType, Value};
use crate::error::{Error, Result};

use serde_derive::{Deserialize, Serialize};

/// The Catalog stores schema information
pub trait Catalog {
    /// Create a new table
    fn create_table(&mut self, table: Table) -> Result<()>;
    /// Delete an existing table, or error if it doesn't exist
    fn delete_table(&mut self, table: &str) -> Result<()>;
    /// Read a table, if it exists
    fn read_table(&self, table: &str) -> Result<Option<Table>>;
    /// Iterate over all tables
    fn tables(&self) -> Result<Tables>;

    /// Read a table, or error if it doesn't exist
    fn must_read_table(&self, table: &str) -> Result<Table> {
        self.read_table(table)?
            .ok_or_else(|| Error::Value(format!("unknown table '{}'", table)))
    }

    /// Return all references to a table, as table, column pairs
    fn table_references(&self, table: &str, with_self: bool) -> Result<Vec<(String, Vec<String>)>> {
        Ok(self
            .tables()?
            .filter(|t| with_self || t.name != table)
            .map(|t| {
                (
                    t.name,
                    t.columns
                        .iter()
                        .filter(|c| c.references.as_deref() == Some(table))
                        .map(|c| c.name.clone())
                        .collect::<Vec<_>>(),
                )
            })
            .filter(|(_, refs)| !refs.is_empty())
            .collect())
    }
}

/// A table scan iterator
pub type Tables = Box<dyn DoubleEndedIterator<Item = Table> + Send>;

/// A table schema
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Table {
    /// The table name
    pub name: String,
    /// The table columns
    pub columns: Vec<Column>,
}

impl Table {
    /// Create a new table schema
    pub fn new(name: String, columns: Vec<Column>) -> Result<Self> {
        Ok(Self {
            name: name,
            columns,
        })
    }

    /// Fetch a column by name
    pub fn get_column(&self, name: &str) -> Result<&Column> {
        self.columns.iter().find(|c| c.name == name).ok_or_else(|| {
            Error::Value(format!(
                "Column '{}' not found in table '{}'",
                name, self.name
            ))
        })
    }

    /// Fetch a column index by name
    pub fn get_column_index(&self, name: &str) -> Result<usize> {
        self.columns
            .iter()
            .position(|c| c.name == name)
            .ok_or_else(|| {
                Error::Value(format!(
                    "Column '{}' not found in table '{}'",
                    name, self.name
                ))
            })
    }

    /// Return the primary key column of the table
    pub fn get_primary_key(&self) -> Result<&Column> {
        self.columns
            .iter()
            .find(|c| c.primary_key)
            .ok_or_else(|| Error::Value(format!("Table '{}' has no primary key", self.name)))
    }

    /// Return the primary key value of the row
    pub fn get_row_key(&self, row: &[Value]) -> Result<Value> {
        row.get(
            self.columns
                .iter()
                .position(|c| c.primary_key)
                .ok_or_else(|| Error::Value(format!("Table '{}' has no primary key", self.name)))?,
        )
        .cloned()
        .ok_or_else(|| Error::Value(format!("Table '{}' has no primary key", self.name)))
    }

    /// Validate the table schema
    pub fn validate(&self, txn: &mut dyn Transaction) -> Result<()> {
        if self.columns.is_empty() {
            return Err(Error::Value(format!(
                "Table '{}' has no columns",
                self.name
            )));
        }

        match self.columns.iter().filter(|c| c.primary_key).count() {
            0 => {
                return Err(Error::Value(format!(
                    "Table '{}' has no primary key",
                    self.name
                )))
            }
            1 => {}
            _ => {
                return Err(Error::Value(format!(
                    "Table '{}' has multiple primary keys",
                    self.name
                )))
            }
        };
        for column in &self.columns {
            column.validate(self, txn)?;
        }

        Ok(())
    }

    /// Validate a row
    pub fn validate_row(&self, row: &[Value], txn: &mut dyn Transaction) -> Result<()> {
        if row.len() != self.columns.len() {
            return Err(Error::Value(format!(
                "Row has {} columns, expected {}",
                row.len(),
                self.columns.len()
            )));
        }

        let pk = self.get_row_key(row)?;
        for (column, value) in self.columns.iter().zip(row.iter()) {
            column.validate_value(self, &pk, value, txn)?;
        }

        Ok(())
    }
}

impl std::fmt::Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE TABLE {} (\n{}\n)",
            format_ident(&self.name),
            self.columns
                .iter()
                .map(|c| format!("  {}", c))
                .collect::<Vec<String>>()
                .join(",\n")
        )
    }
}

/// A table column schema
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Column {
    /// The column name
    pub name: String,
    /// The column type
    pub data_type: DataType,
    /// The column default value
    pub default: Option<Value>,
    /// The column is nullable
    pub nullable: bool,
    /// The column is a primary key
    pub primary_key: bool,
    /// The column is unique
    pub unique: bool,
    /// The column is indexed
    pub indexed: bool,
    /// The column is a foreign key
    pub references: Option<String>,
}

impl Column {
    /// Validate the column schema
    pub fn validate(&self, table: &Table, txn: &mut dyn Transaction) -> Result<()> {
        // validate the primary key
        if self.primary_key && !self.unique {
            return Err(Error::Value(format!(
                "Primary key column '{}' must be unique",
                self.name
            )));
        }
        if self.primary_key && self.nullable {
            return Err(Error::Value(format!(
                "Primary key column '{}' cannot be nullable",
                self.name
            )));
        }
        if self.primary_key && self.default.is_some() {
            return Err(Error::Value(format!(
                "Primary key column '{}' cannot have a default value",
                self.name
            )));
        }

        // validate default value
        if let Some(default) = &self.default {
            if let Some(datetype) = default.data_type() {
                if datetype != self.data_type {
                    return Err(Error::Value(format!(
                        "Default value for column '{}' has type {}, expected {}",
                        self.name, datetype, self.data_type
                    )));
                }
            } else if !self.nullable {
                return Err(Error::Value(format!(
                    "Default value for non-nullable column '{}' cannot be NULL",
                    self.name
                )));
            }
        } else if self.nullable {
            return Err(Error::Value(format!(
                "Nullable column '{}' must have a default value",
                self.name
            )));
        }

        // validate references
        if let Some(reference) = &self.references {
            let target = if reference == &table.name {
                table.clone()
            } else if let Some(table) = txn.read_table(reference)? {
                table
            } else {
                return Err(Error::Value(format!(
                    "Table '{}' referenced by column '{}' does not exist",
                    reference, self.name,
                )));
            };

            if self.data_type != target.get_primary_key()?.data_type {
                return Err(Error::Value(format!(
                    "Column '{}' has type {}, but referenced column '{}' has type {}",
                    self.name,
                    self.data_type,
                    target.name,
                    target.get_primary_key()?.data_type
                )));
            }
        }

        Ok(())
    }

    /// Validate a column value
    pub fn validate_value(
        &self,
        table: &Table,
        pk: &Value,
        value: &Value,
        txn: &mut dyn Transaction,
    ) -> Result<()> {
        // validate date_type
        match value.data_type() {
            None if self.nullable => Ok(()),
            None => Err(Error::Value(format!(
                "Column '{}' cannot be NULL",
                self.name
            ))),
            Some(ref data_type) if data_type != &self.data_type => Err(Error::Value(format!(
                "Column '{}' has type {}, expected {}",
                self.name, data_type, self.data_type
            ))),
            _ => Ok(()),
        }?;

        // validate value
        match value {
            Value::String(s) if s.len() > 1024 => Err(Error::Value(format!(
                "String value for column '{}' is too long",
                self.name
            ))),
            _ => Ok(()),
        }?;

        // validate outgoing references
        if let Some(target) = &self.references {
            match value {
                Value::Null => Ok(()),
                Value::Float(f) if f.is_nan() => Ok(()),
                value if target == &table.name && value == pk => Ok(()),
                value if txn.read_row(target, value)?.is_none() => Err(Error::Value(format!(
                    "Referenced row '{}' in table '{}' does not exist",
                    value, target
                ))),
                _ => Ok(()),
            }?;
        }

        // validate uniqueness constraint
        if self.unique && !self.primary_key && value != &Value::Null {
            let index = table.get_column_index(&self.name)?;
            let mut scan = txn.scan(table.name.as_str(), None)?;

            while let Some(row) = scan.next().transpose()? {
                if row.get(index).unwrap_or(&Value::Null) == value
                    && &table.get_row_key(&row)? != pk
                {
                    return Err(Error::Value(format!(
                        "Duplicate value '{}' for unique column '{}'",
                        value, self.name
                    )));
                }
            }
        }

        Ok(())
    }
}

impl std::fmt::Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut sql = format_ident(&self.name);
        sql += &format!(" {}", self.data_type);
        if self.primary_key {
            sql += " PRIMARY KEY";
        }
        if !self.nullable && !self.primary_key {
            sql += " NOT NULL";
        }
        if let Some(default) = &self.default {
            sql += &format!(" DEFAULT {}", default);
        }
        if self.unique && !self.primary_key {
            sql += " UNIQUE";
        }
        if let Some(reference) = &self.references {
            sql += &format!(" REFERENCES {}", reference);
        }
        if self.indexed {
            sql += " INDEXED";
        }

        write!(f, "{}", sql)
    }
}

fn format_ident(ident: &str) -> String {
    lazy_static::lazy_static! {
        static ref RE_IDENT: regex::Regex = regex::Regex::new(r#"^\w[\w_]*$"#).unwrap();
    }

    if RE_IDENT.is_match(ident) {
        ident.to_string()
    } else {
        format!("\"{}\"", ident.replace("\"", "\"\""))
    }
}
