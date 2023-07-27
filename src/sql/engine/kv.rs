use super::super::schema::{Catalog, Table, Tables};
use super::super::types::{Expression, Row, Value};
use super::Transaction as _;
use crate::error::{Error, Result};
use crate::storage::kv;

use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::clone::Clone;
use std::collections::HashSet;

///  基于底层MVCC键值存储的SQL引擎
pub struct KV {
    /// 底层键值存储
    pub(super) kv: kv::MVCC,
}

impl KV {
    /// 创建一个新的SQL引擎
    pub fn new(kv: kv::MVCC) -> Self {
        Self { kv }
    }

    pub fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.kv.get_metadata(key)
    }

    pub fn set_metadata(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.kv.set_metadata(key, value)
    }
}

impl super::Engine for KV {
    type Transaction = Transaction;

    fn begin(&self, mode: super::Mode) -> Result<Self::Transaction> {
        Ok(Self::Transaction::new(self.kv.begin_with_mode(mode)?))
    }

    fn resume(&self, id: u64) -> Result<Self::Transaction> {
        Ok(Self::Transaction::new(self.kv.resume(id)?))
    }
}

// FIXME Implement Clone manually due to https://github.com/rust-lang/rust/issues/26925
impl Clone for KV {
    fn clone(&self) -> Self {
        Self {
            kv: self.kv.clone(),
        }
    }
}

fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    Ok(bincode::serialize(value)?)
}

fn deserialize<'a, T: Deserialize<'a>>(value: &'a [u8]) -> Result<T> {
    Ok(bincode::deserialize(value)?)
}

/// 基于底层MVCC键值存储的SQL事务
pub struct Transaction {
    txn: kv::mvcc::Transaction,
}

impl Transaction {
    /// 创建一个新的SQL事务
    pub fn new(txn: kv::mvcc::Transaction) -> Self {
        Self { txn }
    }

    /// 加载索引
    fn index_load(&self, table: &str, column: &str, value: &Value) -> Result<HashSet<Value>> {
        Ok(self
            .txn
            .get(&Key::Index(table.into(), column.into(), Some(value.into())).encode())?
            .map(|v| deserialize(&v))
            .transpose()?
            .unwrap_or_else(HashSet::new))
    }

    /// 保存索引
    fn index_save(
        &mut self,
        table: &str,
        column: &str,
        value: &Value,
        index: HashSet<Value>,
    ) -> Result<()> {
        let key = Key::Index(table.into(), column.into(), Some(value.into())).encode();
        if index.is_empty() {
            self.txn.delete(&key)
        } else {
            self.txn.set(&key, serialize(&index)?)
        }
    }
}

impl super::Transaction for Transaction {
    fn id(&self) -> u64 {
        self.txn.id()
    }

    fn mode(&self) -> super::Mode {
        self.txn.mode()
    }

    fn commit(self) -> Result<()> {
        self.txn.commit()
    }

    fn rollback(self) -> Result<()> {
        self.txn.rollback()
    }

    fn create_row(&mut self, table: &str, row: Row) -> Result<()> {
        let table = self.must_read_table(table)?;
        table.validate_row(&row, self)?;
        let id = table.get_row_key(&row)?;
        if self.read_row(&table.name, &id)?.is_some() {
            return Err(Error::Value(format!(
                "Duplicate primary key {} for table {}",
                id, table.name
            )));
        }

        self.txn.set(
            &Key::Row(Cow::Borrowed(&table.name), Some(Cow::Borrowed(&id))).encode(),
            serialize(&row)?,
        )?;

        // 更新索引
        for (i, column) in table.columns.iter().enumerate().filter(|(_, c)| c.indexed) {
            let mut index = self.index_load(&table.name, &column.name, &row[i])?;
            index.insert(id.clone());
            self.index_save(&table.name, &column.name, &row[i], index)?;
        }

        Ok(())
    }

    fn delete_row(&mut self, table: &str, id: &Value) -> Result<()> {
        let table = self.must_read_table(table)?;
        for (t, cs) in self.table_references(&table.name, true)? {
            let t = self.must_read_table(&t)?;
            let cs = cs
                .into_iter()
                .map(|c| Ok((t.get_column_index(&c)?, c)))
                .collect::<Result<Vec<_>>>()?;
            let mut scan = self.scan(&t.name, None)?;

            while let Some(row) = scan.next().transpose()? {
                for (i, c) in &cs {
                    if &row[*i] == id && (table.name != t.name || id != &t.get_row_key(&row)?) {
                        return Err(Error::Value(format!(
                            "Primary key {} is referenced by table {} column {}",
                            id, t.name, c
                        )));
                    }
                }
            }
        }

        let indexes: Vec<_> = table
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.indexed)
            .collect();
        if !indexes.is_empty() {
            if let Some(row) = self.read_row(&table.name, id)? {
                for (i, column) in indexes {
                    let mut index = self.index_load(&table.name, &column.name, &row[i])?;
                    index.remove(id);
                    self.index_save(&table.name, &column.name, &row[i], index)?;
                }
            }
        }
        self.txn
            .delete(&Key::Row(table.name.into(), Some(id.into())).encode())
    }

    fn read_row(&self, table: &str, id: &Value) -> Result<Option<Row>> {
        self.txn
            .get(&Key::Row(table.into(), Some(id.into())).encode())?
            .map(|v| deserialize(&v))
            .transpose()
    }

    fn update_row(&mut self, table: &str, id: &Value, row: Row) -> Result<()> {
        let table = self.must_read_table(table)?;
        // 检查主键是否更改, 如果是, 我们通过删除旧的行并创建新的行来处理它. 否则, 我们原地更新行.
        if id != &table.get_row_key(&row)? {
            self.delete_row(&table.name, id)?;
            self.create_row(&table.name, row)?;
            return Ok(());
        }

        // 更新索引
        let indexes: Vec<_> = table
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.indexed)
            .collect();
        if !indexes.is_empty() {
            let old = self.read_row(&table.name, id)?.unwrap();
            for (i, column) in indexes {
                if old[i] == row[i] {
                    continue;
                }
                let mut index = self.index_load(&table.name, &column.name, &old[i])?;
                index.remove(id);
                self.index_save(&table.name, &column.name, &old[i], index)?;

                let mut index = self.index_load(&table.name, &column.name, &row[i])?;
                index.insert(id.clone());
                self.index_save(&table.name, &column.name, &row[i], index)?;
            }
        }

        table.validate_row(&row, self)?;
        self.txn.set(
            &Key::Row(table.name.into(), Some(id.into())).encode(),
            serialize(&row)?,
        )
    }

    fn read_index(&self, table: &str, column: &str, value: &Value) -> Result<HashSet<Value>> {
        if !self.must_read_table(table)?.get_column(column)?.indexed {
            return Err(Error::Value(format!(
                "Column {} is not indexed in table {}",
                column, table
            )));
        }
        self.index_load(table, column, value)
    }

    fn scan(&self, table: &str, filter: Option<Expression>) -> Result<super::Scan> {
        let table = self.must_read_table(table)?;
        let iter = self
            .txn
            .scan_prefix(&Key::Row((&table.name).into(), None).encode())?
            .map(|r| r.and_then(|(_, v)| deserialize(&v)))
            .filter_map(move |r| match r {
                Ok(row) => match &filter {
                    Some(filter) => match filter.evaluate(Some(&row)) {
                        Ok(Value::Boolean(b)) if b => Some(Ok(row)),
                        Ok(Value::Boolean(_)) | Ok(Value::Null) => None,
                        Ok(v) => Some(Err(Error::Value(format!(
                            "Filter expression returned non-boolean value {}",
                            v
                        )))),
                        Err(e) => Some(Err(e)),
                    },
                    None => Some(Ok(row)),
                },
                e => Some(e),
            });

        Ok(Box::new(iter))
    }

    fn scan_index(&self, table: &str, column: &str) -> Result<super::IndexScan> {
        let table = self.must_read_table(table)?;
        let column = table.get_column(column)?;
        if !column.indexed {
            return Err(Error::Value(format!(
                "Column {} is not indexed in table {}",
                column.name, table.name
            )));
        }

        let iter = self
            .txn
            .scan_prefix(&Key::Index((&table.name).into(), (&column.name).into(), None).encode())?
            .map(|r| -> Result<(Value, HashSet<Value>)> {
                let (k, v) = r?;
                let value = match Key::decode(&k)? {
                    Key::Index(_, _, Some(pk)) => pk.into_owned(),
                    _ => return Err(Error::Internal("Invalid index key".into())),
                };

                Ok((value, deserialize(&v)?))
            });

        Ok(Box::new(iter))
    }
}

impl Catalog for Transaction {
    fn create_table(&mut self, table: Table) -> Result<()> {
        if self.read_table(&table.name)?.is_some() {
            return Err(Error::Value(format!("Table {} already exists", table.name)));
        }
        table.validate(self)?;
        self.txn.set(
            &Key::Table(Some((&table.name).into())).encode(),
            serialize(&table)?,
        )
    }

    fn delete_table(&mut self, table: &str) -> Result<()> {
        let table = self.must_read_table(table)?;
        if let Some((t, cs)) = self.table_references(&table.name, false)?.first() {
            return Err(Error::Value(format!(
                "Table {} is referenced by table {} column {}",
                table.name, t, cs[0]
            )));
        }

        let mut scan = self.scan(&table.name, None)?;
        while let Some(row) = scan.next().transpose()? {
            self.delete_row(&table.name, &table.get_row_key(&row)?)?
        }
        self.txn
            .delete(&Key::Table(Some(table.name.into())).encode())
    }

    fn read_table(&self, table: &str) -> Result<Option<Table>> {
        self.txn
            .get(&Key::Table(Some(table.into())).encode())?
            .map(|v| deserialize(&v))
            .transpose()
    }

    fn tables(&self) -> Result<Tables> {
        Ok(Box::new(
            self.txn
                .scan_prefix(&Key::Table(None).encode())?
                .map(|r| r.and_then(|(_, v)| deserialize(&v)))
                .collect::<Result<Vec<_>>>()?
                .into_iter(),
        ))
    }
}

/// 编码SQL键, 使用保序编码 - 有关详细信息, 请参阅 `kv::encoding`. 选项可以为 None 以获取键空间前缀.
/// 我们直接使用表和列名称作为标识符, 以避免额外的间接和相关开销. 不可能更改名称, 因此这是可以的.
/// 使用 Cows, 因为我们想在编码时借用, 但在解码时返回所有权.
enum Key<'a> {
    /// 给定表名称的表模式键
    Table(Option<Cow<'a, str>>),
    /// 索引条目的键
    Index(Cow<'a, str>, Cow<'a, str>, Option<Cow<'a, Value>>),
    /// 由表名称和行主键标识的行的键
    Row(Cow<'a, str>, Option<Cow<'a, Value>>),
}

impl<'a> Key<'a> {
    /// 将键编码为字节序列
    fn encode(self) -> Vec<u8> {
        use kv::encoding::*;
        match self {
            Self::Table(None) => vec![0x01],
            Self::Table(Some(name)) => [&[0x01][..], &encode_string(&name)].concat(),
            Self::Index(table, column, None) => {
                [&[0x02][..], &encode_string(&table), &encode_string(&column)].concat()
            }
            Self::Index(table, column, Some(value)) => [
                &[0x02][..],
                &encode_string(&table),
                &encode_string(&column),
                &encode_value(&value),
            ]
            .concat(),
            Self::Row(table, None) => [&[0x03][..], &encode_string(&table)].concat(),
            Self::Row(table, Some(pk)) => {
                [&[0x03][..], &encode_string(&table), &encode_value(&pk)].concat()
            }
        }
    }

    /// 将字节序列解码为键
    fn decode(mut bytes: &[u8]) -> Result<Self> {
        use kv::encoding::*;
        let bytes = &mut bytes;
        let key = match take_byte(bytes)? {
            0x01 => Self::Table(Some(take_string(bytes)?.into())),
            0x02 => Self::Index(
                take_string(bytes)?.into(),
                take_string(bytes)?.into(),
                Some(take_value(bytes)?.into()),
            ),
            0x03 => Self::Row(take_string(bytes)?.into(), Some(take_value(bytes)?.into())),
            b => return Err(Error::Internal(format!("Unknown SQL key prefix {:x?}", b))),
        };
        if !bytes.is_empty() {
            return Err(Error::Internal(
                "Unexpected data remaining at end of SQL key".into(),
            ));
        }

        Ok(key)
    }
}
