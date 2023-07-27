use super::encoding;
use super::{Range, Store};
use crate::error::{Error, Result};

use serde::{Deserialize, Serialize};
// use serde_derive::{Deserialize, Serialize};

use std::collections::HashSet;
use std::iter::Peekable;
use std::ops::{Bound, RangeBounds};
use std::sync::RwLockReadGuard;
use std::{
    borrow::Cow,
    sync::{Arc, RwLock, RwLockWriteGuard},
};

/// MVCC 状态
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Status {
    pub txns: u64,
    pub txns_active: u64,
    pub storage: String,
}

/// 基于 MVCC 的事务性键值存储.
pub struct MVCC {
    /// The underlying store. It is protected by a mutex so it can be shared between txns.
    store: Arc<RwLock<Box<dyn Store>>>,
}

impl Clone for MVCC {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
        }
    }
}

impl MVCC {
    pub fn new(store: Box<dyn Store>) -> Self {
        Self {
            store: Arc::new(RwLock::new(store)),
        }
    }

    pub fn begin(&self) -> Result<Transaction> {
        Transaction::begin(self.store.clone(), Mode::ReadWrite)
    }

    pub fn begin_with_mode(&self, mode: Mode) -> Result<Transaction> {
        Transaction::begin(self.store.clone(), mode)
    }

    pub fn resume(&self, id: u64) -> Result<Transaction> {
        Transaction::resume(self.store.clone(), id)
    }

    pub fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.store.read()?.get(&Key::Metadata(key.into()).encode())
    }

    pub fn set_metadata(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.store
            .write()?
            .set(&Key::Metadata(key.into()).encode(), value)
    }

    pub fn status(&self) -> Result<Status> {
        let store = self.store.read()?;
        Ok(Status {
            txns: match store.get(&Key::TxnNext.encode())? {
                Some(ref value) => deserialize(value)?,
                None => 1,
            },
            txns_active: store
                .scan(Range::from(
                    Key::TxnActive(0).encode()..Key::TxnActive(u64::MAX).encode(),
                ))
                .try_fold(0, |count, r| r.map(|_| count + 1))?,
            storage: store.to_string(),
        })
    }
}

/// 序列化 MVCC 元数据
fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>> {
    Ok(bincode::serialize(value)?)
}

/// 反序列化 MVCC 元数据
fn deserialize<'a, V: Deserialize<'a>>(value: &'a [u8]) -> Result<V> {
    Ok(bincode::deserialize(value)?)
}

// MVCC 事务
pub struct Transaction {
    /// 底层存储. 它受到互斥锁的保护, 因此可以在多个事务之间共享.
    store: Arc<RwLock<Box<dyn Store>>>,
    /// 唯一的事务 ID
    id: u64,
    /// 事务模式
    mode: Mode,
    /// 事务正在读取的快照
    snapshot: Snapshot,
}

impl Transaction {
    /// 以给定模式开始新事务
    pub fn begin(store: Arc<RwLock<Box<dyn Store>>>, mode: Mode) -> Result<Self> {
        let mut session = store.write()?;

        let id = match session.get(&Key::TxnNext.encode())? {
            Some(ref value) => deserialize(value)?,
            None => 1,
        };
        session.set(&Key::TxnNext.encode(), serialize(&(id + 1))?)?;
        session.set(&Key::TxnActive(id).encode(), serialize(&mode)?)?;

        // 总是获取快照, 即使是快照事务, 因为所有事务都会增加事务 ID 计数器,
        // 我们需要正确记录当前活动事务, 以供将来查看该事务的任何快照事务使用.
        let mut snapshot = Snapshot::take(&mut session, id)?;
        std::mem::drop(session);
        if let Mode::Snapshot { version } = &mode {
            snapshot = Snapshot::restore(&store.read()?, *version)?
        }

        Ok(Self {
            store,
            id,
            mode,
            snapshot,
        })
    }

    /// 使用给定 ID 恢复活动事务. 如果事务是非活动的, 则返回错误
    fn resume(store: Arc<RwLock<Box<dyn Store>>>, id: u64) -> Result<Self> {
        let session = store.read()?;
        let mode = match session.get(&Key::TxnActive(id).encode())? {
            Some(value) => deserialize(&value)?,
            None => return Err(Error::Value(format!("Transaction {} is not active", id))),
        };

        let snapshot = match &mode {
            Mode::Snapshot { version } => Snapshot::restore(&session, *version)?,
            _ => Snapshot::restore(&session, id)?,
        };
        std::mem::drop(session);

        Ok(Self {
            store,
            id,
            mode,
            snapshot,
        })
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn mode(&self) -> Mode {
        self.mode
    }

    pub fn commit(self) -> Result<()> {
        let mut session = self.store.write()?;
        session.delete(&Key::TxnActive(self.id).encode())?;
        session.flush()?;
        Ok(())
    }

    pub fn rollback(self) -> Result<()> {
        let mut session = self.store.write()?;
        if self.mode.mutable() {
            let mut rollback = Vec::new();
            let mut scan = session.scan(Range::from(
                Key::TxnUpdate(self.id, vec![].into()).encode()
                    ..Key::TxnUpdate(self.id + 1, vec![].into()).encode(),
            ));
            while let Some((key, _)) = scan.next().transpose()? {
                match Key::decode(&key)? {
                    Key::TxnUpdate(_, updated_key) => rollback.push(updated_key.into_owned()),
                    k => return Err(Error::Internal(format!("Expected TxnUpdate, got {:?}", k)))?,
                };
                rollback.push(key);
            }
            std::mem::drop(scan);
            for key in rollback.into_iter() {
                session.delete(&key)?;
            }
        }
        session.delete(&Key::TxnActive(self.id).encode())?;
        session.flush()?;
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.write(key, None)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let session = self.store.read()?;
        let mut scan = session
            .scan(Range::from(
                Key::Record(key.into(), 0).encode()..=Key::Record(key.into(), self.id).encode(),
            ))
            .rev();
        while let Some((key, value)) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::Record(_, txn_id) => {
                    if self.snapshot.is_visible(txn_id) {
                        return deserialize(&value);
                    }
                }
                k => return Err(Error::Internal(format!("Expected Record, got {:?}", k)))?,
            };
        }
        Ok(None)
    }

    pub fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Result<super::Scan> {
        let start = match range.start_bound() {
            Bound::Excluded(key) => {
                Bound::Excluded(Key::Record(key.into(), std::u64::MAX).encode())
            }
            Bound::Included(key) => Bound::Included(Key::Record(key.into(), 0).encode()),
            Bound::Unbounded => Bound::Included(Key::Record(vec![].into(), 0).encode()),
        };
        let end = match range.end_bound() {
            Bound::Excluded(key) => Bound::Excluded(Key::Record(key.into(), 0).encode()),
            Bound::Included(key) => {
                Bound::Included(Key::Record(key.into(), std::u64::MAX).encode())
            }
            Bound::Unbounded => Bound::Unbounded,
        };
        let scan = self.store.read()?.scan(Range::from((start, end)));
        Ok(Box::new(Scan::new(scan, self.snapshot.clone())))
    }

    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<super::Scan> {
        if prefix.is_empty() {
            return Err(Error::Internal("Scan prefix cannot be empty".to_string()));
        }
        let start = prefix.to_vec();
        let mut end = start.clone();
        for i in (0..end.len()).rev() {
            match end[i] {
                // if all 0xff we could in principle use `Range::Unbounded`, but it won't happen
                0xff if i == 0 => {
                    return Err(Error::Internal("Invalid prefix scan range".to_string()))
                }
                0xff => {
                    end[i] = 0x00;
                    continue;
                }
                v => {
                    end[i] = v + 1;
                    break;
                }
            }
        }
        self.scan(start..end)
    }

    pub fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.write(key, Some(value))
    }

    /// 为一个 key 写入一个 value, `None` 值表示删除.
    fn write(&self, key: &[u8], value: Option<Vec<u8>>) -> Result<()> {
        if !self.mode.mutable() {
            return Err(Error::ReadOnly);
        }

        let mut session = self.store.write()?;

        // 检查 key 是否是脏的, 即它是否有任何未提交的更新, 通过扫描对当前事务不可见的任何版本来检查.
        let min = self
            .snapshot
            .invisible
            .iter()
            .min()
            .cloned()
            .unwrap_or(self.id + 1);
        let mut scan = session
            .scan(Range::from(
                Key::Record(key.into(), min).encode()
                    ..=Key::Record(key.into(), std::u64::MAX).encode(),
            ))
            .rev();
        while let Some((key, _)) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::Record(_, version) => {
                    if !self.snapshot.is_visible(version) {
                        // 写冲突
                        return Err(Error::Serialization);
                    }
                }
                k => {
                    return Err(Error::Internal(format!(
                        "Expected Txn::Record, got {:?}",
                        k
                    )))
                }
            };
        }
        std::mem::drop(scan);

        // 写入 key 及其更新记录.
        let key = Key::Record(key.into(), self.id).encode();
        let update = Key::TxnUpdate(self.id, (&key).into()).encode();
        session.set(&update, vec![])?;
        session.set(&key, serialize(&value)?)
    }
}

/// MVCC 事务模式
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum Mode {
    /// 读写事务
    ReadWrite,
    /// 只读事务
    ReadOnly,
    /// 从特定快照版本读取的只读事务.
    ///
    /// 版本必须指向已提交的事务. 对原始事务可见的任何更改都将对快照事务可见.
    /// (例如, 在快照事务开始时尚未提交的事务对快照不可见, 即使它们具有较低的版本.)
    Snapshot { version: u64 },
}

impl Mode {
    /// 检查事务是否可变
    pub fn mutable(&self) -> bool {
        match self {
            Mode::ReadWrite => true,
            Mode::ReadOnly | Mode::Snapshot { .. } => false,
        }
    }

    /// 检查一个模式是否满足另一个模式(例如, ReadWrite 满足 ReadOnly).
    pub fn satisfies(&self, other: &Mode) -> bool {
        match (self, other) {
            (Mode::ReadWrite, Mode::ReadOnly) => true,
            (Mode::Snapshot { .. }, Mode::ReadOnly) => true,
            (_, _) if self == other => true,
            _ => false,
        }
    }
}

/// 一个带版本的快照, 包含事务的可见性信息.
#[derive(Clone)]
struct Snapshot {
    /// 快照的事务 ID
    version: u64,
    /// 在事务开始时处于活动状态的事务 ID 集合, 因此对快照不可见.
    invisible: HashSet<u64>,
}

impl Snapshot {
    /// 获取一个新快照, 将其保存为 `Key::TxnSnapshot(version)`
    fn take(session: &mut RwLockWriteGuard<Box<dyn Store>>, version: u64) -> Result<Self> {
        let mut snapshot = Self {
            version,
            invisible: HashSet::new(),
        };
        let mut scan = session.scan(Range::from(
            Key::TxnActive(0).encode()..Key::TxnActive(version).encode(),
        ));

        while let Some((key, _)) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::TxnActive(txn_id) => {
                    snapshot.invisible.insert(txn_id);
                }
                k => return Err(Error::Internal(format!("Expected TxnActive, got {:?}", k)))?,
            };
        }
        std::mem::drop(scan);
        session.set(
            &Key::TxnSnapshot(version).encode(),
            serialize(&snapshot.invisible)?,
        )?;
        Ok(snapshot)
    }

    /// 从 `Key::TxnSnapshot(version)` 恢复现有快照, 如果不存在则返回错误
    fn restore(session: &RwLockReadGuard<Box<dyn Store>>, version: u64) -> Result<Self> {
        match session.get(&Key::TxnSnapshot(version).encode())? {
            Some(ref value) => Ok(Self {
                version,
                invisible: deserialize(value)?,
            }),
            None => Err(Error::Value(format!(
                "Snapshot not found for version {}",
                version
            ))),
        }
    }

    /// 检查给定版本对快照是否可见.
    fn is_visible(&self, version: u64) -> bool {
        version <= self.version && self.invisible.get(&version).is_none()
    }
}

/// MVCC 键. 编码保留键的分组和排序. 使用 Cow, 因为我们想在编码时借用, 在解码时拥有数据的所有权
#[derive(Debug)]
enum Key<'a> {
    /// 下一个事务 ID. 用于开启新事务.
    TxnNext,
    /// 活动事务标记, 包含事务模式. 用于检测并发事务, 以及恢复.
    TxnActive(u64),
    /// 事务快照, 包含事务开始时的活动事务集合.
    TxnSnapshot(u64),
    /// 事务 ID 和 key 的更新标记, 用于回滚.
    TxnUpdate(u64, Cow<'a, [u8]>),
    /// 一个 key-version 对的记录.
    Record(Cow<'a, [u8]>, u64),
    /// 任意的无版本元数据.
    Metadata(Cow<'a, [u8]>),
}

impl<'a> Key<'a> {
    /// 将 key 编码为字节向量
    fn encode(self) -> Vec<u8> {
        use encoding::*;
        match self {
            Self::TxnNext => vec![0x01],
            Self::TxnActive(id) => [&[0x02][..], &encode_u64(id)].concat(),
            Self::TxnSnapshot(version) => [&[0x03][..], &encode_u64(version)].concat(),
            Self::TxnUpdate(id, key) => {
                [&[0x04][..], &encode_u64(id), &encode_bytes(&key)].concat()
            }
            Self::Metadata(key) => [&[0x05][..], &encode_bytes(&key)].concat(),
            Self::Record(key, version) => {
                [&[0xff][..], &encode_bytes(&key), &encode_u64(version)].concat()
            }
        }
    }

    /// 从字节表示中解码 key
    fn decode(mut bytes: &[u8]) -> Result<Self> {
        use encoding::*;
        let bytes = &mut bytes;
        let key = match take_byte(bytes)? {
            0x01 => Self::TxnNext,
            0x02 => Self::TxnActive(take_u64(bytes)?),
            0x03 => Self::TxnSnapshot(take_u64(bytes)?),
            0x04 => Self::TxnUpdate(take_u64(bytes)?, take_bytes(bytes)?.into()),
            0x05 => Self::Metadata(take_bytes(bytes)?.into()),
            0xff => Self::Record(take_bytes(bytes)?.into(), take_u64(bytes)?),
            b => return Err(Error::Internal(format!("Unknown MVCC key prefix {:x?}", b))),
        };
        if !bytes.is_empty() {
            return Err(Error::Internal(format!(
                "MVCC key has trailing bytes {:x?}",
                bytes
            )));
        }
        Ok(key)
    }
}

/// A key range scan.
pub struct Scan {
    /// The augmented KV store iterator, with key (decoded) and value. Note that we don't retain
    /// the decoded version, so there will be multiple keys (for each version). We want the last.
    scan: Peekable<super::Scan>,
    /// Keep track of `next_back()` seen keys, whose previous versions we want to skip.
    next_back_seen: Option<Vec<u8>>,
}

impl Scan {
    /// 创建一个新的扫描.
    fn new(mut scan: super::Scan, snapshot: Snapshot) -> Self {
        // 增强底层扫描以解码键并过滤不可见版本. 我们不返回版本, 因为我们不需要它,
        // 但是要注意返回键的所有版本-我们通常只需要最后一个, 这是 next() 和 next_back() 方法需要处理的.
        // 我们也不解码值, 因为我们只需要解码最后一个版本.
        scan = Box::new(scan.filter_map(move |r| {
            r.and_then(|(k, v)| match Key::decode(&k)? {
                Key::Record(_, version) if !snapshot.is_visible(version) => Ok(None),
                Key::Record(key, _) => Ok(Some((key.into_owned(), v))),
                k => Err(Error::Internal(format!("Expected Record, got {:?}", k))),
            })
            .transpose()
        }));
        Self {
            scan: scan.peekable(),
            next_back_seen: None,
        }
    }

    // 带有错误处理的 `next()` 实现
    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some((key, value)) = self.scan.next().transpose()? {
            // 只有当它是键的最后一个版本时才返回该项.
            if match self.scan.peek() {
                Some(Ok((peek_key, _))) if *peek_key != key => true,
                Some(Ok(_)) => false,
                Some(Err(err)) => return Err(err.clone()),
                None => true,
            } {
                // 只返回没有标记为删除的项.
                if let Some(value) = deserialize(&value)? {
                    return Ok(Some((key, value)));
                }
            }
        }
        Ok(None)
    }

    // 带有错误处理的 `next_back()` 实现
    fn try_nexn_back(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some((key, value)) = self.scan.next_back().transpose()? {
            // 只有当它是键的最后一个版本时才返回该项.
            if match &self.next_back_seen {
                Some(seen_key) if *seen_key != key => true,
                Some(_) => false,
                None => true,
            } {
                self.next_back_seen = Some(key.clone());
                // 只返回没有标记为删除的项.
                if let Some(value) = deserialize(&value)? {
                    return Ok(Some((key, value)));
                }
            }
        }
        Ok(None)
    }
}

impl Iterator for Scan {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl DoubleEndedIterator for Scan {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.try_nexn_back().transpose()
    }
}

#[cfg(test)]
pub mod tests {
    use super::super::Test;
    use super::*;

    fn setup() -> MVCC {
        MVCC::new(Box::new(Test::new()))
    }

    #[test]
    fn test_begin() -> Result<()> {
        let mvcc = setup();

        let txn = mvcc.begin()?;
        assert_eq!(txn.id(), 1);
        assert_eq!(txn.mode(), Mode::ReadWrite);
        txn.commit()?;

        let txn = mvcc.begin()?;
        assert_eq!(txn.id(), 2);
        txn.rollback()?;

        let txn = mvcc.begin()?;
        assert_eq!(txn.id(), 3);
        txn.commit()?;

        Ok(())
    }

    #[test]
    fn test_begin_with_mode_readonly() -> Result<()> {
        let mvcc = setup();
        let txn = mvcc.begin_with_mode(Mode::ReadOnly)?;
        assert_eq!(txn.id(), 1);
        assert_eq!(txn.mode(), Mode::ReadOnly);
        txn.commit()?;

        Ok(())
    }

    #[test]
    fn test_begin_with_mode_readwrite() -> Result<()> {
        let mvcc = setup();
        let txn = mvcc.begin_with_mode(Mode::ReadWrite)?;
        assert_eq!(txn.id(), 1);
        assert_eq!(txn.mode(), Mode::ReadWrite);
        txn.commit()?;

        Ok(())
    }

    #[test]
    fn test_begin_with_mode_snapshot() -> Result<()> {
        let mvcc = setup();

        // write a couple of versions for a key
        let mut txn = mvcc.begin_with_mode(Mode::ReadWrite)?;
        txn.set(b"key", b"value1".to_vec())?;
        txn.commit()?;
        let mut txn = mvcc.begin_with_mode(Mode::ReadWrite)?;
        txn.set(b"key", b"value2".to_vec())?;
        txn.commit()?;

        // check that we can start a snapshot in version 1
        let txn = mvcc.begin_with_mode(Mode::Snapshot { version: 1 })?;
        assert_eq!(txn.id(), 3);
        assert_eq!(txn.mode(), Mode::Snapshot { version: 1 });
        assert_eq!(txn.get(b"key")?, Some(b"value1".to_vec()));
        txn.commit()?;

        // check that we can start a snapshot in a past snapshot transaction
        let txn = mvcc.begin_with_mode(Mode::Snapshot { version: 3 })?;
        assert_eq!(txn.id(), 4);
        assert_eq!(txn.mode(), Mode::Snapshot { version: 3 });
        assert_eq!(txn.get(b"key")?, Some(b"value2".to_vec()));
        txn.commit()?;

        // check that the current transaction ID is valid as a snapshot version
        let txn = mvcc.begin_with_mode(Mode::Snapshot { version: 5 })?;
        assert_eq!(txn.id(), 5);
        assert_eq!(txn.mode(), Mode::Snapshot { version: 5 });
        txn.commit()?;

        // check that any future transaction ID is not valid as a snapshot version
        assert_eq!(
            mvcc.begin_with_mode(Mode::Snapshot { version: 9 }).err(),
            Some(Error::Value("Snapshot not found for version 9".to_string())),
        );

        // check that concurrent transactions are hidden from snapshots of past transactions.
        let mut txn_active = mvcc.begin()?;
        let txn_snapshot = mvcc.begin_with_mode(Mode::Snapshot { version: 1 })?;
        assert_eq!(txn_active.id(), 7);
        assert_eq!(txn_snapshot.id(), 8);
        txn_active.set(b"key", b"value7".to_vec())?;
        assert_eq!(txn_snapshot.get(b"key")?, Some(b"value1".to_vec()));
        txn_active.commit()?;
        txn_snapshot.commit()?;

        let txn = mvcc.begin_with_mode(Mode::Snapshot { version: 8 })?;
        assert_eq!(txn.id(), 9);
        assert_eq!(txn.get(b"key")?, Some(b"value2".to_vec()));
        txn.commit()?;

        Ok(())
    }

    #[test]
    fn test_resume() -> Result<()> {
        let mvcc = setup();

        // 首先写入一组应该可见的值
        let mut t1 = mvcc.begin()?;
        t1.set(b"k1", b"v1".to_vec())?;
        t1.set(b"k2", b"v1".to_vec())?;
        t1.commit()?;

        // 然后开启三个事务
        // t3将会在t2, t4提交后恢复, 但是它们的修改对t3不可见.
        // t3自己的修改是可见的.
        let mut t2 = mvcc.begin()?;
        let mut t3 = mvcc.begin()?;
        let mut t4 = mvcc.begin()?;

        t2.set(b"k1", b"v2".to_vec())?;
        t3.set(b"k2", b"v3".to_vec())?;
        t4.set(b"k3", b"v4".to_vec())?;

        t2.commit()?;
        t4.commit()?;

        // 恢复 t3, 检查它的修改是否可见.
        let id = t3.id();
        std::mem::drop(t3);
        let t3_resume = mvcc.resume(id)?;
        assert_eq!(t3_resume.id(), 3);
        assert_eq!(t3_resume.mode(), Mode::ReadWrite);

        assert_eq!(t3_resume.get(b"k1")?, Some(b"v1".to_vec()));
        assert_eq!(t3_resume.get(b"k2")?, Some(b"v3".to_vec()));
        assert_eq!(t3_resume.get(b"k3")?, None);

        // 新开启的事务应该看不到 t3 的修改, 但是能看到 t2 和 t4 的修改.
        let t5 = mvcc.begin()?;
        assert_eq!(t5.get(b"k1")?, Some(b"v2".to_vec()));
        assert_eq!(t5.get(b"k2")?, Some(b"v1".to_vec()));
        assert_eq!(t5.get(b"k3")?, Some(b"v4".to_vec()));
        t5.rollback()?;

        // t3_resume 提交后, 新的事务应该能看到它的修改.
        t3_resume.commit()?;

        let t6 = mvcc.begin()?;
        assert_eq!(t6.get(b"k1")?, Some(b"v2".to_vec()));
        assert_eq!(t6.get(b"k2")?, Some(b"v3".to_vec()));
        assert_eq!(t6.get(b"k3")?, Some(b"v4".to_vec()));
        t6.rollback()?;

        // 开启一个 snapshot 事务, 并进行 resume.
        let t7 = mvcc.begin_with_mode(Mode::Snapshot { version: 1 })?;
        assert_eq!(t7.id(), 7);
        assert_eq!(t7.get(b"k1")?, Some(b"v1".to_vec()));

        let id = t7.id();
        std::mem::drop(t7);
        let t7_resume = mvcc.resume(id)?;
        assert_eq!(t7_resume.id(), 7);
        assert_eq!(t7_resume.mode(), Mode::Snapshot { version: 1 });
        assert_eq!(t7_resume.get(b"k1")?, Some(b"v1".to_vec()));
        t7_resume.commit()?;

        // resume 一个非活跃事务应该返回错误.
        assert_eq!(
            mvcc.resume(7).err(),
            Some(Error::Value("Transaction 7 is not active".to_string())),
        );

        Ok(())
    }

    #[test]
    fn test_txn_delete_conflict() -> Result<()> {
        let mvcc = setup();

        let mut txn = mvcc.begin()?;
        txn.set(b"k", b"v".to_vec())?;
        txn.commit()?;

        let mut t1 = mvcc.begin()?;
        let mut t2 = mvcc.begin()?;
        let mut t3 = mvcc.begin()?;

        t2.delete(b"k")?;
        assert_eq!(t1.delete(b"k"), Err(Error::Serialization));
        assert_eq!(t3.delete(b"k"), Err(Error::Serialization));
        t2.commit()?;

        Ok(())
    }

    #[test]
    fn test_delele_idempotent() -> Result<()> {
        let mvcc = setup();

        let mut txn = mvcc.begin()?;
        txn.delete(b"k")?;
        txn.commit()?;

        Ok(())
    }

    #[test]
    fn test_txn_get() -> Result<()> {
        let mvcc = setup();

        let mut txn = mvcc.begin()?;
        assert_eq!(txn.get(b"k")?, None);
        txn.set(b"k", b"v".to_vec())?;
        assert_eq!(txn.get(b"k")?, Some(b"v".to_vec()));
        txn.set(b"k", b"v2".to_vec())?;
        assert_eq!(txn.get(b"k")?, Some(b"v2".to_vec()));
        txn.commit()?;

        Ok(())
    }

    #[test]
    fn test_txn_get_deleted() -> Result<()> {
        let mvcc = setup();

        let mut txn = mvcc.begin()?;
        txn.set(b"k", b"v".to_vec())?;
        txn.commit()?;

        let mut txn = mvcc.begin()?;
        txn.delete(b"k")?;
        txn.commit()?;

        let txn = mvcc.begin()?;
        assert_eq!(txn.get(b"k")?, None);
        txn.commit()?;

        Ok(())
    }

    #[test]
    fn test_txn_get_hides_newer() -> Result<()> {
        let mvcc = setup();

        let mut t1 = mvcc.begin()?;
        let t2 = mvcc.begin()?;
        let mut t3 = mvcc.begin()?;

        t1.set(b"k1", b"v1".to_vec())?;
        t1.commit()?;
        t3.set(b"k3", b"v3".to_vec())?;
        t3.commit()?;

        assert_eq!(t2.get(b"k1")?, None);
        assert_eq!(t2.get(b"k3")?, None);

        Ok(())
    }

    #[test]
    fn test_txn_get_hides_uncommitted() -> Result<()> {
        let mvcc = setup();

        let mut t1 = mvcc.begin()?;
        t1.set(b"k1", b"v1".to_vec())?;
        let t2 = mvcc.begin()?;
        let mut t3 = mvcc.begin()?;
        t3.set(b"k3", b"v3".to_vec())?;

        assert_eq!(t2.get(b"k1")?, None);
        assert_eq!(t2.get(b"k3")?, None);

        Ok(())
    }

    #[test]
    fn test_txn_get_readonly_historical() -> Result<()> {
        let mvcc = setup();

        let mut txn = mvcc.begin()?;
        txn.set(b"k1", b"v1".to_vec())?;
        txn.commit()?;

        let mut txn = mvcc.begin()?;
        txn.set(b"k2", b"v2".to_vec())?;
        txn.commit()?;

        let mut txn = mvcc.begin()?;
        txn.set(b"k3", b"v3".to_vec())?;
        txn.commit()?;

        let tr = mvcc.begin_with_mode(Mode::Snapshot { version: 2 })?;
        assert_eq!(tr.get(b"k1")?, Some(b"v1".to_vec()));
        assert_eq!(tr.get(b"k2")?, Some(b"v2".to_vec()));
        assert_eq!(tr.get(b"k3")?, None);

        Ok(())
    }

    #[test]
    fn test_txn_get_serial() -> Result<()> {
        let mvcc = setup();

        let mut txn = mvcc.begin()?;
        txn.set(b"k1", b"v1".to_vec())?;
        txn.commit()?;

        let txn = mvcc.begin()?;
        assert_eq!(txn.get(b"k1")?, Some(b"v1".to_vec()));

        Ok(())
    }

    #[test]
    fn test_txn_scan() -> Result<()> {
        let mvcc = setup();
        let mut txn = mvcc.begin()?;

        txn.set(b"k1", b"v1".to_vec())?;

        txn.delete(b"k2")?;

        txn.set(b"k3", b"v1".to_vec())?;
        txn.set(b"k3", b"v2".to_vec())?;
        txn.delete(b"k3")?;
        txn.set(b"k3", b"v3".to_vec())?;

        txn.set(b"k4", b"v1".to_vec())?;
        txn.set(b"k4", b"v2".to_vec())?;
        txn.set(b"k4", b"v3".to_vec())?;
        txn.set(b"k4", b"v4".to_vec())?;
        txn.delete(b"k4")?;

        txn.set(b"k5", b"v1".to_vec())?;
        txn.set(b"k5", b"v2".to_vec())?;
        txn.set(b"k5", b"v3".to_vec())?;
        txn.delete(b"k5")?;
        txn.set(b"k5", b"v4".to_vec())?;
        txn.set(b"k5", b"v5".to_vec())?;
        txn.commit()?;

        // 向前扫描
        let txn = mvcc.begin()?;
        assert_eq!(
            txn.scan(..)?.collect::<Result<Vec<_>>>()?,
            vec![
                (b"k1".to_vec(), b"v1".to_vec()),
                (b"k3".to_vec(), b"v3".to_vec()),
                (b"k5".to_vec(), b"v5".to_vec()),
            ]
        );

        // 向后扫描
        assert_eq!(
            txn.scan(..)?.rev().collect::<Result<Vec<_>>>()?,
            vec![
                (b"k5".to_vec(), b"v5".to_vec()),
                (b"k3".to_vec(), b"v3".to_vec()),
                (b"k1".to_vec(), b"v1".to_vec()),
            ]
        );

        // 前后交替
        let mut scan = txn.scan(..)?;
        assert_eq!(
            scan.next().transpose()?,
            Some((b"k1".to_vec(), b"v1".to_vec()))
        );
        assert_eq!(
            scan.next_back().transpose()?,
            Some((b"k5".to_vec(), b"v5".to_vec()))
        );
        assert_eq!(
            scan.next_back().transpose()?,
            Some((b"k3".to_vec(), b"v3".to_vec()))
        );
        assert_eq!(scan.next().transpose()?, None);
        std::mem::drop(scan);

        txn.commit()?;

        Ok(())
    }

    #[test]
    fn test_txn_scan_key_version_overlap() -> Result<()> {
        // 这里的想法是, 使用一个简单的 key/version 连接, 我们得到了重叠的条目, 这些条目混乱了扫描.
        // 例如:
        //
        // 00|00 00 00 00 00 00 00 01
        // 00 00 00 00 00 00 00 00 02|00 00 00 00 00 00 00 02
        // 00|00 00 00 00 00 00 00 03
        //
        // 键编码应该对此有抵抗力.

        let mvcc = setup();

        let mut txn = mvcc.begin()?;
        txn.set(&[0], vec![0])?; // v0
        txn.set(&[0], vec![1])?; // v1
        txn.set(&[0, 0, 0, 0, 0, 0, 0, 0, 2], vec![2])?; // v2
        txn.set(&[0], vec![3])?; // v3
        txn.commit()?;

        let txn = mvcc.begin()?;
        assert_eq!(
            txn.scan(..)?.collect::<Result<Vec<_>>>()?,
            vec![
                (vec![0], vec![3]),
                (vec![0, 0, 0, 0, 0, 0, 0, 0, 2], vec![2]),
            ]
        );

        Ok(())
    }

    #[test]
    fn test_txn_scan_prefix() -> Result<()> {
        let mvcc = setup();
        let mut txn = mvcc.begin()?;

        // 写入一些有相同前缀的键
        txn.set(b"k1", b"v1".to_vec())?;
        txn.set(b"k1a", b"v1a".to_vec())?;
        txn.set(b"k2", b"v2".to_vec())?;
        txn.set(b"k2a", b"v2a".to_vec())?;
        txn.set(b"k2b", b"v2b".to_vec())?;
        txn.set(b"k2c", b"v2c".to_vec())?;
        txn.set(b"k3", b"v3".to_vec())?;
        txn.commit()?;

        // 向前扫描
        let txn = mvcc.begin()?;
        assert_eq!(
            txn.scan_prefix(b"k2")?.collect::<Result<Vec<_>>>()?,
            vec![
                (b"k2".to_vec(), b"v2".to_vec()),
                (b"k2a".to_vec(), b"v2a".to_vec()),
                (b"k2b".to_vec(), b"v2b".to_vec()),
                (b"k2c".to_vec(), b"v2c".to_vec()),
            ]
        );

        // 向后扫描
        assert_eq!(
            txn.scan_prefix(b"k2")?.rev().collect::<Result<Vec<_>>>()?,
            vec![
                (b"k2c".to_vec(), b"v2c".to_vec()),
                (b"k2b".to_vec(), b"v2b".to_vec()),
                (b"k2a".to_vec(), b"v2a".to_vec()),
                (b"k2".to_vec(), b"v2".to_vec()),
            ]
        );

        // 前后交替
        let mut scan = txn.scan_prefix(b"k2")?;
        assert_eq!(
            scan.next().transpose()?,
            Some((b"k2".to_vec(), b"v2".to_vec()))
        );
        assert_eq!(
            scan.next_back().transpose()?,
            Some((b"k2c".to_vec(), b"v2c".to_vec()))
        );
        assert_eq!(
            scan.next_back().transpose()?,
            Some((b"k2b".to_vec(), b"v2b".to_vec()))
        );
        assert_eq!(
            scan.next().transpose()?,
            Some((b"k2a".to_vec(), b"v2a".to_vec()))
        );
        assert_eq!(scan.next().transpose()?, None);
        std::mem::drop(scan);

        txn.commit()?;

        Ok(())
    }

    #[test]
    fn test_txn_set_conflict() -> Result<()> {
        let mvcc = setup();

        let mut t1 = mvcc.begin()?;
        let mut t2 = mvcc.begin()?;
        let mut t3 = mvcc.begin()?;

        t2.set(b"k", b"v".to_vec())?;
        assert_eq!(t1.set(b"k", b"v".to_vec()), Err(Error::Serialization));
        assert_eq!(t3.set(b"k", b"v".to_vec()), Err(Error::Serialization));
        t2.commit()?;

        Ok(())
    }

    #[test]
    fn test_txn_set_conflict_committed() -> Result<()> {
        let mvcc = setup();

        let mut t1 = mvcc.begin()?;
        let mut t2 = mvcc.begin()?;
        let mut t3 = mvcc.begin()?;

        t2.set(b"k", b"v".to_vec())?;
        t2.commit()?;
        assert_eq!(t1.set(b"k", b"v".to_vec()), Err(Error::Serialization));
        assert_eq!(t3.set(b"k", b"v".to_vec()), Err(Error::Serialization));

        Ok(())
    }

    #[test]
    fn test_txn_set_rollback() -> Result<()> {
        let mvcc = setup();

        let mut txn = mvcc.begin()?;
        txn.set(b"k", b"v".to_vec())?;
        txn.commit()?;

        let t1 = mvcc.begin()?;
        let mut t2 = mvcc.begin()?;
        let mut t3 = mvcc.begin()?;

        t2.set(b"k", b"v2".to_vec())?;
        t2.rollback()?;
        assert_eq!(t1.get(b"k")?, Some(b"v".to_vec()));
        t1.commit()?;
        t3.set(b"k", b"v3".to_vec())?;
        t3.commit()?;

        Ok(())
    }

    // 脏写是指一个事务重写另一个事务写入而未提交的数据.
    // 这是不允许的, 因为它会导致未提交的数据丢失.
    #[test]
    fn test_txn_anomaly_dirty_write() -> Result<()> {
        let mvcc = setup();

        let mut t1 = mvcc.begin()?;
        let mut t2 = mvcc.begin()?;

        t1.set(b"k", b"v".to_vec())?;
        assert_eq!(t2.set(b"k", b"v2".to_vec()), Err(Error::Serialization));

        Ok(())
    }

    // 脏读是指一个事务读取到另一个事务写入但未提交的数据.
    #[test]
    fn test_txn_anomaly_dirty_read() -> Result<()> {
        let mvcc = setup();

        let mut t1 = mvcc.begin()?;
        let t2 = mvcc.begin()?;

        t1.set(b"k", b"v".to_vec())?;
        assert_eq!(t2.get(b"k")?, None);

        Ok(())
    }

    // 丢失更新是指两个事务都在读取并写入同一个键, 但是第二个事务的写入覆盖了第一个事务的写入.
    #[test]
    fn test_txn_anomaly_lost_update() -> Result<()> {
        let mvcc = setup();

        let mut t0 = mvcc.begin()?;
        t0.set(b"k", b"v0".to_vec())?;
        t0.commit()?;

        let mut t1 = mvcc.begin()?;
        let mut t2 = mvcc.begin()?;

        t1.get(b"k")?;
        t2.get(b"k")?;

        t1.set(b"k", b"v1".to_vec())?;
        assert_eq!(t2.set(b"k", b"v2".to_vec()), Err(Error::Serialization));

        Ok(())
    }

    // 模糊读(不可重复读)是指 t2 在 t1 更新一个值后读取到了该值的变化.
    #[test]
    fn test_txn_anomaly_fuzzy_read() -> Result<()> {
        let mvcc = setup();

        let mut t0 = mvcc.begin()?;
        t0.set(b"k", b"v0".to_vec())?;
        t0.commit()?;

        let mut t1 = mvcc.begin()?;
        let t2 = mvcc.begin()?;

        assert_eq!(t2.get(b"k")?, Some(b"v0".to_vec()));
        t1.set(b"k", b"v1".to_vec())?;
        t1.commit()?;
        assert_eq!(t2.get(b"k")?, Some(b"v0".to_vec()));

        Ok(())
    }

    // 读偏差是指 当 t1 在读取 a 和 b 时, t2 在这两个读取之间更新了 b.
    #[test]
    fn test_txn_anomaly_read_skew() -> Result<()> {
        let mvcc = setup();

        let mut t0 = mvcc.begin()?;
        t0.set(b"a", b"v0".to_vec())?;
        t0.set(b"b", b"v0".to_vec())?;
        t0.commit()?;

        let t1 = mvcc.begin()?;
        let mut t2 = mvcc.begin()?;

        assert_eq!(t1.get(b"a")?, Some(b"v0".to_vec()));
        t2.set(b"a", b"v1".to_vec())?;
        t2.set(b"b", b"v1".to_vec())?;
        t2.commit()?;
        assert_eq!(t1.get(b"b")?, Some(b"v0".to_vec()));

        Ok(())
    }

    // 幻读是指当 t1 在根据某些条件读取一组行时, t2 的修改导致条件匹配的行集发生变化.
    #[test]
    fn test_txn_anomaly_phantom_read() -> Result<()> {
        let mvcc = setup();

        let mut t0 = mvcc.begin()?;
        t0.set(b"a", b"v0".to_vec())?;
        t0.set(b"b", b"v1".to_vec())?;
        t0.commit()?;

        let t1 = mvcc.begin()?;
        let mut t2 = mvcc.begin()?;

        assert_eq!(t1.get(b"a")?, Some(b"v0".to_vec()));
        assert_eq!(t1.get(b"b")?, Some(b"v1".to_vec()));

        t2.set(b"b", b"v2".to_vec())?;
        t2.commit()?;

        assert_eq!(t1.get(b"a")?, Some(b"v0".to_vec()));
        assert_eq!(t1.get(b"b")?, Some(b"v1".to_vec()));

        Ok(())
    }

    #[test]
    fn test_metadata() -> Result<()> {
        let mvcc = setup();

        mvcc.set_metadata(b"foo", b"bar".to_vec())?;
        assert_eq!(mvcc.get_metadata(b"foo")?, Some(b"bar".to_vec()));

        assert_eq!(mvcc.get_metadata(b"baz")?, None);

        mvcc.set_metadata(b"foo", b"baz".to_vec())?;
        assert_eq!(mvcc.get_metadata(b"foo")?, Some(b"baz".to_vec()));

        Ok(())
    }
}
