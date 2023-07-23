use serde_derive::{Deserialize, Serialize};

use crate::error::Error;

use super::{Range, Store};

use std::sync::{Arc, RwLock};

/// MVCC status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Status {
    pub txns: u64,
    pub txns_active: u64,
    pub storage: String,
}

/// An MVCC-based transactional key-value store.
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

    /// Serialize MVCC metadata.
    fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>> {
        Ok(bincode::serialize(value)?)
    }

    /// Deserialize MVCC metadata.
    fn deserialize<'a, V: Deserialize<'a>>(value: &'a [u8]) -> Result<V> {
        Ok(bincode::deserialize(value)?)
    }
}

// An MCVC transaction
pub struct Transaction {
    /// The underlying store. It is protected by a mutex so it can be shared between txns.
    store: Arc<RwLock<Box<dyn Store>>>,
    /// The unique transaction ID
    id: u64,
    /// The transaction mode
    mode: Mode,
    /// The snapshot that the transaction is reading from
    snapshot: Snapshot,
}

impl Transaction {
    /// Begin a new transaction in the given mode
    pub fn begin(store: Arc<RwLock<Box<dyn Store>>>, mode: Mode) -> Result<Self> {
        let mut session = store.write()?;

        let id = match session.get(&Key::TxnNext.encode())? {
            Some(ref value) => deserialize(v)?,
            None => 1,
        };
        session.set(&Key::TxnNext.encode(), serialize(&(id + 1))?)?;
        session.set(&Key::TxnActive(id).encode(), serialize(&mode)?)?;

        // we always take a snapshot, even for snapshot transactions, because all transactions
        // increment the transaction ID counter, and we need to properly record the currently
        // active transactions for any future snapshot transactions looking at this one.
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

    /// Resume an active transaction with the given ID. Errors if the transaction is not active.
    fn resume(store: Arc<RwLock<Box<dyn Store>>>, id: u64) -> Result<Self> {
        let session = store.read()?;
        let mode = match session.get(&Key::TxnActive(id).encode())? {
            Some(value) => deserialize(&value)?,
            None => return Err(Error::Value(format!("transaction {} is not active", id))),
        };
        std::mem::drop(session);

        let snapshot = match &mode {
            Mode::Snapshot { version } => Snapshot::restore(&session, *version)?,
            _ => Snapshot::restore(&session, id)?,
        };

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
            while let Some((key, _)) = scan.next().transpose() {
                match Key::decode(&key?)? {
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
                Key::Record(key.into(), 0).encode()..Key::Record(key.into(), self.id).encode(),
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
}
