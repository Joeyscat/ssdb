use std::{
    fmt::Display,
    sync::{Arc, RwLock},
};

use super::{Range, Scan, StdMemory, Store};
use crate::error::Result;

/// Key-value storage backend for testing. Protects an inner StdMemory with a RwLock,
/// so it can be cloned and inspected.
pub struct Test {
    kv: Arc<RwLock<StdMemory>>,
}

#[test]
fn tests() -> Result<()> {
    use super::TestSuite;
    Test::test()
}

impl Test {
    fn new() -> Self {
        Self {
            kv: Arc::new(RwLock::new(StdMemory::new())),
        }
    }
}

impl Display for Test {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(test)")
    }
}

impl Store for Test {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.kv.read()?.get(key)
    }

    fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.kv.write()?.set(key, value)
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.kv.write()?.delete(key)
    }

    fn scan(&self, range: Range) -> Scan {
        Box::new(
            self.kv
                .read()
                .unwrap()
                .scan(range)
                .collect::<Vec<Result<_>>>()
                .into_iter(),
        )
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}
