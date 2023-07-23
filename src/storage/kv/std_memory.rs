use super::{Range, Scan, Store};
use crate::error::Result;

use std::collections::BTreeMap;
use std::fmt::Display;

/// In-memory key-value store using the standard library's BTreeMap.
pub struct StdMemory {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl StdMemory {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }
}

impl Display for StdMemory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(stdmemory)")
    }
}

impl Store for StdMemory {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.data.get(key).cloned())
    }

    fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.data.insert(key.to_vec(), value);
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.data.remove(key);
        Ok(())
    }

    fn scan(&self, range: Range) -> Scan {
        // 
        Box::new(
            self.data
                .range(range)
                .map(|(k, v)| Ok((k.clone(), v.clone())))
                .collect::<Vec<_>>()
                .into_iter(),
        )
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
impl super::TestSuite<StdMemory> for StdMemory {
    fn setup() -> Result<StdMemory> {
        Ok(StdMemory::new())
    }
}

#[test]
fn tests() -> Result<()> {
    use super::TestSuite;
    StdMemory::test()
}
