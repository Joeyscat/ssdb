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

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.data.insert(key.to_vec(), value);
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.data.remove(key);
        Ok(())
    }

    fn scan(&self, range: Range) -> Scan {
        // 由于范围迭代器返回借用的项, 因此需要在迭代期间对其进行读取锁定. 这太粗糙了, 因此我们在这里缓冲整个迭代.
        // 相反, 应该使用带有arc-mutex的迭代器, 它能够通过再次抓取锁来恢复迭代.
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
