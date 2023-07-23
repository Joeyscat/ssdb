pub mod encoding;
pub mod mvcc;
mod std_memory;
#[cfg(test)]
mod test;

pub use mvcc::MVCC;
pub use std_memory::StdMemory;
#[cfg(test)]
pub use test::Test;

use crate::error::Result;
use std::{
    fmt::Display,
    ops::{Bound, RangeBounds},
};

pub trait Store: Display + Send + Sync {
    /// Get a value by key, or None if it doesn't exist.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Set a value for a key. Overwrites any existing value.
    fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()>;

    /// Delete a key, or do nothing if it doesn't exist.
    fn delete(&self, key: &[u8]) -> Result<()>;

    /// Iterate over an ordered range of key-value pairs.
    fn scan(&self, range: Range) -> Scan;

    /// Flush any buffered data to the underlying storage.
    fn flush(&mut self) -> Result<()>;
}

/// A scan range
pub struct Range {
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
}

impl Range {
    /// Create a new range from the given RUST range. We can't use the RangeBounds directly
    /// in `scan()` since that prevents us from using `Store` as a trait object.
    /// Also, we can't take `AsRef<u8>` or other convenient types, since that won't work
    /// with e.g. `..` range syntax.
    pub fn from<R: RangeBounds<Vec<u8>>>(range: R) -> Self {
        Self {
            start: match range.start_bound() {
                Bound::Included(key) => Bound::Included(key.to_vec()),
                Bound::Excluded(key) => Bound::Excluded(key.to_vec()),
                Bound::Unbounded => Bound::Unbounded,
            },
            end: match range.end_bound() {
                Bound::Included(key) => Bound::Included(key.to_vec()),
                Bound::Excluded(key) => Bound::Excluded(key.to_vec()),
                Bound::Unbounded => Bound::Unbounded,
            },
        }
    }

    /// Check if a value is within the range
    fn contains(&self, v: &[u8]) -> bool {
        (match &self.start {
            Bound::Included(start) => &**start <= v,
            Bound::Excluded(start) => &**start < v,
            Bound::Unbounded => true,
        }) && (match &self.end {
            Bound::Included(end) => v <= &**end,
            Bound::Excluded(end) => v < &**end,
            Bound::Unbounded => true,
        })
    }
}

impl RangeBounds<Vec<u8>> for Range {
    fn start_bound(&self) -> Bound<&Vec<u8>> {
        match &self.start {
            Bound::Included(v) => Bound::Included(v),
            Bound::Excluded(v) => Bound::Excluded(v),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    fn end_bound(&self) -> Bound<&Vec<u8>> {
        match &self.end {
            Bound::Included(v) => Bound::Included(v),
            Bound::Excluded(v) => Bound::Excluded(v),
            Bound::Unbounded => Bound::Unbounded,
        }
    }
}

/// Iterator over an ordered range of key-value pairs.
pub type Scan = Box<dyn DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> + Send>;

#[cfg(test)]
trait TestSuite<S: Store> {
    fn setup() -> Result<S>;

    fn test() -> Result<()> {
        Self::test_get()?;
        Self::test_set()?;
        Self::test_delete()?;
        Self::test_scan()?;
        Self::test_random()?;
        Ok(())
    }

    fn test_get() -> Result<()> {
        let store = Self::setup()?;
        assert_eq!(store.get(b"foo")?, None);
        store.set(b"foo", b"bar".to_vec())?;
        assert_eq!(store.get(b"foo")?, Some(b"bar".to_vec()));
        assert_eq!(store.get(b"foox")?, None);
        Ok(())
    }

    fn test_set() -> Result<()> {
        let store = Self::setup()?;
        store.set(b"foo", b"bar".to_vec())?;
        assert_eq!(store.get(b"foo")?, Some(b"bar".to_vec()));
        store.set(b"foo", b"baz".to_vec())?;
        assert_eq!(store.get(b"foo")?, Some(b"baz".to_vec()));
        Ok(())
    }

    fn test_delete() -> Result<()> {
        let store = Self::setup()?;
        store.set(b"foo", b"bar".to_vec())?;
        assert_eq!(store.get(b"foo")?, Some(b"bar".to_vec()));
        store.delete(b"foo")?;
        assert_eq!(store.get(b"foo")?, None);
        store.delete(b"foo")?;
        Ok(())
    }

    fn test_scan() -> Result<()> {
        let store = Self::setup()?;
        store.set(b"foo", b"bar".to_vec())?;
        store.set(b"foo1", b"bar1".to_vec())?;
        store.set(b"foo2", b"bar2".to_vec())?;
        store.set(b"foo3", b"bar3".to_vec())?;
        store.set(b"foo4", b"bar4".to_vec())?;

        // forward/backward ranges
        assert_eq!(
            store
                .scan(Range::from(b"foo1".to_vec()..b"foo6".to_vec()))
                .collect::<Result<Vec<_>>>()?,
            vec![
                (b"foo1".to_vec(), b"bar1".to_vec()),
                (b"foo2".to_vec(), b"bar2".to_vec()),
                (b"foo3".to_vec(), b"bar3".to_vec()),
                (b"foo4".to_vec(), b"bar4".to_vec()),
            ]
        );
        assert_eq!(
            store
                .scan(Range::from(b"foo1".to_vec()..b"foo6".to_vec()))
                .rev()
                .collect::<Result<Vec<_>>>()?,
            vec![
                (b"foo4".to_vec(), b"bar4".to_vec()),
                (b"foo3".to_vec(), b"bar3".to_vec()),
                (b"foo2".to_vec(), b"bar2".to_vec()),
                (b"foo1".to_vec(), b"bar1".to_vec()),
            ]
        );

        // inclusive/exclusive ranges
        assert_eq!(
            store
                .scan(Range::from(b"foo1".to_vec()..b"foo2".to_vec()))
                .collect::<Result<Vec<_>>>()?,
            vec![(b"foo1".to_vec(), b"bar1".to_vec())]
        );
        assert_eq!(
            store
                .scan(Range::from(b"foo1".to_vec()..=b"foo3".to_vec()))
                .collect::<Result<Vec<_>>>()?,
            vec![
                (b"foo1".to_vec(), b"bar1".to_vec()),
                (b"foo2".to_vec(), b"bar2".to_vec()),
                (b"foo3".to_vec(), b"bar3".to_vec()),
            ]
        );

        // unbounded ranges
        assert_eq!(
            store
                .scan(Range::from(..b"foo2".to_vec()))
                .collect::<Result<Vec<_>>>()?,
            vec![
                (b"foo".to_vec(), b"bar".to_vec()),
                (b"foo1".to_vec(), b"bar1".to_vec()),
            ]
        );
        assert_eq!(
            store
                .scan(Range::from(b"foo2".to_vec()..))
                .collect::<Result<Vec<_>>>()?,
            vec![
                (b"foo3".to_vec(), b"bar3".to_vec()),
                (b"foo4".to_vec(), b"bar4".to_vec()),
            ]
        );

        // full range
        assert_eq!(
            store.scan(Range::from(..)).collect::<Result<Vec<_>>>()?,
            vec![
                (b"foo".to_vec(), b"bar".to_vec()),
                (b"foo1".to_vec(), b"bar1".to_vec()),
                (b"foo2".to_vec(), b"bar2".to_vec()),
                (b"foo3".to_vec(), b"bar3".to_vec()),
                (b"foo4".to_vec(), b"bar4".to_vec()),
            ]
        );

        // empty ranges
        assert_eq!(
            store
                .scan(Range::from(b"foo2".to_vec()..b"foo2".to_vec()))
                .collect::<Result<Vec<_>>>()?,
            vec![]
        );

        Ok(())
    }

    fn test_random() -> Result<()> {
        use rand::Rng;
        let mut store = Self::setup()?;
        let mut rng: rand::rngs::StdRng = rand::SeedableRng::seed_from_u64(397_427_893);

        // create a bunch of random items and store them
        let mut items = Vec::new();
        for i in 0..1000_u64 {
            items.push((rng.gen::<[u8; 32]>().to_vec(), i.to_be_bytes().to_vec()));
        }
        for (k, v) in items.iter() {
            store.set(k, v.clone())?;
        }

        // fetch the random items, both via scan and get
        for (k, v) in items.iter() {
            assert_eq!(store.get(k)?, Some(v.clone()));
        }

        let mut expect = items.clone();
        expect.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            expect,
            store.scan(Range::from(..)).collect::<Result<Vec<_>>>()?
        );
        expect.reverse();
        assert_eq!(
            expect,
            store
                .scan(Range::from(..))
                .rev()
                .collect::<Result<Vec<_>>>()?
        );

        // delete the random items
        for (k, _) in items.iter() {
            store.delete(k)?;
            assert_eq!(store.get(k)?, None);
        }
        assert!(store
            .scan(Range::from(..))
            .collect::<Result<Vec<_>>>()?
            .is_empty());

        Ok(())
    }
}
