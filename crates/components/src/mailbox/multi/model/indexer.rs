use std::{cmp::Ordering, ops::{Deref, DerefMut}, collections::BinaryHeap};

use serde::Serialize;

pub type RefIndex<'a> = &'a [u8];

/// Abstract unique operation, all impls must has unique index.
/// e.g. a key-value storage has unique key of each entry, no matter
/// put or get, the unique key must be assigned, so that we can 
/// find which group prepare to write (or read).
pub trait Unique: Serialize {
    fn get_index(&self) -> RefIndex;
}

/// Wrapped entry, using to wrap actual entry with comparator.
pub struct Entry<T: Unique>(pub T);

impl<T: Unique + Default> Entry<T> {
    #[inline]
    pub fn take(mut self) -> T {
        std::mem::take(&mut self.0)
    }
}

impl<T: Unique> PartialOrd for Entry<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let idx1 = self.0.get_index();
        let idx2 = other.0.get_index();
        idx2.partial_cmp(idx1)
    }
}

impl<T: Unique> Ord for Entry<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        let oid = other.0.get_index();
        oid.cmp(self.0.get_index())
    }
}

impl<T: Unique> PartialEq for Entry<T>  {
    fn eq(&self, other: &Self) -> bool {
        self.0.get_index() == other.0.get_index()
    }
}

impl<T: Unique> Eq for Entry<T>  {}

impl<T: Unique> Deref for Entry<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Unique> DerefMut for Entry<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// e.g. [1,2,3,4,5] in router: { 3: group_[0, 3), 5: [3,6)) }
/// then aggregate [1,2] to 3 (mapped group), [3,4,5] to 5
pub struct WriteBatch<T: Unique> {
    pub entries: BinaryHeap<Entry<T>>
}

impl<E: Unique> Deref for WriteBatch<E> {
    type Target = BinaryHeap<Entry<E>>;

    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}

impl<E: Unique> DerefMut for WriteBatch<E> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.entries
    }
}

impl<E: Unique> WriteBatch<E> {
    pub fn new() -> Self {
        Self {
            entries: BinaryHeap::default(),
        }
    }

    pub fn insert(&mut self, entry: E) {
        self.entries.push(Entry(entry));
    }

    #[inline]
    pub fn first_index(&self) -> Option<RefIndex> {
        self.entries.peek().map(|f| f.get_index())
    }

    #[inline]
    pub fn last_index(&self) -> Option<RefIndex> {
        self.entries.iter().last().map(|e| e.get_index())
    }
}

impl<E: Unique + Default> WriteBatch<E> {
    pub fn pop(&mut self) -> Option<E> {
        let ent = self.entries.pop()?;
        Some(ent.take())
    }
}

impl<T: Unique> From<Vec<T>> for WriteBatch<T> {
    fn from(ori: Vec<T>) -> Self {
        let mut entries = BinaryHeap::new();
        for v in ori {
            entries.push(Entry(v));
        }
        Self {
            entries
        }
    }
}