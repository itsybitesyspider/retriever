use std::collections::HashMap;
use std::hash::{BuildHasher, Hash};

/// A measurement of the memory allocated -vs- used.
///
/// The meaning of these fields is the same as for a standard Vec.
#[derive(Clone, Copy, Debug)]
pub struct MemoryUsage {
    /// The size of each element of this collection. Not well defined for every collection.
    pub size_of: Option<usize>,

    /// The number of elements in this collection.
    pub len: usize,

    /// The allocated capacity of this collection.
    pub capacity: usize,
}

/// Trait implemented by collections that can use and release memory.
pub trait MemoryUser {
    /// Measure the memory used by this collection.
    fn memory_usage(&self) -> MemoryUsage;

    /// Shrink this collection. Since retriever is a composite of many collections,
    /// this method accepts a predicate to determine whether to shrink any particular
    /// component, and if so, how much capacity should remain after shrinking.
    fn shrink_with<F: Fn(&MemoryUsage) -> Option<usize>>(&mut self, f: F);

    /// A default strategy to shrink a collection when the unused allocation exceeds the
    /// the used allocation by the given ratio.
    fn shrink_by_ratio(&mut self, ratio: usize) {
        self.shrink_with(|usage| {
            let excess = usage.capacity - usage.len;

            if excess > usage.len * ratio {
                Some(usage.len * ratio)
            } else {
                None
            }
        });
    }

    /// A good default shrink operation.
    fn shrink(&mut self) {
        self.shrink_by_ratio(4);
    }
}

impl<T> MemoryUser for Vec<T> {
    fn memory_usage(&self) -> MemoryUsage {
        MemoryUsage {
            size_of: Some(std::mem::size_of::<T>()),
            len: self.len(),
            capacity: self.capacity(),
        }
    }

    fn shrink_with<F: Fn(&MemoryUsage) -> Option<usize>>(&mut self, f: F) {
        if let Some(_min_capacity) = f(&self.memory_usage()) {
            // TODO: use shrink_to, currently unstable
            self.shrink_to_fit();
        }
    }
}

impl<K: Eq + Hash, V, S: BuildHasher> MemoryUser for HashMap<K, V, S> {
    fn memory_usage(&self) -> MemoryUsage {
        MemoryUsage {
            size_of: Some(std::mem::size_of::<K>() + std::mem::size_of::<V>()),
            len: self.len(),
            capacity: self.capacity(),
        }
    }

    fn shrink_with<F: Fn(&MemoryUsage) -> Option<usize>>(&mut self, f: F) {
        if let Some(_min_capacity) = f(&self.memory_usage()) {
            self.shrink_to_fit();
        }
    }
}

impl MemoryUsage {
    /// Merge two memory usages into a total of both.
    pub fn merge(a: MemoryUsage, b: MemoryUsage) -> MemoryUsage {
        MemoryUsage {
            size_of: if a.size_of == b.size_of {
                a.size_of
            } else {
                None
            },
            len: a.len + b.len,
            capacity: a.capacity + b.capacity,
        }
    }
}
