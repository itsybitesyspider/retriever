#[cfg(feature = "log")]
use log::warn;

use crate::traits::memory_usage::{MemoryUsage, MemoryUser};
use std::ops::{Index, IndexMut};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

static ID_COUNTER: AtomicU64 = AtomicU64::new(0);

const SCALE: usize = 0x10;
const STRIDE: [usize; 5] = [
    SCALE,
    SCALE * SCALE,
    SCALE * SCALE * SCALE,
    SCALE * SCALE * SCALE * SCALE,
    SCALE * SCALE * SCALE * SCALE * SCALE,
];

struct ChangedVec {
    count: u128,
    counts: [Vec<u128>; 5],
}

pub(crate) struct RVec<T> {
    id: u64,
    parent_id: Option<u64>,
    parent_count: u128,
    data: Vec<T>,
    changed_vec: ChangedVec,
}

impl<T> RVec<T> {
    /// Length of this RVec. As Vec::len().
    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }

    /// Number of changes made to this RVec.
    #[cfg(test)]
    pub(crate) fn change_count(&self) -> u128 {
        self.changed_vec.count
    }

    /// Touch an element of this RVec, but index.
    pub(crate) fn touch(&mut self, i: usize) -> &mut Self {
        if i / STRIDE[0] + 1 > self.changed_vec.counts[0].len() {
            for (j, stride) in STRIDE.iter().enumerate() {
                self.changed_vec.counts[j]
                    .resize(self.changed_vec.counts[j].len().max(i / stride + 1), 0);
            }
        }

        self.changed_vec.count += 1;
        self.changed_vec.counts[0][i / STRIDE[0]] = self.changed_vec.count;
        self.changed_vec.counts[1][i / STRIDE[1]] = self.changed_vec.count;
        self.changed_vec.counts[2][i / STRIDE[2]] = self.changed_vec.count;
        self.changed_vec.counts[3][i / STRIDE[3]] = self.changed_vec.count;
        self.changed_vec.counts[4][i / STRIDE[4]] = self.changed_vec.count;

        self
    }

    fn resize_touch(&mut self, new_size: usize) -> &mut Self
    where
        T: Default,
    {
        resize_to_fit(&mut self.changed_vec.counts, new_size);

        self.data.resize_with(new_size, Default::default);
        for i in self.data.len()..new_size {
            self.touch(i);
        }

        self
    }

    /// Push a single element to this RVec. As Vec::push(..).
    pub(crate) fn push(&mut self, t: T) {
        self.data.push(t);
        self.touch(self.data.len() - 1);
    }

    /// Swap and remove a single element from this RVec. As Vec::swap_remove(..).
    pub(crate) fn swap_remove(&mut self, i: usize) -> T {
        self.touch(self.data.len() - 1);
        self.touch(i);
        self.data.swap_remove(i)
    }

    fn reset(&mut self) {
        #[cfg(feature = "log")]
        {
            let warning_msg = "Retriever: Forced to reset a reduction vector. This sometimes happens in normal usage, but frequent resets may represent incorrect usage and result in poor performance.";
            warn!("{}", warning_msg);
        }

        *self = RVec::from(vec![]);
    }

    fn validate_parent_id<S>(&mut self, source: &RVec<S>) {
        if let Some(parent_id) = self.parent_id {
            if parent_id != source.id {
                self.reset();
            }
        }

        if self.parent_id.is_none() {
            self.parent_id = Some(source.id);
        }

        assert_eq!(self.parent_id, Some(source.id));
    }

    pub(crate) fn reduce<S, Op>(&mut self, source: &RVec<S>, group_size: usize, mut op: Op)
    where
        Op: FnMut(&[S], &T, usize) -> Option<T>,
        T: Default,
    {
        // Validate the parent (source) Id, so we know we aren't reducing the wrong parent
        self.validate_parent_id(source);

        // If the parent (source) is shorter than last time, we need to delete the extra
        // elements from the reduction (self) RVec and notify via the callback.
        {
            let old_size = self.data.len();
            let new_size = (source.data.len() + group_size - 1) / group_size;

            for i in new_size..old_size {
                let none = op(&[], &self.data[i], i);
                assert!(none.is_none());
            }

            self.resize_touch(new_size);
        }

        // Figure out how stale is our knowledge of the state of the parent (source)
        // and which of the reduction (self) elements need to be recalculated
        let expected_count = self.parent_count;
        let mut needs_recalc = Vec::new();
        let mut i = 0;

        // I suspect this is a strong candidate for futher optimization.
        // Should consider a stack of nested loops to avoid redundant checks of stride counts.
        while i < source.data.len() {
            if source.changed_vec.counts[4][i / STRIDE[4]] <= expected_count {
                i = ((i / STRIDE[4]) + 1) * STRIDE[4];
            } else if source.changed_vec.counts[3][i / STRIDE[3]] <= expected_count {
                i = ((i / STRIDE[3]) + 1) * STRIDE[3];
            } else if source.changed_vec.counts[2][i / STRIDE[2]] <= expected_count {
                i = ((i / STRIDE[2]) + 1) * STRIDE[2];
            } else if source.changed_vec.counts[1][i / STRIDE[1]] <= expected_count {
                i = ((i / STRIDE[1]) + 1) * STRIDE[1];
            } else if source.changed_vec.counts[0][i / STRIDE[0]] > expected_count {
                let stop = ((i / STRIDE[0]) + 1) * STRIDE[0];
                while i < stop {
                    needs_recalc.push(i / group_size);
                    i = ((i / group_size) + 1) * group_size;
                }
            } else {
                let stop = ((i / STRIDE[0]) + 1) * STRIDE[0];
                while i < stop {
                    i += 1;
                }
            }
        }

        // Perform the updates
        let source_length = source.data.len();
        let dest_length = self.data.len();
        for i in needs_recalc.into_iter().filter(|i| *i < dest_length) {
            if let Some(replacement) = op(
                &source.data[i * group_size..((i + 1) * group_size).min(source_length)],
                &self.data[i],
                i,
            ) {
                self[i] = replacement;
            }
        }

        self.parent_count = source.changed_vec.count;
    }
}

impl<T> Default for RVec<T> {
    fn default() -> Self {
        Self::from(Vec::new())
    }
}

impl<T> Index<usize> for RVec<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        &self.data[index]
    }
}

impl<T> IndexMut<usize> for RVec<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.touch(index);
        &mut self.data[index]
    }
}

impl<T> From<Vec<T>> for RVec<T> {
    fn from(data: Vec<T>) -> Self {
        let mut counts: [Vec<u128>; 5] =
            [Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()];
        resize_to_fit(&mut counts, data.len());

        RVec {
            id: ID_COUNTER.fetch_add(1, Ordering::Relaxed),
            data,
            parent_count: 0,
            parent_id: None,
            changed_vec: ChangedVec { count: 0, counts },
        }
    }
}

/// Resize the hierarchical change count vectors to fit the size of the data.
fn resize_to_fit(counts: &mut [Vec<u128>; 5], len: usize) {
    counts[0].resize(len / STRIDE[0] + 1, 0);
    counts[1].resize(len / STRIDE[1] + 1, 0);
    counts[2].resize(len / STRIDE[2] + 1, 0);
    counts[3].resize(len / STRIDE[3] + 1, 0);
    counts[4].resize(len / STRIDE[4] + 1, 0);
}

impl<T> Into<Vec<T>> for RVec<T> {
    fn into(self) -> Vec<T> {
        self.data
    }
}

impl<T> std::ops::Deref for RVec<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        &self.data
    }
}

impl<T> Clone for RVec<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        let mut result = RVec::default();

        for e in self.data.iter() {
            result.push(e.clone());
        }

        result
    }
}

impl MemoryUser for ChangedVec {
    fn memory_usage(&self) -> MemoryUsage {
        MemoryUsage {
            size_of: Some(std::mem::size_of::<u128>()),
            len: self.counts.iter().map(|v| v.len()).sum::<usize>(),
            capacity: self.counts.iter().map(|v| v.capacity()).sum::<usize>(),
        }
    }

    fn shrink_with<F: Fn(&MemoryUsage) -> Option<usize>>(&mut self, f: F) {
        for count in self.counts.iter_mut() {
            count.shrink_with(&f);
        }
    }
}

impl<T> MemoryUser for RVec<T> {
    fn memory_usage(&self) -> MemoryUsage {
        let changed_vec = self.changed_vec.memory_usage();
        let data = self.data.memory_usage();

        MemoryUsage {
            size_of: None,
            len: changed_vec.len + data.len,
            capacity: changed_vec.capacity + data.capacity,
        }
    }

    fn shrink_with<F: Fn(&MemoryUsage) -> Option<usize>>(&mut self, f: F) {
        self.changed_vec.shrink_with(&f);
        self.data.shrink_with(&f);
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_len() {
        use super::*;

        let mut v = RVec::default();
        v.push(0);
        v.push(1);
        v.push(2);
        assert_eq!(v.len(), 3);
    }

    #[test]
    fn test_change_count() {
        use super::*;

        let mut v = RVec::default();
        v.push(1);
        v.push(2);
        v.push(3);
        v[0] = 4;
        v[2] = 5;
        v[1] = 6;
        v.swap_remove(1);
        v.swap_remove(1);

        assert_eq!(v.change_count(), 10);
    }

    #[test]
    fn test_map_reduce_small_sum() {
        use super::*;

        let mut v = RVec::default();
        v.push(1);
        v.push(2);
        v.push(3);

        let mut result = RVec::default();
        result.reduce(&v, 100, |xs, _, _| Some(xs.iter().sum::<i32>()));

        assert_eq!(result[0], 6);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_map_reduce_small_sum_with_edit() {
        use super::*;

        let mut v = RVec::default();
        v.push(1);
        v.push(2);
        v.push(3);

        let mut result = RVec::default();
        result.reduce(&v, 100, |xs, _, _| Some(xs.iter().sum::<i32>()));

        assert_eq!(result[0], 6);
        assert_eq!(result.len(), 1);

        v[1] = 4;

        result.reduce(&v, 100, |xs, _, _| Some(xs.iter().sum::<i32>()));

        assert_eq!(result[0], 8);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_map_reduce_small_sum_with_removal() {
        use super::*;

        let mut v = RVec::default();
        v.push(1);
        v.push(2);
        v.push(3);

        let mut result = RVec::default();
        result.reduce(&v, 100, |xs, _, _| Some(xs.iter().sum::<i32>()));

        assert_eq!(result[0], 6);
        assert_eq!(result.len(), 1);

        v.swap_remove(2);

        result.reduce(&v, 100, |xs, _, _| Some(xs.iter().sum::<i32>()));

        assert_eq!(result[0], 3);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_map_reduce_with_two_layers() {
        use super::*;

        let mut v = RVec::default();
        v.push(1);
        v.push(2);
        v.push(3);
        v.push(4);
        v.push(5);
        v.push(6);
        v.push(7);
        v.push(8);
        v.push(9);
        v.push(10);

        let mut layer_1 = RVec::default();
        layer_1.reduce(&v, 2, |xs, _, _| {
            if xs.is_empty() {
                None
            } else {
                Some(xs.iter().sum::<i32>())
            }
        });

        assert_eq!(layer_1[0], 3);
        assert_eq!(layer_1[1], 7);
        assert_eq!(layer_1[2], 11);
        assert_eq!(layer_1[3], 15);
        assert_eq!(layer_1[4], 19);
        assert_eq!(layer_1.len(), 5);

        v.swap_remove(3);
        layer_1.reduce(&v, 2, |xs, _, _| {
            if xs.is_empty() {
                None
            } else {
                Some(xs.iter().sum::<i32>())
            }
        });

        assert_eq!(layer_1[0], 3);
        assert_eq!(layer_1[1], 13);
        assert_eq!(layer_1[2], 11);
        assert_eq!(layer_1[3], 15);
        assert_eq!(layer_1[4], 9);
        assert_eq!(layer_1.len(), 5);

        v.swap_remove(5);
        layer_1.reduce(&v, 2, |xs, _, _| {
            if xs.is_empty() {
                None
            } else {
                Some(xs.iter().sum::<i32>())
            }
        });

        assert_eq!(layer_1[0], 3);
        assert_eq!(layer_1[1], 13);
        assert_eq!(layer_1[2], 14);
        assert_eq!(layer_1[3], 15);
        assert_eq!(layer_1.len(), 4);
    }

    #[test]
    fn test_map_reduce_with_changing_source_should_no_longer_panic() {
        use super::*;

        #[cfg(feature = "log")]
        let _ = simple_logger::init();

        let mut v = RVec::default();
        v.push(1);
        v.push(2);
        v.push(3);

        let mut result = RVec::default();
        result.reduce(&v, 2, |xs, _, _| Some(xs.iter().sum::<i32>()));
        assert_eq!(3, result[0]);
        assert_eq!(3, result[1]);

        let mut w = RVec::default();
        w.push(4);
        w.push(5);
        w.push(6);

        result.reduce(&w, 2, |xs, _, _| Some(xs.iter().sum::<i32>()));
        assert_eq!(9, result[0]);
        assert_eq!(6, result[1]);
    }
}
