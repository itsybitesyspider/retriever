#[cfg(feature = "log")]
use log::warn;

use std::ops::{Index, IndexMut};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

static ID_COUNTER: AtomicU64 = AtomicU64::new(0);

const SCALE: usize = 0x10;
const STRIDE_0: usize = 0x10;
const STRIDE_1: usize = STRIDE_0 * SCALE;
const STRIDE_2: usize = STRIDE_1 * SCALE;
const STRIDE_3: usize = STRIDE_2 * SCALE;
const STRIDE_4: usize = STRIDE_3 * SCALE;

struct ChangedVec {
    count: u128,
    counts_0: Vec<u128>,
    counts_1: Vec<u128>,
    counts_2: Vec<u128>,
    counts_3: Vec<u128>,
    counts_4: Vec<u128>,
}

pub(crate) struct MrVec<T> {
    id: u64,
    parent_id: Option<u64>,
    parent_count: u128,
    data: Vec<T>,
    changed_vec: ChangedVec,
}

impl<T> MrVec<T> {
    /// Length of this MrVec. As Vec::len().
    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }

    /// Number of changes made to this MrVec.
    #[cfg(test)]
    pub(crate) fn change_count(&self) -> u128 {
        self.changed_vec.count
    }

    /// Touch an element of this MrVec, but index.
    pub(crate) fn touch(&mut self, i: usize) -> &mut Self {
        if i/STRIDE_0+1 > self.changed_vec.counts_0.len() {
          self.changed_vec.counts_0.resize(self.changed_vec.counts_0.len().max(i/STRIDE_0+1), 0);
          self.changed_vec.counts_1.resize(self.changed_vec.counts_1.len().max(i/STRIDE_1+1), 0);
          self.changed_vec.counts_2.resize(self.changed_vec.counts_2.len().max(i/STRIDE_2+1), 0);
          self.changed_vec.counts_3.resize(self.changed_vec.counts_3.len().max(i/STRIDE_3+1), 0);
          self.changed_vec.counts_4.resize(self.changed_vec.counts_4.len().max(i/STRIDE_4+1), 0);
        }

        self.changed_vec.count += 1;
        self.changed_vec.counts_0[i / STRIDE_0] = self.changed_vec.count;
        self.changed_vec.counts_1[i / STRIDE_1] = self.changed_vec.count;
        self.changed_vec.counts_2[i / STRIDE_2] = self.changed_vec.count;
        self.changed_vec.counts_3[i / STRIDE_3] = self.changed_vec.count;
        self.changed_vec.counts_4[i / STRIDE_4] = self.changed_vec.count;

        self
    }

    fn resize_touch(&mut self, new_size: usize) -> &mut Self
    where
        T: Default,
    {
        for i in self.data.len().min(new_size)..self.data.len().max(new_size) {
            self.touch(i);
        }

        self.data.resize_with(new_size, Default::default);

        self
    }

    /// Push a single element to this MrVec. As Vec::push(..).
    pub(crate) fn push(&mut self, t: T) {
        self.data.push(t);
        self.touch(self.data.len() - 1);
    }

    /// Swap and remove a single element from this MrVec. As Vec::swap_remove(..).
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

        *self = MrVec::from(vec![]);
    }

    fn validate_parent_id<S>(&mut self, source: &MrVec<S>) {
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

    pub(crate) fn map_reduce<S, Op>(&mut self, source: &MrVec<S>, group_size: usize, mut op: Op)
    where
        Op: FnMut(&[S], &T, usize) -> Option<T>,
        T: Default,
    {
        // Validate the parent/source Id, so we know we aren't map-reducing the wrong parent
        self.validate_parent_id(source);

        // If the parent/source is shorter than last time, we need to delete the extra
        // elements from the reduction/self MrVec and notify via the callback.
        {
            let old_size = self.data.len();
            let new_size = (source.data.len() + group_size - 1) / group_size;

            for i in new_size..old_size {
                let none = op(&[], &self.data[i], i);
                assert!(none.is_none());
            }

            self.resize_touch(new_size);
        }

        // Figure out how stale is our knowledge of the state of the parent/source
        // and which of the reduction/self elements need to be recalculated
        let expected_count = self.parent_count;
        let mut needs_recalc = Vec::new();
        let mut i = 0;

        while i < source.data.len() {
            if source.changed_vec.counts_4[i / STRIDE_4] <= expected_count {
                i = ((i / STRIDE_4) + 1) * STRIDE_4;
            } else if source.changed_vec.counts_3[i / STRIDE_3] <= expected_count {
                i = ((i / STRIDE_3) + 1) * STRIDE_3;
            } else if source.changed_vec.counts_2[i / STRIDE_2] <= expected_count {
                i = ((i / STRIDE_2) + 1) * STRIDE_2;
            } else if source.changed_vec.counts_1[i / STRIDE_1] <= expected_count {
                i = ((i / STRIDE_1) + 1) * STRIDE_1;
            } else if source.changed_vec.counts_0[i / STRIDE_0] > expected_count {
                needs_recalc.push(i / group_size);
                i = ((i / group_size) + 1) * group_size;
            } else {
                i += 1;
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

impl<T> Default for MrVec<T> {
    fn default() -> Self {
        Self::from(Vec::new())
    }
}

impl<T> Index<usize> for MrVec<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        &self.data[index]
    }
}

impl<T> IndexMut<usize> for MrVec<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.touch(index);
        &mut self.data[index]
    }
}

impl<T> From<Vec<T>> for MrVec<T> {
    fn from(data: Vec<T>) -> Self {
        let mut counts_0 = Vec::new();
        counts_0.resize(data.len() / STRIDE_0 + 1, 0);

        let mut counts_1 = Vec::new();
        counts_1.resize(data.len() / STRIDE_1 + 1, 0);

        let mut counts_2 = Vec::new();
        counts_2.resize(data.len() / STRIDE_2 + 1, 0);

        let mut counts_3 = Vec::new();
        counts_3.resize(data.len() / STRIDE_3 + 1, 0);

        let mut counts_4 = Vec::new();
        counts_4.resize(data.len() / STRIDE_4 + 1, 0);

        MrVec {
            id: ID_COUNTER.fetch_add(1, Ordering::Relaxed),
            data,
            parent_count: 0,
            parent_id: None,
            changed_vec: ChangedVec {
                count: 0,
                counts_0,
                counts_1,
                counts_2,
                counts_3,
                counts_4,
            },
        }
    }
}

impl<T> Into<Vec<T>> for MrVec<T> {
    fn into(self) -> Vec<T> {
        self.data
    }
}

impl<T> std::ops::Deref for MrVec<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        &self.data
    }
}

impl<T> Clone for MrVec<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        let mut result = MrVec::default();

        for e in self.data.iter() {
            result.push(e.clone());
        }

        result
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_len() {
        use super::*;

        let mut v = MrVec::default();
        v.push(0);
        v.push(1);
        v.push(2);
        assert_eq!(v.len(), 3);
    }

    #[test]
    fn test_change_count() {
        use super::*;

        let mut v = MrVec::default();
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

        let mut v = MrVec::default();
        v.push(1);
        v.push(2);
        v.push(3);

        let mut result = MrVec::default();
        result.map_reduce(&v, 100, |xs, _, _| Some(xs.iter().sum::<i32>()));

        assert_eq!(result[0], 6);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_map_reduce_small_sum_with_edit() {
        use super::*;

        let mut v = MrVec::default();
        v.push(1);
        v.push(2);
        v.push(3);

        let mut result = MrVec::default();
        result.map_reduce(&v, 100, |xs, _, _| Some(xs.iter().sum::<i32>()));

        assert_eq!(result[0], 6);
        assert_eq!(result.len(), 1);

        v[1] = 4;

        result.map_reduce(&v, 100, |xs, _, _| Some(xs.iter().sum::<i32>()));

        assert_eq!(result[0], 8);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_map_reduce_small_sum_with_removal() {
        use super::*;

        let mut v = MrVec::default();
        v.push(1);
        v.push(2);
        v.push(3);

        let mut result = MrVec::default();
        result.map_reduce(&v, 100, |xs, _, _| Some(xs.iter().sum::<i32>()));

        assert_eq!(result[0], 6);
        assert_eq!(result.len(), 1);

        v.swap_remove(2);

        result.map_reduce(&v, 100, |xs, _, _| Some(xs.iter().sum::<i32>()));

        assert_eq!(result[0], 3);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_map_reduce_with_two_layers() {
        use super::*;

        let mut v = MrVec::default();
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

        let mut layer_1 = MrVec::default();
        layer_1.map_reduce(&v, 2, |xs, _, _| {
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
        layer_1.map_reduce(&v, 2, |xs, _, _| {
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
        layer_1.map_reduce(&v, 2, |xs, _, _| {
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

        let mut v = MrVec::default();
        v.push(1);
        v.push(2);
        v.push(3);

        let mut result = MrVec::default();
        result.map_reduce(&v, 2, |xs, _, _| Some(xs.iter().sum::<i32>()));
        assert_eq!(3, result[0]);
        assert_eq!(3, result[1]);

        let mut w = MrVec::default();
        w.push(4);
        w.push(5);
        w.push(6);

        result.map_reduce(&w, 2, |xs, _, _| Some(xs.iter().sum::<i32>()));
        assert_eq!(9, result[0]);
        assert_eq!(6, result[1]);
    }
}
