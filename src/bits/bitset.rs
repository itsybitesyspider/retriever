use super::bitfield::*;
use crate::traits::idxset::IdxSet;
use crate::traits::memory_usage::{MemoryUsage, MemoryUser};
use std::iter::Filter;
use std::iter::FromIterator;
use std::sync::Arc;

/// A sparse bitset.
///
/// Why use a bespoke bitset implementation? This implementation has some qualities not easily obtained elsewhere:
///
/// * O(1) clone and non-borrowing iterator,
/// * We can iterate over the bitset at the block level, perform block-level intersections, unions, etc, even with other types that are not necessarily bitsets.
/// * Utility methods can shove bitfield data into a stack-allocated fixed-size array, slice, or SmallVec.
/// * We have complete freedom to make further low-level optimizations according to our needs
/// * Some of the benefits of other implementations (such as run-length encoding) are moderated by the fact that our bitsets correspond to other structures that are already using O(n) memory anyway.
/// * As reluctant as I was to do it, maintaining this implementation has not proven particularly onerous,
///   while the appealing properties listed above have proven particularly beneficial.
#[derive(Clone)]
pub struct Bitset {
    bits: Arc<Vec<Bitfield>>,
}

/// An iterator over a `Bitset`.
pub struct BitsetIter {
    bits: Arc<Vec<Bitfield>>,
    front: usize,
    back: usize,
}

impl Bitset {
    /// Construct a new empty bitset
    pub fn new() -> Self {
        Self::default()
    }

    /// True if there are no bits set
    pub fn is_empty(&self) -> bool {
        self.bits.len() == 0
    }

    /// The number of bits set
    pub fn len(&self) -> usize {
        self.bits.iter().map(|b| b.ones()).sum::<usize>()
    }

    /// Set the specific bit position in this Bitset
    pub fn set(&mut self, i: usize) {
        match self
            .bits
            .binary_search_by_key(&start_of(i), Bitfield::sort_order)
        {
            Ok(bidx) => {
                Arc::make_mut(&mut self.bits)[bidx].set(i);
            }
            Err(bidx) => {
                Arc::make_mut(&mut self.bits).insert(bidx, Bitfield::new(i));
            }
        }
    }

    /// Set the specific bit position in this Bitset
    pub fn unset(&mut self, i: usize) {
        if let Ok(bidx) = self
            .bits
            .binary_search_by_key(&start_of(i), Bitfield::sort_order)
        {
            Arc::make_mut(&mut self.bits)[bidx].unset(i);
        }
    }

    /// Set the specific bit position in this Bitset
    pub fn get(&self, i: usize) -> bool {
        match self
            .bits
            .binary_search_by_key(&start_of(i), Bitfield::sort_order)
        {
            Ok(bidx) => self.bits[bidx].get(i),
            Err(_) => false,
        }
    }

    /// Set the specific bit position within a slice of Bitfields. If this succeeds,
    /// the result is Some(()). It is possible that this will fail because there is
    /// not enought room in the slice, in which case the result will be none.
    #[must_use = "this can fail"]
    pub fn set_in_slice(slice: &mut [Bitfield], i: usize) -> Option<()> {
        match slice.binary_search_by_key(&start_of(i), Bitfield::sort_order) {
            Ok(bidx) => {
                slice[bidx].set(i);
                Some(())
            }
            Err(_) if !slice[slice.len() - 1].valid() => {
                slice[slice.len() - 1] = Bitfield::new(i);
                slice.sort_unstable();
                Some(())
            }
            Err(_) => None,
        }
    }

    /// Set the specific bit position in this Bitset
    pub fn unset_in_slice(slice: &mut [Bitfield], i: usize) {
        if let Ok(bidx) = slice.binary_search_by_key(&start_of(i), Bitfield::sort_order) {
            slice[bidx].unset(i);
            if slice[bidx].bits == 0b0 {
                slice[bidx] = Bitfield::default();
                slice.sort_unstable();
            }
        }
    }

    /// Set the specific bit position in this Bitset
    pub fn get_in_slice(slice: &[Bitfield], i: usize) -> bool {
        match slice.binary_search_by_key(&start_of(i), Bitfield::sort_order) {
            Ok(bidx) => slice[bidx].get(i),
            Err(_) => false,
        }
    }

    /// Set the specific bit position in this Bitset
    pub fn intersect_in_slice(slice: &[Bitfield], bits: &Bitfield) -> Bitfield {
        match slice.binary_search_by_key(&start_of(bits.start()), Bitfield::sort_order) {
            Ok(bidx) => slice[bidx].intersect(&bits),
            Err(_) => Bitfield::new_empty(bits.start()),
        }
    }

    /// Iterate over all Bitfields in this Bitset.
    ///
    /// You might don't want the Bitfield items themselves. To get at the actual bit indices
    /// (usize values), use flatten().
    ///
    /// ```
    /// # use retriever::bits::Bitset;
    /// # let mut bitset = Bitset::new();
    /// bitset.set(17);
    ///
    /// for idx in bitset.iter().flatten() {
    ///   assert_eq!(17, idx);
    /// }
    /// ```
    pub fn iter(&self) -> <Self as IntoIterator>::IntoIter {
        self.clone().into_iter()
    }
}

impl Default for Bitset {
    fn default() -> Self {
        Bitset {
            bits: Arc::new(Vec::new()),
        }
    }
}

impl Iterator for BitsetIter {
    type Item = Bitfield;

    fn next(&mut self) -> Option<Bitfield> {
        if self.front < self.back {
            let result = self.bits[self.front];
            self.front += 1;
            Some(result)
        } else {
            None
        }
    }
}

impl DoubleEndedIterator for BitsetIter {
    fn next_back(&mut self) -> Option<Bitfield> {
        if self.front < self.back {
            self.back -= 1;
            let result = self.bits[self.back];
            Some(result)
        } else {
            None
        }
    }
}

impl IntoIterator for Bitset {
    type Item = Bitfield;
    type IntoIter = BitsetIter;

    fn into_iter(self) -> Self::IntoIter {
        let front = 0;
        let back = self.bits.len();

        BitsetIter {
            bits: self.bits,
            front,
            back,
        }
    }
}

impl FromIterator<usize> for Bitset {
    fn from_iter<I: IntoIterator<Item = usize>>(iter: I) -> Self {
        let mut result = Self::default();

        for i in iter {
            result.set(i);
        }

        result
    }
}

impl IdxSet for Bitset {
    type IdxIter = BitsetIter;

    fn into_idx_iter(self) -> Self::IdxIter {
        self.into_iter()
    }

    fn size(&self) -> usize {
        self.bits.len()
    }

    fn intersect(&self, bits: &Bitfield) -> Bitfield {
        Bitset::intersect_in_slice(&self.bits, bits)
    }
}

impl IdxSet for Vec<Bitfield> {
    #[allow(clippy::type_complexity)]
    type IdxIter = Filter<<Vec<Bitfield> as IntoIterator>::IntoIter, fn(&Bitfield) -> bool>;

    fn into_idx_iter(self) -> Self::IdxIter {
        self.into_iter().filter(Bitfield::valid)
    }

    fn size(&self) -> usize {
        self.len()
    }

    fn intersect(&self, bits: &Bitfield) -> Bitfield {
        Bitset::intersect_in_slice(self, bits)
    }
}

#[cfg(feature = "smallvec")]
impl<A> IdxSet for smallvec::SmallVec<A>
where
    A: smallvec::Array<Item = Bitfield>,
{
    type IdxIter = Filter<<smallvec::SmallVec<A> as IntoIterator>::IntoIter, fn(&Bitfield) -> bool>;

    fn into_idx_iter(self) -> Self::IdxIter {
        self.into_iter().filter(Bitfield::valid)
    }

    fn size(&self) -> usize {
        self.len()
    }

    fn intersect(&self, bits: &Bitfield) -> Bitfield {
        Bitset::intersect_in_slice(self, bits)
    }
}

impl MemoryUser for Bitset {
    fn memory_usage(&self) -> MemoryUsage {
        let len: usize = self
            .bits
            .iter()
            .filter(|bitfield| bitfield.ones() > 0)
            .count();

        MemoryUsage {
            size_of: Some(std::mem::size_of::<Bitfield>()),
            len,
            capacity: self.bits.capacity(),
        }
    }

    fn shrink_with<F: Fn(&MemoryUsage) -> Option<usize>>(&mut self, f: F) {
        Arc::make_mut(&mut self.bits).retain(|bitfield| bitfield.ones() > 0);

        if let Some(_min_capacity) = f(&self.memory_usage()) {
            Arc::make_mut(&mut self.bits).shrink_to_fit();
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::Rng;
    use std::collections::BTreeSet;

    #[test]
    fn test_single_bit() {
        let mut b = Bitset::default();

        assert!(!b.get(7));
        b.set(7);
        assert!(b.get(7));

        assert!(!b.get(0));
        assert!(!b.get(8));
        assert!(!b.get(6));
        assert!(!b.get(257));

        assert_eq!(1, b.iter().count());

        for i in b.iter().flatten() {
            assert_eq!(7, i);
        }
    }

    #[test]
    fn test_tight_cluster() {
        let mut b = Bitset::default();

        b.set(19);
        b.set(20);
        b.set(21);
        b.set(23);
        b.set(24);
        b.set(27);

        assert!(b.get(19));
        assert!(b.get(20));
        assert!(b.get(21));
        assert!(!b.get(22));
        assert!(b.get(23));
        assert!(b.get(24));
        assert!(!b.get(25));
        assert!(!b.get(26));
        assert!(b.get(27));

        assert_eq!(6, b.iter().flatten().count());

        let v: Vec<_> = b.iter().flatten().collect();
        assert_eq!(&v, &[19, 20, 21, 23, 24, 27]);
    }

    #[test]
    fn test_unset() {
        let mut b = Bitset::default();

        b.set(19);
        b.set(20);
        b.set(21);
        b.set(23);
        b.set(24);
        b.set(27);

        assert!(b.get(19));
        assert!(b.get(20));
        assert!(b.get(21));
        assert!(!b.get(22));
        assert!(b.get(23));
        assert!(b.get(24));
        assert!(!b.get(25));
        assert!(!b.get(26));
        assert!(b.get(27));

        b.unset(19);
        b.unset(20);
        b.unset(21);
        b.unset(23);
        b.unset(24);
        b.unset(27);

        assert_eq!(0, b.iter().flatten().count());
    }

    #[test]
    fn test_sparse() {
        let mut b = Bitset::default();

        b.set(10);
        b.set(20);
        b.set(40);
        b.set(80);
        b.set(100);

        b.set(1000);
        b.set(2000);
        b.set(4000);
        b.set(8000);
        b.set(10000);

        b.set(20000);
        b.set(40000);
        b.set(80000);
        b.set(100_000);
        b.set(200_000);

        b.set(400_000);
        b.set(800_000);
        b.set(1_000_000);
        b.set(2_000_000);
        b.set(4_000_000);

        b.set(8_000_000);
        b.set(10_000_000);
        b.set(20_000_000);
        b.set(40_000_000);
        b.set(80_000_000);

        b.set(100_000_000);
        b.set(200_000_000);
        b.set(400_000_000);
        b.set(800_000_000);

        assert!(!b.get(600_000_000));
        assert!(b.get(800_000_000));

        assert_eq!(29, b.iter().flatten().count());
    }

    #[test]
    fn test_random() {
        let mut b = Bitset::default();
        let mut h = BTreeSet::new();

        for _ in 0..1000 {
            let x = rand::thread_rng().gen_range(0..10_000);
            b.set(x);
            h.insert(x);
        }

        let mut fore = Vec::new();
        for i in b.iter().flatten() {
            fore.push(i);
            assert!(h.contains(&i));
        }

        let mut aft = Vec::new();
        for i in b.iter().flatten().rev() {
            aft.push(i);
            assert!(h.contains(&i));
        }

        aft.reverse();
        assert_eq!(&fore, &aft);

        for (i, v) in h.iter().enumerate() {
            assert!(b.get(*v));
            assert_eq!(&fore[i], v);
            assert_eq!(&aft[i], v);
        }

        for x in 0..10_000 {
            assert_eq!(b.get(x), h.contains(&x));
        }
    }
}
