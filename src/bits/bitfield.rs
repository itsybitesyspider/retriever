use crate::traits::idxset::IdxSet;
use std::iter::{once, Once};
use std::ops::Range;

pub(crate) const BITS: usize = (std::mem::size_of::<usize>() * 8);

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum SortOrder {
    Some(usize),
    Extra,
}

pub(super) fn start_of(idx: usize) -> SortOrder {
    SortOrder::Some((idx / BITS) * BITS)
}

/// A bitset consisting of size_of<usize>() consecutive bits that are also size_of<usize>() aligned.
#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub struct Bitfield {
    pub(super) start: usize,
    pub(super) bits: usize,
}

impl Default for Bitfield {
    fn default() -> Self {
        Bitfield {
            start: !0b0,
            bits: 0b0,
        }
    }
}

/// An iterator over a Bitfield.
#[derive(Clone)]
pub struct BitfieldIter {
    start: usize,
    bits: usize,
    front: isize,
    back: isize,
}

impl Bitfield {
    /// True if this Bitfield is valid. The only way to construct an invalid bitfield is Bitfield::default().
    pub(crate) fn valid(&self) -> bool {
        self.start != !0b0
    }

    /// Construct a new Bitfield with the given value.
    pub(crate) fn new(i: usize) -> Self {
        Bitfield {
            start: i / BITS,
            bits: 0b1 << (i % BITS),
        }
    }

    /// Construct a new Bitfield with no bits set. The Bitfield's range will include the specified bit index.
    pub(crate) fn new_empty(i: usize) -> Self {
        Bitfield {
            start: i / BITS,
            bits: 0,
        }
    }

    /// Intersection of two bitfields. If they do not have overlapping ranges, then the result will always be the empty set.
    pub(crate) fn intersect(&self, other: &Bitfield) -> Bitfield {
        assert!(self.valid());
        assert!(other.valid());
        Bitfield {
            start: self.start,
            bits: {
                if self.start == other.start {
                    self.bits & other.bits
                } else {
                    0b0
                }
            },
        }
    }

    /// Construct a Bitfield from the given Range of indices. This consumes the given indices from the range and adds them to returned Bitfield.
    /// The Bitfield can consume at most `size_of<usize>()` bits, so some portion of the Range is likely to remain afterwards.
    pub(crate) fn from_range(i: &mut Range<usize>) -> Option<Self> {
        let mut result = Self::new(i.next()?);

        if result.bits == 0b1 && i.end - i.start >= BITS {
            result.bits = !0b0_usize;
            i.start = (i.start / BITS + 1) * BITS;
            Some(result)
        } else {
            while i.start < i.end && i.start < result.start() + BITS {
                result.set(i.start);
                i.start += 1;
            }
            Some(result)
        }
    }

    /// Construct a Bitfield from the high end of the given Range of indices.
    pub(crate) fn from_range_rev(i: &mut Range<usize>) -> Option<Self> {
        let mut result = Self::new(i.next_back()?);

        while i.start < i.end && result.start() < i.end {
            i.end -= 1;
            result.set(i.end);
        }

        Some(result)
    }

    /// Clip this Bitfield to the given Range of indices.
    pub(crate) fn clip(mut self, range: &Range<usize>) -> Self {
        assert!(self.valid());

        if range.end < self.start || range.start >= self.start + BITS {
            self.bits = 0b0;
            return self;
        }

        for i in self.start..range.start {
            self.unset(i);
        }

        for i in range.end..self.start + BITS {
            self.unset(i);
        }

        self
    }

    pub(crate) fn ones(&self) -> usize {
        self.bits.count_ones() as usize
    }

    pub(crate) fn start(&self) -> usize {
        assert!(self.valid());
        self.start * BITS
    }

    pub(super) fn sort_order(&self) -> SortOrder {
        if !self.valid() {
            SortOrder::Extra
        } else {
            SortOrder::Some(self.start * BITS)
        }
    }

    pub(crate) fn set(&mut self, i: usize) {
        assert!(self.valid());
        assert_eq!(i / BITS, self.start);

        self.bits |= 0b1 << (i % BITS);
    }

    pub(crate) fn unset(&mut self, i: usize) {
        assert!(self.valid());
        assert_eq!(i / BITS, self.start);

        self.bits &= !(0b1 << (i % BITS));
    }

    pub(crate) fn get(&self, i: usize) -> bool {
        assert!(self.valid());
        assert_eq!(i / BITS, self.start);

        self.bits & (0b1 << (i % BITS)) != 0
    }
}

impl IntoIterator for Bitfield {
    type IntoIter = BitfieldIter;
    type Item = usize;

    fn into_iter(self) -> Self::IntoIter {
        assert!(self.valid());
        BitfieldIter {
            start: self.start * BITS,
            bits: self.bits,
            front: self.bits.trailing_zeros() as isize,
            back: self.bits.leading_zeros() as isize,
        }
    }
}

impl Iterator for BitfieldIter {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<usize> {
        if self.front < BITS as isize - self.back {
            let result = self.start + self.front as usize;
            self.front += 1;
            if self.front < BITS as isize {
                self.front += (self.bits >> self.front).trailing_zeros() as isize;
            }
            Some(result)
        } else {
            None
        }
    }
}

impl DoubleEndedIterator for BitfieldIter {
    #[inline]
    fn next_back(&mut self) -> Option<usize> {
        if self.front < BITS as isize - self.back {
            self.back += 1;
            let result = self.start + BITS - self.back as usize;
            if self.back < BITS as isize {
                self.back += (self.bits << self.back).leading_zeros() as isize;
            }
            Some(result)
        } else {
            None
        }
    }
}

impl From<usize> for Bitfield {
    fn from(idx: usize) -> Self {
        Bitfield::new(idx)
    }
}

impl<T> From<Option<T>> for Bitfield
where
    T: Into<Bitfield>,
{
    fn from(other: Option<T>) -> Self {
        match other {
            Some(t) => t.into(),
            None => Bitfield::new_empty(0),
        }
    }
}

impl IdxSet for Bitfield {
    type IdxIter = Once<Bitfield>;

    fn into_idx_iter(self) -> Self::IdxIter {
        once(self)
    }

    fn size(&self) -> usize {
        1
    }

    fn intersect(&self, idx: &Bitfield) -> Bitfield {
        self.intersect(idx)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::Rng;
    use std::collections::BTreeSet;

    #[test]
    fn test_from_range() {
        for _ in 0..1000 {
            let range =
                rand::thread_rng().gen_range(0, 1000)..rand::thread_rng().gen_range(0, 10000);
            let mut ranger = range.clone();

            let mut bits = Vec::new();
            while let Some(bitties) = Bitfield::from_range(&mut ranger) {
                bits.push(bitties);
            }

            let mut idxs = Vec::new();
            for i in bits.iter().copied().flatten() {
                assert!(range.contains(&i));
                idxs.push(i);
            }

            for i in 0..10000 {
                assert_eq!(
                    range.contains(&i),
                    idxs.binary_search(&i).is_ok(),
                    "i: {}, range: {:?}",
                    i,
                    &range
                );
            }
        }
    }

    #[test]
    fn test_from_range_rev() {
        for _ in 0..1000 {
            let range =
                rand::thread_rng().gen_range(0, 1000)..rand::thread_rng().gen_range(0, 10000);
            let mut ranger = range.clone();

            let mut bits = Vec::new();
            while let Some(bitties) = Bitfield::from_range_rev(&mut ranger) {
                bits.push(bitties);
            }
            bits.reverse();

            let mut idxs = Vec::new();
            for i in bits.iter().copied().flatten() {
                assert!(range.contains(&i));
                idxs.push(i);
            }

            for i in 0..10000 {
                assert_eq!(
                    range.contains(&i),
                    idxs.binary_search(&i).is_ok(),
                    "i: {}, range: {:?}",
                    i,
                    &range
                );
            }
        }
    }

    #[test]
    fn test_two_bits() {
        let mut b = Bitfield::new(7);

        assert!(b.get(7));
        assert!(!b.get(21));
        b.set(21);
        assert!(b.get(21));

        assert!(!b.get(0));
        assert!(!b.get(8));
        assert!(!b.get(6));
        assert!(!b.get(20));
        assert!(!b.get(22));
        assert!(!b.get(31));
        assert!(!b.get(BITS - 1));
        assert!(!b.get(BITS - 6));
        assert!(!b.get(BITS - 8));

        assert_eq!(2, b.into_iter().count());

        for i in b.into_iter() {
            assert!(i == 7 || i == 21);
        }
    }

    #[test]
    fn test_unset() {
        let mut b = Bitfield::new(21);

        b.set(19);
        b.set(20);
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

        assert_eq!(3, b.into_iter().count());
        assert_eq!(3, b.into_iter().rev().count());

        b.unset(23);
        b.unset(24);
        b.unset(27);

        assert_eq!(0, b.into_iter().count());
    }

    #[test]
    fn test_random() {
        let mut b = Bitfield::new(57602);
        let mut h = BTreeSet::new();

        h.insert(57602);

        for _ in 0..16 {
            let x = rand::thread_rng().gen_range(57600, 57600 + BITS);
            b.set(x);
            h.insert(x);
        }

        let mut fore = Vec::new();
        for i in b.into_iter() {
            fore.push(i);
            assert!(h.contains(&i));
        }

        let mut aft = Vec::new();
        for i in b.into_iter().rev() {
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

        for x in 57600..57600 + BITS {
            assert_eq!(b.get(x), h.contains(&x));
        }
    }
}
