use crate::bits::bitfield::Bitfield;
use crate::traits::idxset::IdxSet;
use std::ops::Range;

/// An `IdxSet` representing a contiguous range of indices.
#[derive(Clone)]
pub struct IdxRange(pub Range<usize>);

impl Iterator for IdxRange {
    type Item = Bitfield;

    fn next(&mut self) -> Option<Bitfield> {
        Some(Bitfield::from_range(&mut self.0)?)
    }
}

impl DoubleEndedIterator for IdxRange {
    fn next_back(&mut self) -> Option<Bitfield> {
        Some(Bitfield::from_range_rev(&mut self.0)?)
    }
}

impl IdxSet for IdxRange {
    type IdxIter = <Self as IntoIterator>::IntoIter;

    fn into_idx_iter(self) -> Self::IdxIter {
        self
    }

    fn size(&self) -> usize {
        ((self.0.end - 1) / crate::bits::bitfield::BITS + 1)
            - self.0.start / crate::bits::bitfield::BITS
    }

    fn intersect(&self, idx: &Bitfield) -> Bitfield {
        (*idx).clip(&self.0)
    }
}
