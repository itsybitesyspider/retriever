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
        if self.0.end <= self.0.start {
            0
        } else {
            let bitfield_end = (self.0.end - 1) / crate::bits::bitfield::BITS + 1;
            let bitfield_start = self.0.start / crate::bits::bitfield::BITS;
            bitfield_end - bitfield_start
        }
    }

    fn intersect(&self, idx: &Bitfield) -> Bitfield {
        (*idx).clip(&self.0)
    }
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_size_of_zero_length_idxrange() {
        let zero_range = IdxRange(0..0);

        assert_eq!(zero_range.size(), 0);
    }

    #[test]
    fn test_size_of_single_length_idxrange() {
        let zero_range = IdxRange(0..1);

        assert_eq!(zero_range.size(), 1);
    }

    #[test]
    fn test_size_of_short_idxrange() {
        let zero_range = IdxRange(0..7);

        assert_eq!(zero_range.size(), 1);
    }
}