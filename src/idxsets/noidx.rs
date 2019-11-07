use crate::bits::bitfield::Bitfield;
use crate::traits::idxset::IdxSet;

/// An `IdxSet` containing nothing.
#[derive(Clone)]
pub struct NoIdx;

impl Iterator for NoIdx {
    type Item = Bitfield;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

impl DoubleEndedIterator for NoIdx {
    fn next_back(&mut self) -> Option<Self::Item> {
        None
    }
}

impl IdxSet for NoIdx {
    type IdxIter = <Self as IntoIterator>::IntoIter;

    fn into_idx_iter(self) -> Self::IdxIter {
        self
    }

    fn size(&self) -> usize {
        0
    }

    fn intersect(&self, idx: &Bitfield) -> Bitfield {
        Bitfield::new_empty(idx.start())
    }
}
