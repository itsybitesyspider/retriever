use crate::bits::Bitfield;
use crate::idxsets::intersection::Intersection;
use std::iter::Flatten;

/// A set of `usize` indices.
pub trait IdxSet: Sized + Clone {
    /// A sorted `Iterator` over this `IdxSet`.
    type IdxIter: Iterator<Item = Bitfield> + DoubleEndedIterator;

    /// Convert this `IdxSet` into a sorted `Iterator`.
    fn into_idx_iter(self) -> Self::IdxIter;
    /// The best estimate of the size of this `IdxSet`, as a count of the number of `Bitfield` chunks it contains.
    fn size(&self) -> usize;
    /// Intersect the given `Bitfield` with the contents of this `IdxSet`.
    fn intersect(&self, idx: &Bitfield) -> Bitfield;

    /// Construct the intersection of this `IdxSet` with another `IdxSet`.
    fn intersection<B>(self, b: B) -> Intersection<Self, B>
    where
        B: IdxSet,
    {
        Intersection::new(self, b)
    }
}

impl<T> IdxSet for Option<T>
where
    T: IdxSet,
{
    type IdxIter = Flatten<<Option<T::IdxIter> as IntoIterator>::IntoIter>;

    fn into_idx_iter(self) -> Self::IdxIter {
        self.map(|x| x.into_idx_iter()).into_iter().flatten()
    }

    fn size(&self) -> usize {
        self.as_ref().map(T::size).unwrap_or(0)
    }

    fn intersect(&self, idx: &Bitfield) -> Bitfield {
        self.as_ref()
            .map(|x| x.intersect(idx))
            .unwrap_or_else(|| Bitfield::new_empty(idx.start()))
    }
}
