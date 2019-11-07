use crate::bits::bitfield::Bitfield;
use crate::traits::idxset::IdxSet;

/// The intersection of two `IdxSets`.
#[derive(Clone)]
pub struct Intersection<A, B> {
    a: A,
    b: B,
}

/// An iterator over an `Intersection`.
pub enum IntersectionIter<A: IdxSet, B: IdxSet> {
    /// Use when `A` is the smaller IdxSet.
    A {
        /// An iterator over A
        a: A::IdxIter,
        /// B
        b: B,
    },
    /// Use when `B` is the smaller IdxSet.
    B {
        /// A
        a: A,
        /// An iterator over B
        b: B::IdxIter,
    },
}

impl<A, B> Intersection<A, B>
where
    A: IdxSet,
    B: IdxSet,
{
    /// Construct the intersection of two `IdxSets`.
    pub fn new(a: A, b: B) -> Self {
        Intersection { a, b }
    }
}

impl<A, B> IdxSet for Intersection<A, B>
where
    A: IdxSet,
    B: IdxSet,
{
    type IdxIter = IntersectionIter<A, B>;

    fn into_idx_iter(self) -> IntersectionIter<A, B> {
        if self.a.size() < self.b.size() {
            IntersectionIter::A {
                a: self.a.into_idx_iter(),
                b: self.b,
            }
        } else {
            IntersectionIter::B {
                a: self.a,
                b: self.b.into_idx_iter(),
            }
        }
    }

    fn size(&self) -> usize {
        self.a.size().min(self.b.size())
    }

    fn intersect(&self, other: &Bitfield) -> Bitfield {
        self.b.intersect(&self.a.intersect(other))
    }
}

impl<A, B> Iterator for IntersectionIter<A, B>
where
    A: IdxSet,
    B: IdxSet,
{
    type Item = Bitfield;

    #[inline(always)]
    fn next(&mut self) -> Option<Bitfield> {
        match self {
            IntersectionIter::A { a, b } => Some(b.intersect(&a.next()?)),
            IntersectionIter::B { a, b } => Some(a.intersect(&b.next()?)),
        }
    }
}

impl<A, B> DoubleEndedIterator for IntersectionIter<A, B>
where
    A: IdxSet,
    B: IdxSet,
{
    #[inline(always)]
    fn next_back(&mut self) -> Option<Bitfield> {
        match self {
            IntersectionIter::A { a, b } => Some(b.intersect(&a.next_back()?)),
            IntersectionIter::B { a, b } => Some(a.intersect(&b.next_back()?)),
        }
    }
}
