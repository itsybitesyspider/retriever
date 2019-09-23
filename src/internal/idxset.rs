use crate::internal::bitset::Bitset;
use std::ops::Range;

pub struct IdxSet(IdxSetEnum);
pub struct IdxIter(IdxIterEnum);

enum IdxSetEnum {
    Nothing,
    ExactValue(usize),
    ToLength(usize),
    Bitset(Bitset),
}

enum IdxIterEnum {
    Range(Range<usize>),
    Bitset(<Bitset as IntoIterator>::IntoIter),
}

impl IdxSet {
    pub fn contains(&self, idx: usize) -> bool {
        match &self.0 {
            IdxSetEnum::Nothing => false,
            IdxSetEnum::ExactValue(x) => idx == *x,
            IdxSetEnum::ToLength(range) => (0..*range).contains(&idx),
            IdxSetEnum::Bitset(hs) => hs.get(idx),
        }
    }

    pub fn iter(&self) -> IdxIter {
        IdxIter(match &self.0 {
            IdxSetEnum::Nothing => IdxIterEnum::Range(0..0),
            IdxSetEnum::ExactValue(x) =>
            {
                #[allow(clippy::range_plus_one)]
                IdxIterEnum::Range(*x..*x + 1)
            }
            IdxSetEnum::ToLength(range) => IdxIterEnum::Range(0..*range),
            IdxSetEnum::Bitset(bs) => IdxIterEnum::Bitset(bs.iter()),
        })
    }

    pub fn intersection(a: Self, b: Self) -> Self {
        IdxSet(match (a.0, b.0) {
            (IdxSetEnum::Nothing, _) => IdxSetEnum::Nothing,
            (_, IdxSetEnum::Nothing) => IdxSetEnum::Nothing,
            (IdxSetEnum::ExactValue(x), rest) => {
                if IdxSet(rest).contains(x) {
                    IdxSetEnum::ExactValue(x)
                } else {
                    IdxSetEnum::Nothing
                }
            }
            (rest, IdxSetEnum::ExactValue(x)) => {
                if IdxSet(rest).contains(x) {
                    IdxSetEnum::ExactValue(x)
                } else {
                    IdxSetEnum::Nothing
                }
            }
            (IdxSetEnum::ToLength(ax), IdxSetEnum::ToLength(bs)) => {
                IdxSetEnum::ToLength(ax.min(bs))
            }
            (IdxSetEnum::ToLength(ax), rest) => {
                IdxSetEnum::Bitset(IdxSet(rest).iter().filter(|x| *x < ax).collect())
            }
            (rest, IdxSetEnum::ToLength(bx)) => {
                IdxSetEnum::Bitset(IdxSet(rest).iter().filter(|x| *x < bx).collect())
            }
            (IdxSetEnum::Bitset(ax), IdxSetEnum::Bitset(bx)) => {
                IdxSetEnum::Bitset(ax.intersection(bx))
            }
        })
    }

    pub fn len(&self) -> usize {
        match &self.0 {
            IdxSetEnum::Nothing => 0,
            IdxSetEnum::ExactValue(_) => 1,
            IdxSetEnum::ToLength(length) => *length,
            IdxSetEnum::Bitset(bs) => bs.len(),
        }
    }

    /// IdxSet that iterates from 0 to the given length.
    pub fn from_length(len: usize) -> Self {
        IdxSet(IdxSetEnum::ToLength(len))
    }
}

impl Iterator for IdxIter {
    type Item = usize;

    // this inline seems to be a very strong win; I'm guessing because the caller gets to optimize out the branch indirection
    #[inline(always)]
    fn next(&mut self) -> Option<usize> {
        match &mut self.0 {
            IdxIterEnum::Range(range) => range.next(),
            IdxIterEnum::Bitset(bs) => bs.next(),
        }
    }
}

impl DoubleEndedIterator for IdxIter {
    #[inline(always)]
    fn next_back(&mut self) -> Option<usize> {
        match &mut self.0 {
            IdxIterEnum::Range(range) => range.next_back(),
            IdxIterEnum::Bitset(bs) => bs.next_back(),
        }
    }
}

impl<T: Into<IdxSet>> From<Option<T>> for IdxSet {
    fn from(other: Option<T>) -> Self {
        other
            .map(|x| x.into())
            .unwrap_or(IdxSet(IdxSetEnum::Nothing))
    }
}

impl From<usize> for IdxSet {
    fn from(other: usize) -> Self {
        IdxSet(IdxSetEnum::ExactValue(other))
    }
}

impl From<Bitset> for IdxSet {
    fn from(other: Bitset) -> Self {
        IdxSet(IdxSetEnum::Bitset(other))
    }
}
