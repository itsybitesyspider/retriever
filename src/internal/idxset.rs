use crate::internal::bits::Bitset;
use std::ops::Range;

#[derive(Clone)]
pub struct IdxSet(IdxSetEnum);
pub struct IdxIter(IdxIterEnum, Vec<IdxSet>);

#[derive(Clone)]
enum IdxSetEnum {
    Nothing,
    ExactValue(usize),
    ToLength(usize),
    Bitset(Bitset),
    Intersection(Vec<IdxSet>),
}

enum IdxIterEnum {
    Range(Range<usize>),
    Bitset(<Bitset as IntoIterator>::IntoIter),
}

impl IdxSet {
    pub fn nothing() -> Self {
        IdxSet(IdxSetEnum::Nothing)
    }

    #[inline(always)]
    pub fn contains(&self, idx: usize) -> bool {
        match &self.0 {
            IdxSetEnum::Nothing => false,
            IdxSetEnum::ExactValue(x) => idx == *x,
            IdxSetEnum::ToLength(range) => (0..*range).contains(&idx),
            IdxSetEnum::Bitset(hs) => hs.get(idx),
            IdxSetEnum::Intersection(xs) => xs.iter().all(|x| x.contains(idx)),
        }
    }

    pub fn iter(&self) -> IdxIter {
        match &self.0 {
            IdxSetEnum::Nothing => IdxIter(IdxIterEnum::Range(0..0), Vec::new()),
            IdxSetEnum::ExactValue(x) =>
            {
                #[allow(clippy::range_plus_one)]
                IdxIter(IdxIterEnum::Range(*x..*x + 1), Vec::new())
            }
            IdxSetEnum::ToLength(range) => IdxIter(IdxIterEnum::Range(0..*range), Vec::new()),
            IdxSetEnum::Bitset(bs) => IdxIter(IdxIterEnum::Bitset(bs.iter()), Vec::new()),
            IdxSetEnum::Intersection(xs) if xs.is_empty() => {
                IdxIter(IdxIterEnum::Range(0..0), Vec::new())
            }
            IdxSetEnum::Intersection(xs) => {
                let mut iter = xs[0].iter();
                iter.1.extend_from_slice(&xs[1..]);
                iter
            }
        }
    }

    pub fn intersection(a: Self, b: Self) -> Self {
        IdxSet(match (a.0, b.0) {
            (IdxSetEnum::Intersection(mut xs), IdxSetEnum::Intersection(mut ys)) => {
                xs.append(&mut ys);
                xs.sort_by_key(|x| x.len());
                IdxSetEnum::Intersection(xs)
            }
            (IdxSetEnum::Intersection(mut xs), other) => {
                xs.push(IdxSet(other));
                xs.sort_by_key(|x| x.len());
                IdxSetEnum::Intersection(xs)
            }
            (other, IdxSetEnum::Intersection(mut xs)) => {
                xs.push(IdxSet(other));
                xs.sort_by_key(|x| x.len());
                IdxSetEnum::Intersection(xs)
            }
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
            (ax, bx) => {
                let mut result = vec![IdxSet(ax), IdxSet(bx)];
                result.sort_by_key(|x| x.len());
                IdxSetEnum::Intersection(result)
            }
        })
    }

    pub fn len(&self) -> usize {
        match &self.0 {
            IdxSetEnum::Nothing => 0,
            IdxSetEnum::ExactValue(_) => 1,
            IdxSetEnum::ToLength(length) => *length,
            IdxSetEnum::Bitset(bs) => bs.len(),
            IdxSetEnum::Intersection(xs) => xs.iter().map(|x| x.len()).min().unwrap_or(0),
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
        let result = match &mut self.0 {
            IdxIterEnum::Range(range) => range.next(),
            IdxIterEnum::Bitset(bs) => bs.next(),
        }?;

        if self.1.iter().any(|xs| !xs.contains(result)) {
            return self.next();
        }

        Some(result)
    }
}

impl DoubleEndedIterator for IdxIter {
    #[inline(always)]
    fn next_back(&mut self) -> Option<usize> {
        let result = match &mut self.0 {
            IdxIterEnum::Range(range) => range.next_back(),
            IdxIterEnum::Bitset(bs) => bs.next_back(),
        }?;

        if self.1.iter().any(|xs| !xs.contains(result)) {
            return self.next_back();
        }

        Some(result)
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
