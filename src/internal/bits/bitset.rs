use super::bitfield::*;
use std::iter::Flatten;
use std::iter::FromIterator;
use std::sync::Arc;

/// A sparse bitset
#[derive(Clone)]
pub(crate) struct Bitset {
    bits: Arc<Vec<Bitfield>>,
}

pub(crate) struct BitsetIter {
    bits: Arc<Vec<Bitfield>>,
    i: usize,
}

impl Bitset {
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
        match self.bits.binary_search_by_key(&(i / BITS), Bitfield::start) {
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
        if let Ok(bidx) = self.bits.binary_search_by_key(&(i / BITS), Bitfield::start) {
            Arc::make_mut(&mut self.bits)[bidx].unset(i);
        }
    }

    /// Set the specific bit position in this Bitset
    pub fn get(&self, i: usize) -> bool {
        match self.bits.binary_search_by_key(&(i / BITS), Bitfield::start) {
            Ok(bidx) => self.bits[bidx].get(i),
            Err(_) => false,
        }
    }

    /// Iterate over all values set in this Bitset
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
    type Item = BitfieldIter;

    fn next(&mut self) -> Option<BitfieldIter> {
        if self.i < self.bits.len() {
            let result = self.bits[self.i];
            self.i += 1;
            Some(result.into_iter())
        } else {
            None
        }
    }
}

impl IntoIterator for Bitset {
    type Item = usize;
    type IntoIter = <Flatten<BitsetIter> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        BitsetIter {
            bits: self.bits,
            i: 0,
        }
        .flatten()
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

#[cfg(test)]
mod test {
    use super::*;
    use rand::Rng;
    use std::collections::hash_set::HashSet;

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

        for i in b.iter() {
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

        assert_eq!(6, b.iter().count());

        let v: Vec<_> = b.iter().collect();
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

        assert_eq!(0, b.iter().count());
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

        assert_eq!(29, b.iter().count());
    }

    #[test]
    fn test_random() {
        let mut b = Bitset::default();
        let mut h = HashSet::new();

        for _ in 0..1000 {
            let x = rand::thread_rng().gen_range(0, 10_000);
            b.set(x);
            h.insert(x);
        }

        for i in b.iter() {
            assert!(h.contains(&i));
        }

        for i in h.iter() {
            assert!(b.get(*i));
        }

        for i in h.iter() {
            assert!(b.get(*i));
        }

        for x in 0..10_000 {
            assert_eq!(b.get(x), h.contains(&x));
        }
    }
}
