pub(super) const BITS: usize = (std::mem::size_of::<usize>() * 8);

#[derive(Clone, Copy)]
pub(super) struct Bitfield {
    start: usize,
    bits: usize,
}

pub(crate) struct BitfieldIter {
    start: usize,
    bits: usize,
    front: isize,
    back: isize,
}

impl Bitfield {
    pub(super) fn new(i: usize) -> Self {
        Bitfield {
            start: i / BITS,
            bits: 0b1 << (i % BITS),
        }
    }

    #[inline]
    pub(super) fn ones(&self) -> usize {
        self.bits.count_ones() as usize
    }

    #[inline]
    pub(super) fn start(&self) -> usize {
        self.start
    }

    #[inline]
    pub(super) fn set(&mut self, i: usize) {
        assert_eq!(i / BITS, self.start);

        self.bits |= 0b1 << (i % BITS);
    }

    #[inline]
    pub(super) fn unset(&mut self, i: usize) {
        assert_eq!(i / BITS, self.start);

        self.bits &= !(0b1 << (i % BITS));
    }

    #[inline]
    pub(super) fn get(&self, i: usize) -> bool {
        assert_eq!(i / BITS, self.start);

        self.bits & (0b1 << (i % BITS)) != 0
    }
}

impl IntoIterator for Bitfield {
    type IntoIter = BitfieldIter;
    type Item = usize;

    fn into_iter(self) -> Self::IntoIter {
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

#[cfg(test)]
mod test {
    use super::*;
    use rand::Rng;
    use std::collections::BTreeSet;

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
        assert!(!b.get(BITS-1));
        assert!(!b.get(BITS-6));
        assert!(!b.get(BITS-8));

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
            let x = rand::thread_rng().gen_range(57600, 57600+BITS);
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

        for (i,v) in h.iter().enumerate() {
            assert!(b.get(*v));
            assert_eq!(&fore[i], v);
            assert_eq!(&aft[i], v);
        }

        for x in 57600..57600+BITS {
            assert_eq!(b.get(x), h.contains(&x));
        }
    }
}
