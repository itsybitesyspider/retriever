pub(super) const BITS: usize = (std::mem::size_of::<usize>() * 8);

#[derive(Clone, Copy)]
pub(super) struct Bitfield {
  start: usize,
  bits: usize
}

pub(crate) struct BitfieldIter {
    idx: usize,
    forward: usize,
    bits: usize,
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
            idx: self.start * BITS,
            forward: 0,
            bits: self.bits,
        }
    }
}

impl Iterator for BitfieldIter {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<usize> {
        if self.forward >= BITS {
            return None;
        }

        self.forward += (self.bits >> self.forward).trailing_zeros() as usize;

        if self.forward >= BITS {
            return None;
        }

        let result = self.idx + self.forward;
        self.forward += 1;
        Some(result)
    }
}
