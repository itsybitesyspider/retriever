use crate::bits::bitfield::Bitfield;
use crate::bits::Bitset;
use crate::idxsets::idxrange::IdxRange;
use crate::traits::query::Query;
use crate::traits::record::Record;
use crate::traits::valid_key::ValidKey;
use crate::types::chunk_storage::ChunkStorage;
use crate::types::storage::Storage;
use std::borrow::Borrow;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::ops::Range;
use std::ops::RangeInclusive;

/// A `Query` that visits a collection of explicitly enumerated chunks
/// (as opposed to the `Everything` query, which visits every chunk).
/// This is essential to build efficient queries when `Storage` contains a large
/// number of chunks.
///
/// `Chunks` supports several collection types: `Vec`, `HashSet`, `BTreeSet`,
/// `Range`, `RangeInclusive`, slices, and arrays up to length 16. If the
/// `smallvec` feature is enabled, this adds support for `SmallVec` backed
/// by arrays up to length 16.
#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Chunks<A>(pub A);

impl<Q, ChunkKey, ItemKey, Element> Query<ChunkKey, ItemKey, Element> for Chunks<Vec<Q>>
where
    Q: ValidKey,
    ChunkKey: ValidKey + Borrow<Q>,
    ItemKey: ValidKey,
    Element: Record<ChunkKey, ItemKey>,
{
    type ChunkIdxSet = Bitset;
    type ItemIdxSet = IdxRange;

    fn chunk_idxs(&self, storage: &Storage<ChunkKey, ItemKey, Element>) -> Self::ChunkIdxSet {
        self.0
            .iter()
            .filter_map(|x| storage.internal_idx_of(&x))
            .collect()
    }

    fn item_idxs(
        &self,
        _chunk_key: &ChunkKey,
        chunk_storage: &ChunkStorage<ChunkKey, ItemKey, Element>,
    ) -> Self::ItemIdxSet {
        IdxRange(0..chunk_storage.len())
    }

    #[inline(always)]
    fn test(&self, _element: &Element) -> bool {
        true
    }
}

impl<Q, S, ChunkKey, ItemKey, Element> Query<ChunkKey, ItemKey, Element> for Chunks<HashSet<Q, S>>
where
    Q: ValidKey,
    ChunkKey: ValidKey + Borrow<Q>,
    ItemKey: ValidKey,
    Element: Record<ChunkKey, ItemKey>,
{
    type ChunkIdxSet = Bitset;
    type ItemIdxSet = IdxRange;

    fn chunk_idxs(&self, storage: &Storage<ChunkKey, ItemKey, Element>) -> Self::ChunkIdxSet {
        self.0
            .iter()
            .filter_map(|x| storage.internal_idx_of(&x))
            .collect()
    }

    fn item_idxs(
        &self,
        _chunk_key: &ChunkKey,
        chunk_storage: &ChunkStorage<ChunkKey, ItemKey, Element>,
    ) -> Self::ItemIdxSet {
        IdxRange(0..chunk_storage.len())
    }

    #[inline(always)]
    fn test(&self, _element: &Element) -> bool {
        true
    }
}

impl<Q, ChunkKey, ItemKey, Element> Query<ChunkKey, ItemKey, Element> for Chunks<BTreeSet<Q>>
where
    Q: ValidKey,
    ChunkKey: ValidKey + Borrow<Q>,
    ItemKey: ValidKey,
    Element: Record<ChunkKey, ItemKey>,
{
    type ChunkIdxSet = Bitset;
    type ItemIdxSet = IdxRange;

    fn chunk_idxs(&self, storage: &Storage<ChunkKey, ItemKey, Element>) -> Self::ChunkIdxSet {
        self.0
            .iter()
            .filter_map(|x| storage.internal_idx_of(&x))
            .collect()
    }

    fn item_idxs(
        &self,
        _chunk_key: &ChunkKey,
        chunk_storage: &ChunkStorage<ChunkKey, ItemKey, Element>,
    ) -> Self::ItemIdxSet {
        IdxRange(0..chunk_storage.len())
    }

    #[inline(always)]
    fn test(&self, _element: &Element) -> bool {
        true
    }
}

impl<'a, Q, ChunkKey, ItemKey, Element> Query<ChunkKey, ItemKey, Element> for Chunks<&'a [Q]>
where
    Q: ValidKey,
    ChunkKey: ValidKey + Borrow<Q>,
    ItemKey: ValidKey,
    Element: Record<ChunkKey, ItemKey>,
{
    type ChunkIdxSet = Bitset;
    type ItemIdxSet = IdxRange;

    fn chunk_idxs(&self, storage: &Storage<ChunkKey, ItemKey, Element>) -> Self::ChunkIdxSet {
        self.0
            .iter()
            .filter_map(|x| storage.internal_idx_of(x))
            .collect()
    }

    fn item_idxs(
        &self,
        _chunk_key: &ChunkKey,
        chunk_storage: &ChunkStorage<ChunkKey, ItemKey, Element>,
    ) -> Self::ItemIdxSet {
        IdxRange(0..chunk_storage.len())
    }

    #[inline(always)]
    fn test(&self, _element: &Element) -> bool {
        true
    }
}

impl<Q, ChunkKey, ItemKey, Element> Query<ChunkKey, ItemKey, Element> for Chunks<Range<Q>>
where
    Q: ValidKey,
    Range<Q>: IntoIterator<Item = Q>,
    ChunkKey: ValidKey + Borrow<Q>,
    ItemKey: ValidKey,
    Element: Record<ChunkKey, ItemKey>,
{
    type ChunkIdxSet = Bitset;
    type ItemIdxSet = IdxRange;

    fn chunk_idxs(&self, storage: &Storage<ChunkKey, ItemKey, Element>) -> Self::ChunkIdxSet {
        self.0
            .clone()
            .into_iter()
            .filter_map(|x| storage.internal_idx_of(&x))
            .collect()
    }

    fn item_idxs(
        &self,
        _chunk_key: &ChunkKey,
        chunk_storage: &ChunkStorage<ChunkKey, ItemKey, Element>,
    ) -> Self::ItemIdxSet {
        IdxRange(0..chunk_storage.len())
    }

    #[inline(always)]
    fn test(&self, _element: &Element) -> bool {
        true
    }
}

impl<Q, ChunkKey, ItemKey, Element> Query<ChunkKey, ItemKey, Element> for Chunks<RangeInclusive<Q>>
where
    Q: ValidKey,
    RangeInclusive<Q>: IntoIterator<Item = Q>,
    ChunkKey: ValidKey + Borrow<Q>,
    ItemKey: ValidKey,
    Element: Record<ChunkKey, ItemKey>,
{
    type ChunkIdxSet = Bitset;
    type ItemIdxSet = IdxRange;

    fn chunk_idxs(&self, storage: &Storage<ChunkKey, ItemKey, Element>) -> Self::ChunkIdxSet {
        self.0
            .clone()
            .into_iter()
            .filter_map(|x| storage.internal_idx_of(&x))
            .collect()
    }

    fn item_idxs(
        &self,
        _chunk_key: &ChunkKey,
        chunk_storage: &ChunkStorage<ChunkKey, ItemKey, Element>,
    ) -> Self::ItemIdxSet {
        IdxRange(0..chunk_storage.len())
    }

    #[inline(always)]
    fn test(&self, _element: &Element) -> bool {
        true
    }
}

macro_rules! sized_array_query_impl {
  ( $n:literal ) => {
    #[cfg(feature="smallvec")]
    impl<Q, ChunkKey, ItemKey, Element> Query<ChunkKey, ItemKey, Element> for Chunks<smallvec::SmallVec<[Q;$n]>>
    where
        Q: ValidKey,
        ChunkKey: ValidKey + Borrow<Q>,
        ItemKey: ValidKey,
        Element: Record<ChunkKey, ItemKey>,
    {
        type ChunkIdxSet = smallvec::SmallVec<[Bitfield;$n]>;
        type ItemIdxSet = IdxRange;

        fn chunk_idxs(&self, storage: &Storage<ChunkKey, ItemKey, Element>) -> Self::ChunkIdxSet {
            let mut result = smallvec::SmallVec::new();

            if self.0.len() > $n && self.0.len() > 100 {
              #[cfg(feature ="log")]
              log::warn!("retriever: Chunks<SmallVec<[...;$n]>>: chunk list was much larger than expected; this is a possible cause of slow performance. Use ChunksIter instead.");
            }

            result.resize(self.0.len(),Bitfield::default());

            for chunk_key in self.0.iter() {
                if let Some(idx) = storage.internal_idx_of(chunk_key) {
                    Bitset::set_in_slice(&mut result, idx).expect("capacity should always be sufficient");
                }
            }

            result
        }

        fn item_idxs(
            &self,
            _chunk_key: &ChunkKey,
            chunk_storage: &ChunkStorage<ChunkKey, ItemKey, Element>,
        ) -> Self::ItemIdxSet {
            IdxRange(0..chunk_storage.len())
        }

        #[inline(always)]
        fn test(&self, _element: &Element) -> bool {
            true
        }
    }

    impl<Q, ChunkKey, ItemKey, Element> Query<ChunkKey, ItemKey, Element> for Chunks<[Q;$n]>
    where
        Q: ValidKey,
        ChunkKey: ValidKey + Borrow<Q>,
        ItemKey: ValidKey,
        Element: Record<ChunkKey, ItemKey>,
    {
        #[cfg(feature="smallvec")]
        type ChunkIdxSet = smallvec::SmallVec<[Bitfield;$n]>;
        #[cfg(not(feature="smallvec"))]
        type ChunkIdxSet = Vec<Bitfield>;
        type ItemIdxSet = IdxRange;

        fn chunk_idxs(&self, storage: &Storage<ChunkKey, ItemKey, Element>) -> Self::ChunkIdxSet {
            #[cfg(feature="smallvec")]
            let mut result = smallvec::SmallVec::new();
            #[cfg(not(feature="smallvec"))]
            let mut result = Vec::new();

            result.resize($n,Bitfield::default());

            for chunk_key in self.0.iter() {
                if let Some(idx) = storage.internal_idx_of(chunk_key) {
                    Bitset::set_in_slice(&mut result, idx).expect("capacity should always be sufficient");
                }
            }

            result
        }

        fn item_idxs(
            &self,
            _chunk_key: &ChunkKey,
            chunk_storage: &ChunkStorage<ChunkKey, ItemKey, Element>,
        ) -> Self::ItemIdxSet {
            IdxRange(0..chunk_storage.len())
        }

        #[inline(always)]
        fn test(&self, _element: &Element) -> bool {
            true
        }
    }
  }
}

sized_array_query_impl!(1);
sized_array_query_impl!(2);
sized_array_query_impl!(3);
sized_array_query_impl!(4);
sized_array_query_impl!(5);
sized_array_query_impl!(6);
sized_array_query_impl!(7);
sized_array_query_impl!(8);
sized_array_query_impl!(9);
sized_array_query_impl!(10);
sized_array_query_impl!(11);
sized_array_query_impl!(12);
sized_array_query_impl!(13);
sized_array_query_impl!(14);
sized_array_query_impl!(15);
sized_array_query_impl!(16);
