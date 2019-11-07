use crate::idxsets::idxrange::IdxRange;
use crate::traits::query::Query;
use crate::traits::record::Record;
use crate::traits::valid_key::ValidKey;
use crate::types::chunk_storage::ChunkStorage;
use crate::types::storage::Storage;

/// A query that visits every record in storage.
#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Everything;

impl<ChunkKey, ItemKey, Element> Query<ChunkKey, ItemKey, Element> for Everything
where
    ChunkKey: ValidKey,
    ItemKey: ValidKey,
    Element: Record<ChunkKey, ItemKey>,
{
    type ChunkIdxSet = IdxRange;
    type ItemIdxSet = IdxRange;

    fn chunk_idxs(&self, storage: &Storage<ChunkKey, ItemKey, Element>) -> Self::ChunkIdxSet {
        IdxRange(0..storage.internal_rvec().len())
    }

    fn item_idxs(
        &self,
        _chunk_key: &ChunkKey,
        chunk_storage: &ChunkStorage<ChunkKey, ItemKey, Element>,
    ) -> Self::ItemIdxSet {
        IdxRange(0..chunk_storage.len())
    }

    fn test(&self, _element: &Element) -> bool {
        true
    }
}
