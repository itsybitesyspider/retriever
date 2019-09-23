use crate::internal::bitset::Bitset;
use crate::internal::idxset::IdxSet;
use crate::traits::query::Query;
use crate::traits::record::Record;
use crate::traits::valid_key::ValidKey;
use crate::types::chunk_storage::ChunkStorage;
use crate::types::storage::Storage;

/// Filter by chunks
pub struct Chunks<'a, ChunkKey>(pub &'a [ChunkKey]);

impl<'a, ChunkKey, ItemKey, Element> Query<ChunkKey, ItemKey, Element> for Chunks<'a, ChunkKey>
where
    ChunkKey: ValidKey,
    ItemKey: ValidKey,
    Element: Record<ChunkKey, ItemKey>,
{
    fn chunk_idxs(&self, storage: &Storage<ChunkKey, ItemKey, Element>) -> IdxSet {
        let bitset: Bitset = self
            .0
            .iter()
            .filter_map(|x| storage.internal_idx_of(x))
            .collect();
        IdxSet::from(bitset)
    }

    fn item_idxs(
        &self,
        _chunk_key: &ChunkKey,
        chunk_storage: &ChunkStorage<ChunkKey, ItemKey, Element>,
    ) -> IdxSet {
        IdxSet::from_length(chunk_storage.len())
    }

    fn test(&self, _element: &Element) -> bool {
        true
    }
}
