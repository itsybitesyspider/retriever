use crate::internal::idxset::IdxSet;
use crate::traits::query::Query;
use crate::traits::record::Record;
use crate::traits::valid_key::ValidKey;
use crate::types::chunk_storage::ChunkStorage;
use crate::types::storage::Storage;

/// Query every element.
#[derive(Clone, Copy)]
pub struct Everything;

impl<ChunkKey, ItemKey, Element> Query<ChunkKey, ItemKey, Element> for Everything
where
    ChunkKey: ValidKey,
    ItemKey: ValidKey,
    Element: Record<ChunkKey, ItemKey>,
{
    fn chunk_idxs(&self, storage: &Storage<ChunkKey, ItemKey, Element>) -> IdxSet {
        IdxSet::from_length(storage.chunk_keys().into_iter().count())
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
