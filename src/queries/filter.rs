use crate::internal::idxset::IdxSet;
use crate::traits::query::Query;
use crate::traits::record::Record;
use crate::traits::valid_key::ValidKey;
use crate::types::chunk_storage::ChunkStorage;
use crate::types::storage::Storage;

/// Query every element.
#[derive(Clone, Copy)]
pub struct Filter<Q, F> {
    parent: Q,
    filter: F,
}

impl<Q, F> Filter<Q, F> {
    /// Construct a new filter. Prefer the Query::filter trait method instead.
    pub fn new(parent: Q, filter: F) -> Self {
        Filter { parent, filter }
    }
}

impl<ChunkKey, ItemKey, Element, Q, F> Query<ChunkKey, ItemKey, Element> for Filter<Q, F>
where
    ChunkKey: ValidKey,
    ItemKey: ValidKey,
    Element: Record<ChunkKey, ItemKey>,
    Q: Query<ChunkKey, ItemKey, Element>,
    F: Fn(&Element) -> bool,
{
    fn chunk_idxs(&self, storage: &Storage<ChunkKey, ItemKey, Element>) -> IdxSet {
        self.parent.chunk_idxs(storage)
    }

    fn item_idxs(
        &self,
        chunk_key: &ChunkKey,
        chunk_storage: &ChunkStorage<ChunkKey, ItemKey, Element>,
    ) -> IdxSet {
        self.parent.item_idxs(chunk_key, chunk_storage)
    }

    fn test(&self, element: &Element) -> bool {
        self.parent.test(element) && (self.filter)(element)
    }
}
