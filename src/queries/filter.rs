use crate::traits::query::Query;
use crate::traits::record::Record;
use crate::traits::valid_key::{BorrowedKey, ValidKey};
use crate::types::chunk_storage::ChunkStorage;
use crate::types::storage::Storage;

/// Filter a `Query` by a predicate.
#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Filter<Q, F> {
    parent: Q,
    filter: F,
}

impl<Q, F> Filter<Q, F> {
    /// Construct a new filter query. You probably don't want to call this constructor directly.
    /// Prefer the `Query::filter` method instead.
    pub fn new(parent: Q, filter: F) -> Self {
        Filter { parent, filter }
    }
}

impl<ChunkKey, ItemKey, Element, Q, F> Query<ChunkKey, ItemKey, Element> for Filter<Q, F>
where
    ChunkKey: BorrowedKey + ?Sized,
    ChunkKey::Owned: ValidKey,
    ItemKey: BorrowedKey + ?Sized,
    ItemKey::Owned: ValidKey,
    Element: Record<ChunkKey, ItemKey>,
    Q: Query<ChunkKey, ItemKey, Element>,
    F: Fn(&Element) -> bool,
{
    type ChunkIdxSet = Q::ChunkIdxSet;
    type ItemIdxSet = Q::ItemIdxSet;

    fn chunk_idxs(&self, storage: &Storage<ChunkKey, ItemKey, Element>) -> Self::ChunkIdxSet {
        self.parent.chunk_idxs(storage)
    }

    fn item_idxs(
        &self,
        chunk_key: &ChunkKey,
        chunk_storage: &ChunkStorage<ChunkKey, ItemKey, Element>,
    ) -> Self::ItemIdxSet {
        self.parent.item_idxs(chunk_key, chunk_storage)
    }

    fn test(&self, element: &Element) -> bool {
        self.parent.test(element) && (self.filter)(element)
    }
}
