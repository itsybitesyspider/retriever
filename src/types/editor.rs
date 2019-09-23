use super::chunk_storage::ChunkStorage;
use super::id::Id;
use crate::traits::record::Record;
use crate::traits::valid_key::ValidKey;

/// An Editor for a mutable element.
pub struct Editor<'a, ChunkKey, ItemKey, Element> {
    id: Id<&'a ChunkKey, &'a ItemKey>,
    idx: usize,
    storage: &'a mut ChunkStorage<ChunkKey, ItemKey, Element>,
}

impl<'a, ChunkKey, ItemKey, Element> Editor<'a, ChunkKey, ItemKey, Element>
where
    ChunkKey: ValidKey,
    ItemKey: ValidKey,
    Element: Record<ChunkKey, ItemKey>,
{
    pub(super) fn new<'x>(
        id: Id<&'a ChunkKey, &'a ItemKey>,
        idx: usize,
        storage: &'x mut ChunkStorage<ChunkKey, ItemKey, Element>,
    ) -> Self
    where
        'x: 'a,
    {
        Editor { id, idx, storage }
    }

    /// Returns this element's UniqueId
    pub fn id(&self) -> &Id<&'a ChunkKey, &'a ItemKey> {
        &self.id
    }

    /// Modify the element that backs this editor
    pub fn modify<F>(self, f: F) -> Self
    where
        F: FnOnce(&mut Element),
    {
        let element = self.storage.get_idx_mut(self.idx);
        f(element);
        self
    }

    /// Get a reference to the element that backs this editor
    pub fn get(&self) -> &Element {
        self.storage.get_idx(self.idx)
    }

    /// Get a mutable reference to the element that backs this editor
    pub fn get_mut(&mut self) -> &mut Element {
        self.storage.get_idx_mut(self.idx)
    }
}
