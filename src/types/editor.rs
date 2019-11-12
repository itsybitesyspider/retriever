use super::chunk_storage::ChunkStorage;
use super::id::Id;
use crate::traits::record::Record;
use crate::traits::valid_key::ValidKey;

/// An Editor for an element. An instance of `Editor` is proof that the backing element
/// exists in `Storage`, and allows unlimited mutation (but not removal) of that element.
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

    /// Returns this element's unique `Id`.
    pub fn id(&self) -> &Id<&'a ChunkKey, &'a ItemKey> {
        &self.id
    }

    /// Modify this element.
    ///
    /// For efficiency, try not to call modify until you're absolutely sure you need it. Once you
    /// obtain a mutable reference to the element, it must updated in all indices, which costs
    /// time and memory.
    pub fn modify<F>(self, f: F) -> Self
    where
        F: FnOnce(&mut Element),
    {
        let element = self.storage.get_idx_mut(self.idx);
        f(element);
        self
    }

    /// Get a reference to this element.
    pub fn get(&self) -> &Element {
        self.storage.get_idx(self.idx)
    }

    /// Get a mutable reference to this element.
    ///
    /// For efficiency, try not to call get_mut until you're absolutely sure you need it. Once you
    /// obtain a mutable reference to the element, it must updated in all indices, which costs
    /// time and memory.
    pub fn get_mut(&mut self) -> &mut Element {
        self.storage.get_idx_mut(self.idx)
    }
}
