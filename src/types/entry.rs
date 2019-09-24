use super::chunk_storage::ChunkStorage;
use super::id::Id;
use crate::traits::record::Record;
use crate::traits::valid_key::ValidKey;
use std::borrow::Cow;

/// A mutable Entry to an element.
pub struct Entry<'a, ChunkKey, ItemKey, Element>
where
    ChunkKey: Clone,
    ItemKey: Clone,
{
    id: Id<Cow<'a, ChunkKey>, Cow<'a, ItemKey>>,
    idx: Option<usize>,
    storage: &'a mut ChunkStorage<ChunkKey, ItemKey, Element>,
}

impl<'a, ChunkKey, ItemKey, Element> Entry<'a, ChunkKey, ItemKey, Element>
where
    ChunkKey: ValidKey,
    ItemKey: ValidKey,
    Element: Record<ChunkKey, ItemKey>,
{
    pub(super) fn new(
        id: Id<Cow<'a, ChunkKey>, Cow<'a, ItemKey>>,
        idx: Option<usize>,
        storage: &'a mut ChunkStorage<ChunkKey, ItemKey, Element>,
    ) -> Self {
        Entry { id, idx, storage }
    }

    /// Returns this entry's UniqueId
    pub fn id(&self) -> &Id<Cow<'a, ChunkKey>, Cow<'a, ItemKey>> {
        &self.id
    }

    /// Insert a record at this entry if it does not exist.
    pub fn or_insert_with<F>(mut self, f: F) -> Self
    where
        F: FnOnce() -> Element,
    {
        if self.idx.is_none() {
            let new_value: Element = f();
            let new_chunk_key: Cow<ChunkKey> = new_value.chunk_key();
            let old_chunk_key: Cow<ChunkKey> = Record::<ChunkKey, ItemKey>::chunk_key(&self.id);
            let new_item_key: Cow<ItemKey> = new_value.item_key();
            let old_item_key: Cow<ItemKey> = Record::<ChunkKey, ItemKey>::item_key(&self.id);
            assert_eq!(
                new_chunk_key, old_chunk_key,
                "entry: inserted chunk key does not match entry chunk key"
            );
            assert_eq!(
                new_item_key, old_item_key,
                "entry: inserted item key does not match entry item key"
            );
            self.idx = Some(self.storage.add(new_value));
        }

        self
    }

    /// Modify this entry if it exists.
    pub fn and_modify<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut Element),
    {
        if let Some(element) = self.get_mut() {
            f(element);
        }

        self
    }

    /// Panic if this entry doesn't exist
    pub fn or_panic(self) -> Self {
        self.get().or_else(|| {
            panic!(format!(
                "retriever: Entry::or_panic(): {:?} doesn't exist",
                self.id
            ))
        });

        self
    }

    /// Get a reference to the element that backs this entry
    pub fn get(&self) -> Option<&Element> {
        self.idx.map(|idx| self.storage.get_idx(idx))
    }

    /// Get a reference to the element that backs this entry
    pub fn get_mut(&mut self) -> Option<&mut Element> {
        self.idx.map(move |idx| self.storage.get_idx_mut(idx))
    }

    /// Remove and return the element that backs this entry
    pub fn remove(mut self) -> Option<Element> {
        let idx = self.idx?;
        self.idx = None;
        Some(self.storage.remove_idx(idx))
    }

    /// Remove and return the element, only if the predicate is true
    pub fn remove_if<F>(self, mut f: F) -> Option<Element>
    where
        F: FnMut(&Element) -> bool,
    {
        let should_remove = if let Some(e) = self.get() {
            f(e)
        } else {
            false
        };

        if should_remove {
            self.remove()
        } else {
            None
        }
    }
}
