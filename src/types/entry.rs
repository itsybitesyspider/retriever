use super::chunk_storage::ChunkStorage;
use crate::traits::record::Record;
use crate::traits::valid_key::{BorrowedKey, ValidKey};
use std::borrow::Cow;
use std::fmt::Debug;

/// An `Entry` for an element. This is intended to work in the same way as the `Entries` from
/// rust's standard collections API. An `Entry` refers to an element that we have tried to
/// look up and might or might not have found.
///
/// # Type Parameters
///
/// * `R`: The key used to lookup this `Entry`
/// * `ChunkKey`: The chunk key type of the backing `Storage`
/// * `ItemKey`: The item key type of the backing `Storage`
/// * `Element`: The element type of the backing `Storage`, and also the `Element` represented
///   by this `Entry`.
pub struct Entry<'a, R, ChunkKey: ?Sized, ItemKey: ?Sized, Element>
where
    R: Record<ChunkKey, ItemKey> + 'a,
    ChunkKey: BorrowedKey,
    ChunkKey::Owned: ValidKey,
    ItemKey: BorrowedKey,
    ItemKey::Owned: ValidKey,
{
    id: R,
    idx: Option<usize>,
    storage: &'a mut ChunkStorage<ChunkKey, ItemKey, Element>,
}

impl<'a, R, ChunkKey, ItemKey, Element> Entry<'a, R, ChunkKey, ItemKey, Element>
where
    R: Record<ChunkKey, ItemKey> + 'a,
    ChunkKey: BorrowedKey + ?Sized,
    ChunkKey::Owned: ValidKey,
    ItemKey: BorrowedKey + ?Sized,
    ItemKey::Owned: ValidKey,
    Element: Record<ChunkKey, ItemKey>,
{
    pub(super) fn new(
        id: R,
        idx: Option<usize>,
        storage: &'a mut ChunkStorage<ChunkKey, ItemKey, Element>,
    ) -> Self {
        Entry { id, idx, storage }
    }

    /// Returns the value whose `ChunkKey` and `ItemKey` is used to look up this `Entry`.
    pub fn id(&self) -> &R {
        &self.id
    }

    /// Insert a record at this entry if it does not already exist.
    pub fn or_insert_with<F>(mut self, f: F) -> &'a mut Element
    where
        F: FnOnce() -> Element,
    {
        if let Some(idx) = self.idx {
            self.storage.get_idx_mut(idx)
        } else {
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
            let idx = self.storage.add(new_value);
            self.idx = Some(idx);

            self.storage.get_idx_mut(idx)
        }
    }

    /// Modify this element if it exists. If the element does not exist, nothing happens.
    pub fn and_modify<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut Element),
    {
        if let Some(element) = self.get_mut() {
            f(element);
        }

        self
    }

    /// Panic if this element doesn't exist in `Storage`.
    pub fn or_panic(self) -> Self
    where
        R: Debug,
    {
        self.get().or_else(|| {
            panic!(format!(
                "retriever: Entry::or_panic(): {:?} doesn't exist",
                &self.id
            ))
        });

        self
    }

    /// Get a reference to the element.
    pub fn get(&self) -> Option<&Element> {
        self.idx.map(|idx| self.storage.get_idx(idx))
    }

    /// Get a mutable reference to the element.
    pub fn get_mut(&mut self) -> Option<&mut Element> {
        self.idx.map(move |idx| self.storage.get_idx_mut(idx))
    }

    /// Remove and return the element.
    pub fn remove(mut self) -> Option<Element> {
        let idx = self.idx?;
        self.idx = None;
        Some(self.storage.remove_idx(idx))
    }

    /// Remove and return the element, only if the predicate is true.
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
