use crate::internal::idxset::IdxSet;
use crate::traits::query::Query;
use crate::traits::record::Record;
use crate::traits::valid_key::ValidKey;
use crate::types::chunk_storage::ChunkStorage;
use crate::types::storage::Storage;
use std::borrow::Cow;

/// The nullary ID. Use to construct ids.
pub const ID: Id<(), ()> = Id((), ());

/// A unique Id for a data element in Storage
#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Id<ChunkKey, ItemKey>(pub ChunkKey, pub ItemKey);

impl<ChunkKey, ItemKey> Id<ChunkKey, ItemKey>
where
    ChunkKey: ValidKey,
    ItemKey: ValidKey,
{
    /// Construct a new Id from a given chunk key and item key.
    pub fn new(chunk_key: ChunkKey, item_key: ItemKey) -> Self {
        Id(chunk_key, item_key)
    }

    /// Set the chunk key of an Id to a new value with a new type.
    pub fn chunk<NewChunkKey>(self, new_chunk_key: NewChunkKey) -> Id<NewChunkKey, ItemKey>
    where
        NewChunkKey: ValidKey,
    {
        Id::new(new_chunk_key, self.1)
    }

    /// Set the chunk key of an Id to a new value with a new type.
    pub fn item<NewItemKey>(self, new_item_key: NewItemKey) -> Id<ChunkKey, NewItemKey>
    where
        NewItemKey: ValidKey,
    {
        Id::new(self.0, new_item_key)
    }

    /// Construct a new Id from an existing record
    pub fn of<'a, R>(record: &'a R) -> Id<Cow<'a, ChunkKey>, Cow<'a, ItemKey>>
    where
        R: Record<ChunkKey, ItemKey>,
    {
        Id::new(record.chunk_key(), record.item_key())
    }

    /// Clone a Id and it's component keys
    pub fn cloned<R>(record: &R) -> Self
    where
        R: Record<ChunkKey, ItemKey>,
    {
        Self::new(
            record.chunk_key().into_owned(),
            record.item_key().into_owned(),
        )
    }

    /// Convert Cow keys into owned keys
    pub fn from_cows<'a>(borrowed: Id<Cow<'a, ChunkKey>, Cow<'a, ItemKey>>) -> Self {
        Self::new(borrowed.0.into_owned(), borrowed.1.into_owned())
    }
}

impl<ChunkKey, ItemKey> Record<ChunkKey, ItemKey> for Id<ChunkKey, ItemKey>
where
    ChunkKey: ValidKey,
    ItemKey: ValidKey,
{
    fn chunk_key(&self) -> Cow<ChunkKey> {
        Cow::Borrowed(&self.0)
    }

    fn item_key(&self) -> Cow<ItemKey> {
        Cow::Borrowed(&self.1)
    }
}

impl<'a, ChunkKey, ItemKey> Record<ChunkKey, ItemKey> for Id<&'a ChunkKey, &'a ItemKey>
where
    ChunkKey: ValidKey,
    ItemKey: ValidKey,
{
    fn chunk_key(&self) -> Cow<ChunkKey> {
        Cow::Borrowed(self.0)
    }

    fn item_key(&self) -> Cow<ItemKey> {
        Cow::Borrowed(self.1)
    }
}

impl<'a, ChunkKey, ItemKey> Record<ChunkKey, ItemKey> for Id<Cow<'a, ChunkKey>, Cow<'a, ItemKey>>
where
    ChunkKey: ValidKey,
    ItemKey: ValidKey,
{
    fn chunk_key(&self) -> Cow<ChunkKey> {
        self.0.clone()
    }

    fn item_key(&self) -> Cow<ItemKey> {
        self.1.clone()
    }
}

impl<'a, ChunkKey, ItemKey, Element> Query<ChunkKey, ItemKey, Element> for Id<ChunkKey, ItemKey>
where
    ChunkKey: ValidKey,
    ItemKey: ValidKey,
    Element: Record<ChunkKey, ItemKey>,
{
    fn chunk_idxs(&self, storage: &Storage<ChunkKey, ItemKey, Element>) -> IdxSet {
        IdxSet::from(storage.internal_idx_of(&self.chunk_key()))
    }

    fn item_idxs(
        &self,
        _chunk_key: &ChunkKey,
        chunk_storage: &ChunkStorage<ChunkKey, ItemKey, Element>,
    ) -> IdxSet {
        IdxSet::from(chunk_storage.internal_idx_of(&self.item_key()))
    }

    fn test(&self, element: &Element) -> bool {
        assert!(element.chunk_key() == self.chunk_key() && element.item_key() == self.item_key());
        true
    }
}
