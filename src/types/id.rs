use crate::bits::bitfield::Bitfield;
use crate::traits::query::Query;
use crate::traits::record::Record;
use crate::traits::valid_key::ValidKey;
use crate::types::chunk_storage::ChunkStorage;
use crate::types::storage::Storage;
use std::borrow::Borrow;
use std::borrow::Cow;

/// The nullary ID. Use this as the starting point to construct new IDs from scratch, like this:
/// ```
/// use retriever::prelude::*;
///
/// let my_id = ID.chunk("my-chunk").item(7);
/// ```
pub const ID: Id<(), ()> = Id((), ());

/// A unique `Id` for a data element in `Storage`. An `Id` can be used as the basis for both the
/// `get` or `entry` APIs that match a `Record` or the `query`/`modify`/`remove` APIs that accept a `Query`.
///
/// ```
/// use retriever::prelude::*;
///
/// let mut storage : Storage<u64,&'static str,_> = Storage::new();
///
/// let user_id = 7;
/// let username = String::from("jroberts");
/// let old_password = String::from("PASSWORD!5");
/// let admin = String::from("true");
///
/// storage.add((7,"username",username.clone()));
/// storage.add((7,"password",old_password.clone()));
/// storage.add((7,"admin",admin.clone()));
///
/// let jroberts = ID.chunk(7);
///
/// assert_eq!(
///   &(7,"username",username.clone()),
///   storage.get(&jroberts.item("username")).unwrap()
/// );
///
/// let new_password = String::from("PASSWORD!6");
///
/// storage.modify(&jroberts.item("password"), |mut editor| {
///   editor.get_mut().2 = String::from(new_password.clone());
/// });
/// assert_eq!(
///   &(7,"password",new_password.clone()),
///   storage.get(&jroberts.item("password")).unwrap()
/// );
/// ```
///
#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Id<ChunkKey, ItemKey>(pub ChunkKey, pub ItemKey);

impl<ChunkKey, ItemKey> Id<ChunkKey, ItemKey>
where
    ChunkKey: ValidKey,
    ItemKey: ValidKey,
{
    /// Construct a new `Id` from a given chunk key and item key.
    pub fn new(chunk_key: ChunkKey, item_key: ItemKey) -> Self {
        Id(chunk_key, item_key)
    }

    /// Set the chunk key of an `Id` to a new value. This can be chained to construct `Id`s in fluent style.
    pub fn chunk<NewChunkKey>(self, new_chunk_key: NewChunkKey) -> Id<NewChunkKey, ItemKey>
    where
        NewChunkKey: ValidKey,
    {
        Id::new(new_chunk_key, self.1)
    }

    /// Set the item key of an `Id` to a new value. This can be chained to construct `Id`s in fluent style.
    pub fn item<NewItemKey>(self, new_item_key: NewItemKey) -> Id<ChunkKey, NewItemKey>
    where
        NewItemKey: ValidKey,
    {
        Id::new(self.0, new_item_key)
    }

    /// Construct the `Id` that matches an existing `Record`. `Id<Cow<Key>>` is usually just as good as
    /// an `Id<Key>` of the owned `Key` itself.  See `cloned` for a way to construct a fully-owned `Id`.
    pub fn of<'a, R>(record: &'a R) -> Id<Cow<'a, ChunkKey>, Cow<'a, ItemKey>>
    where
        R: Record<ChunkKey, ItemKey>,
    {
        Id::new(record.chunk_key(), record.item_key())
    }

    /// Construct a fully-owned `Id` that matches an existing `Record`.
    /// This works from any valid `Record`, including an `Id` of `Cow<Key>` or `&Key`.
    pub fn cloned<R>(record: &R) -> Self
    where
        R: Record<ChunkKey, ItemKey>,
    {
        Self::new(
            record.chunk_key().into_owned(),
            record.item_key().into_owned(),
        )
    }
}

impl<ChunkKey, ItemKey, C, I> Record<ChunkKey, ItemKey> for Id<C, I>
where
    ChunkKey: ValidKey,
    ItemKey: ValidKey,
    C: Borrow<ChunkKey>,
    I: Borrow<ItemKey>,
{
    fn chunk_key(&self) -> Cow<ChunkKey> {
        Cow::Borrowed(self.0.borrow())
    }

    fn item_key(&self) -> Cow<ItemKey> {
        Cow::Borrowed(self.1.borrow())
    }
}

impl<'a, ChunkKey, ItemKey, Element, C, I> Query<ChunkKey, ItemKey, Element> for Id<C, I>
where
    Id<C, I>: Record<ChunkKey, ItemKey>,
    ChunkKey: ValidKey,
    ItemKey: ValidKey,
    Element: Record<ChunkKey, ItemKey>,
{
    type ChunkIdxSet = Bitfield;
    type ItemIdxSet = Bitfield;

    fn chunk_idxs(&self, storage: &Storage<ChunkKey, ItemKey, Element>) -> Self::ChunkIdxSet {
        Bitfield::from(storage.internal_idx_of(&self.chunk_key()))
    }

    fn item_idxs(
        &self,
        _chunk_key: &ChunkKey,
        chunk_storage: &ChunkStorage<ChunkKey, ItemKey, Element>,
    ) -> Self::ItemIdxSet {
        Bitfield::from(chunk_storage.internal_idx_of(&self.item_key()))
    }

    fn test(&self, element: &Element) -> bool {
        assert_eq!(self.chunk_key(), element.chunk_key());
        assert_eq!(self.item_key(), element.item_key());
        true
    }
}
