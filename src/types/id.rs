use crate::bits::bitfield::Bitfield;
use crate::traits::query::Query;
use crate::traits::record::Record;
use crate::traits::valid_key::BorrowedKey;
use crate::traits::valid_key::ValidKey;
use crate::types::chunk_storage::ChunkStorage;
use crate::types::storage::Storage;
use std::borrow::Borrow;
use std::borrow::Cow;

/// The nullary `ID`. Use this as the starting point to construct new IDs from scratch, like this:
/// ```
/// use retriever::prelude::*;
///
/// let my_id = ID.chunk("my-chunk").item(7);
/// ```
pub const ID: Id<(), ()> = Id((), ());

/// A unique `Id` for a data element in `Storage`. An `Id` can be used as the basis for almost
/// any operation, uncluding `Storage::get(...)`, `Storage::entry(...)`, `Storage::query(...)`,
/// `Storage::modify(...)` and `Storage::remove(...)`.
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
/// storage.add((user_id,"username",username.clone()));
/// storage.add((user_id,"password",old_password.clone()));
/// storage.add((user_id,"admin",admin.clone()));
///
/// let jroberts = ID.chunk(user_id);
///
/// assert_eq!(
///   &(user_id,"username",username.clone()),
///   storage.get(&jroberts.item("username")).unwrap()
/// );
///
/// let new_password = String::from("PASSWORD!6");
///
/// storage.modify(&jroberts.item("password"), |mut editor| {
///   editor.get_mut().2 = String::from(new_password.clone());
/// });
/// assert_eq!(
///   &(user_id,"password",new_password.clone()),
///   storage.get(&jroberts.item("password")).unwrap()
/// );
/// ```
///
#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Id<C, I>(pub C, pub I);

impl<C, I> Id<C, I> {
    /// Construct a new `Id` from a given chunk key and item key.
    pub fn new(chunk_key: C, item_key: I) -> Self {
        Id(chunk_key, item_key)
    }

    /// Set the chunk key of an `Id` to a new value.
    #[must_use = "This method returns a new Id with the given chunk key."]
    pub fn chunk<CC>(self, new_chunk_key: CC) -> Id<CC, I> {
        Id::new(new_chunk_key, self.1)
    }

    /// Set the item key of an `Id` to a new value.
    #[must_use = "This method returns a new Id with the given item key."]
    pub fn item<II>(self, new_item_key: II) -> Id<C, II> {
        Id::new(self.0, new_item_key)
    }
}

impl<'a, ChunkKey, ItemKey> Id<Cow<'a, ChunkKey>, Cow<'a, ItemKey>>
where
    ChunkKey: BorrowedKey + ?Sized,
    ChunkKey::Owned: ValidKey,
    ItemKey: BorrowedKey + ?Sized,
    ItemKey::Owned: ValidKey,
{
    /// Construct the `Id` that matches an existing `Record`. `Id<Cow<Key>>` is usually just as good as
    /// an `Id<Key>` of the owned `Key` itself.  See `Id::cloned` for a way to construct a fully-owned `Id`.
    #[must_use = "This method returns a new Id and otherwise has no effect."]
    pub fn of<R>(record: &'a R) -> Self
    where
        R: Record<ChunkKey, ItemKey>,
    {
        Id::new(record.chunk_key(), record.item_key())
    }
}

impl<C, I> Id<C, I> {
    /// Construct a fully-owned `Id` that matches an existing `Record`.
    /// This works from any valid `Record`, including an `Id` of `Cow<Key>` or `&Key`.
    #[must_use = "This method returns a new Id and otherwise has no effect."]
    pub fn cloned<ChunkKey, ItemKey, R>(record: &R) -> Self
    where
        ChunkKey: ToOwned<Owned = C> + ?Sized,
        ItemKey: ToOwned<Owned = I> + ?Sized,
        C: Borrow<ChunkKey> + Clone,
        I: Borrow<ItemKey> + Clone,
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
    ChunkKey: BorrowedKey + ?Sized,
    ItemKey: BorrowedKey + ?Sized,
    ChunkKey::Owned: ValidKey + Borrow<ChunkKey>,
    ItemKey::Owned: ValidKey + Borrow<ItemKey>,
    C: ValidKey + Borrow<ChunkKey>,
    I: ValidKey + Borrow<ItemKey>,
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
    Element: Record<ChunkKey, ItemKey>,
    ChunkKey: BorrowedKey + ?Sized,
    ItemKey: BorrowedKey + ?Sized,
    ChunkKey::Owned: ValidKey + Borrow<ChunkKey>,
    ItemKey::Owned: ValidKey + Borrow<ItemKey>,
    C: ValidKey + Borrow<ChunkKey>,
    I: ValidKey + Borrow<ItemKey>,
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
