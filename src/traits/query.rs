use crate::queries::secondary_index::KeySet;
use crate::traits::idxset::IdxSet;
use crate::traits::record::Record;
use crate::traits::valid_key::{BorrowedKey, ValidKey};
use crate::types::chunk_storage::ChunkStorage;
use crate::types::storage::Storage;
use std::borrow::Borrow;
use std::borrow::Cow;
use std::fmt::Debug;
use std::rc::Rc;
use std::sync::Arc;

/// A `Query` defines a subset of the chunks and a subset of the data elements in each chunk.
/// Use a `Query` to iterate, modify, or remove said elements.
pub trait Query<ChunkKey, ItemKey, Element>
where
    ChunkKey: BorrowedKey + ?Sized,
    ChunkKey::Owned: ValidKey,
    ItemKey: BorrowedKey + ?Sized,
    ItemKey::Owned: ValidKey,
{
    /// An `IdxSet` representing the internal chunk indices that will be visited during this Query.
    type ChunkIdxSet: IdxSet;
    /// An `IdxSet` representing the internal item indices that will be visited during this Query.
    type ItemIdxSet: IdxSet;

    /// Determine which chunks are part of this `Query`.
    fn chunk_idxs(&self, storage: &Storage<ChunkKey, ItemKey, Element>) -> Self::ChunkIdxSet;

    /// Determine which data elements of a particular chunk are part of this `Query`.
    fn item_idxs(
        &self,
        chunk_key: &ChunkKey,
        chunk_storage: &ChunkStorage<ChunkKey, ItemKey, Element>,
    ) -> Self::ItemIdxSet;

    /// Test whether or not a particular data element actually belongs to this `Query`.
    fn test(&self, element: &Element) -> bool;

    /// Filter this `Query` according to some predicate.
    fn filter<F>(self, f: F) -> crate::queries::filter::Filter<Self, F>
    where
        Self: Sized,
        F: Fn(&Element) -> bool + 'static,
    {
        crate::queries::filter::Filter::new(self, f)
    }

    /// Filter this `Query` by matching against a `SecondaryIndex`.
    ///
    /// ```
    /// use retriever::prelude::*;
    /// use std::borrow::Cow;
    /// use std::collections::HashSet;
    ///
    /// struct Kitten {
    ///   name: String,
    ///   colors: HashSet<String>,
    /// }
    ///
    /// impl Record<(), String> for Kitten {
    ///   fn chunk_key(self: &Kitten) -> Cow<()> {
    ///     Cow::Owned(())
    ///   }
    ///
    ///   fn item_key(self: &Kitten) -> Cow<String> {
    ///     Cow::Borrowed(&self.name)
    ///   }
    /// }
    ///
    /// let mut storage : Storage<(), String, Kitten> = Storage::new();
    ///
    /// let mut by_color : SecondaryIndex<(),Kitten,HashSet<String>,str> =
    /// SecondaryIndex::new(&storage, |kitten: &Kitten| Cow::Borrowed(&kitten.colors));
    ///
    /// storage.add(Kitten {
    ///   name: String::from("mittens"),
    ///   colors: vec![String::from("black"), String::from("white")].into_iter().collect()
    /// });
    ///
    /// storage.add(Kitten {
    ///   name: String::from("furball"),
    ///   colors: vec![String::from("orange")].into_iter().collect()
    /// });
    ///
    /// storage.add(Kitten {
    ///   name: String::from("midnight"),
    ///   colors: vec![String::from("black")].into_iter().collect()
    /// });
    ///
    /// for kitten in storage.query(&Everything.matching(&mut by_color, Cow::Borrowed("orange"))) {
    ///   assert_eq!("furball", &kitten.name);
    /// }
    ///
    /// assert_eq!(2, storage.query(&Everything.matching(&mut by_color, Cow::Borrowed("black"))).count());
    ///
    /// # storage.validate();
    /// # by_color.validate(&mut storage);
    /// ```
    fn matching<'a, IndexKeys, IndexKey>(
        self,
        secondary_index: &'a crate::queries::secondary_index::SecondaryIndex<
            ChunkKey,
            Element,
            IndexKeys,
            IndexKey,
        >,
        key: Cow<'a, IndexKey>,
    ) -> crate::queries::secondary_index::MatchingSecondaryIndex<
        'a,
        Self,
        ChunkKey,
        Element,
        IndexKeys,
        IndexKey,
    >
    where
        Self: Sized,
        IndexKey: BorrowedKey + ?Sized,
        IndexKey::Owned: ValidKey,
        Element: Record<ChunkKey, ItemKey>,
        for<'k> IndexKeys: Clone + Debug + Default + Eq + KeySet<'k, IndexKey>,
    {
        crate::queries::secondary_index::MatchingSecondaryIndex::new(self, secondary_index, key)
    }
}

impl<'a, Q, ChunkKey: ToOwned, ItemKey: ToOwned, Element> Query<ChunkKey, ItemKey, Element>
    for &'a Q
where
    Q: Query<ChunkKey, ItemKey, Element>,
    ChunkKey: BorrowedKey + ?Sized,
    ChunkKey::Owned: ValidKey,
    ItemKey: BorrowedKey + ?Sized,
    ItemKey::Owned: ValidKey,
{
    type ChunkIdxSet = Q::ChunkIdxSet;
    type ItemIdxSet = Q::ItemIdxSet;

    fn chunk_idxs(&self, storage: &Storage<ChunkKey, ItemKey, Element>) -> Self::ChunkIdxSet {
        Q::chunk_idxs(self, storage)
    }

    fn item_idxs(
        &self,
        chunk_key: &ChunkKey,
        chunk_storage: &ChunkStorage<ChunkKey, ItemKey, Element>,
    ) -> Self::ItemIdxSet {
        Q::item_idxs(self, chunk_key, chunk_storage)
    }

    fn test(&self, element: &Element) -> bool {
        Q::test(self, element)
    }
}

impl<Q, ChunkKey: ToOwned, ItemKey: ToOwned, Element> Query<ChunkKey, ItemKey, Element> for Rc<Q>
where
    Q: Query<ChunkKey, ItemKey, Element>,
    ChunkKey: BorrowedKey + ?Sized,
    ChunkKey::Owned: ValidKey,
    ItemKey: BorrowedKey + ?Sized,
    ItemKey::Owned: ValidKey,
{
    type ChunkIdxSet = Q::ChunkIdxSet;
    type ItemIdxSet = Q::ItemIdxSet;

    fn chunk_idxs(&self, storage: &Storage<ChunkKey, ItemKey, Element>) -> Self::ChunkIdxSet {
        Q::chunk_idxs(Rc::as_ref(self), storage)
    }

    fn item_idxs(
        &self,
        chunk_key: &ChunkKey,
        chunk_storage: &ChunkStorage<ChunkKey, ItemKey, Element>,
    ) -> Self::ItemIdxSet {
        Q::item_idxs(Rc::as_ref(self), chunk_key, chunk_storage)
    }

    fn test(&self, element: &Element) -> bool {
        Q::test(Rc::as_ref(self), element)
    }
}

impl<Q, ChunkKey: ToOwned, ItemKey: ToOwned, Element> Query<ChunkKey, ItemKey, Element> for Arc<Q>
where
    Q: Query<ChunkKey, ItemKey, Element>,
    ChunkKey: BorrowedKey + ?Sized,
    ChunkKey::Owned: ValidKey,
    ItemKey: BorrowedKey + ?Sized,
    ItemKey::Owned: ValidKey,
{
    type ChunkIdxSet = Q::ChunkIdxSet;
    type ItemIdxSet = Q::ItemIdxSet;

    fn chunk_idxs(&self, storage: &Storage<ChunkKey, ItemKey, Element>) -> Self::ChunkIdxSet {
        Q::chunk_idxs(Arc::as_ref(self), storage)
    }

    fn item_idxs(
        &self,
        chunk_key: &ChunkKey,
        chunk_storage: &ChunkStorage<ChunkKey, ItemKey, Element>,
    ) -> Self::ItemIdxSet {
        Q::item_idxs(Arc::as_ref(self), chunk_key, chunk_storage)
    }

    fn test(&self, element: &Element) -> bool {
        Q::test(Arc::as_ref(self), element)
    }
}

impl<'a, Q, ChunkKey, ItemKey, Element> Query<ChunkKey, ItemKey, Element> for Cow<'a, Q>
where
    Q: Query<ChunkKey, ItemKey, Element> + Clone,
    ChunkKey: BorrowedKey + ?Sized,
    ChunkKey::Owned: ValidKey,
    ItemKey: BorrowedKey + ?Sized,
    ItemKey::Owned: ValidKey,
{
    type ChunkIdxSet = Q::ChunkIdxSet;
    type ItemIdxSet = Q::ItemIdxSet;

    fn chunk_idxs(&self, storage: &Storage<ChunkKey, ItemKey, Element>) -> Self::ChunkIdxSet {
        Q::chunk_idxs(Cow::borrow(self), storage)
    }

    fn item_idxs(
        &self,
        chunk_key: &ChunkKey,
        chunk_storage: &ChunkStorage<ChunkKey, ItemKey, Element>,
    ) -> Self::ItemIdxSet {
        Q::item_idxs(Cow::borrow(self), chunk_key, chunk_storage)
    }

    fn test(&self, element: &Element) -> bool {
        Q::test(Cow::borrow(self), element)
    }
}
