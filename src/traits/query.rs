use crate::traits::idxset::IdxSet;
use crate::traits::record::Record;
use crate::traits::valid_key::ValidKey;
use crate::types::chunk_storage::ChunkStorage;
use crate::types::storage::Storage;
use std::borrow::Borrow;
use std::borrow::Cow;
use std::fmt::Debug;
use std::hash::Hash;
use std::rc::Rc;
use std::sync::Arc;

/// A Query defines a subset of the chunks and a subset of the data elements in each chunk.
pub trait Query<ChunkKey, ItemKey, Element> {
    /// An IdxSet representing the internal chunk indices that will be visited during this Query.
    type ChunkIdxSet: IdxSet;
    /// An IdxSet representing the internal item indices that will be visited during this Query.
    type ItemIdxSet: IdxSet;

    /// Determine which chunks are part of this Query.
    fn chunk_idxs(&self, storage: &Storage<ChunkKey, ItemKey, Element>) -> Self::ChunkIdxSet;

    /// Determine which data elements of a particular chunk are part of this query.
    fn item_idxs(
        &self,
        chunk_key: &ChunkKey,
        chunk_storage: &ChunkStorage<ChunkKey, ItemKey, Element>,
    ) -> Self::ItemIdxSet;

    /// Test whether or not a particular data element actually belongs to this query.
    fn test(&self, element: &Element) -> bool;

    /// Filter this query according to some predicate.
    fn filter<F>(self, f: F) -> crate::queries::filter::Filter<Self, F>
    where
        Self: Sized,
        F: Fn(&Element) -> bool + 'static,
    {
        crate::queries::filter::Filter::new(self, f)
    }

    /// Filter this Query by matching against a secondary index.
    ///
    /// ```
    /// use retriever::prelude::*;
    /// use std::borrow::Cow;
    ///
    /// struct Kitten {
    ///   name: String,
    ///   colors: Vec<String>,
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
    /// let mut by_color : SecondaryIndex<(),Kitten,Vec<String>,String> =
    /// SecondaryIndex::new(&storage, |kitten: &Kitten| Cow::Borrowed(&kitten.colors));
    ///
    /// storage.add(Kitten {
    ///   name: String::from("mittens"),
    ///   colors: vec![String::from("black"), String::from("white")]
    /// });
    ///
    /// storage.add(Kitten {
    ///   name: String::from("furball"),
    ///   colors: vec![String::from("orange")]
    /// });
    ///
    /// storage.add(Kitten {
    ///   name: String::from("midnight"),
    ///   colors: vec![String::from("black")]
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
    fn matching<'a, B, IndexKeys, IndexKey>(
        self,
        secondary_index: &'a crate::queries::secondary_index::SecondaryIndex<
            ChunkKey,
            Element,
            IndexKeys,
            IndexKey,
        >,
        key: Cow<'a, B>,
    ) -> crate::queries::secondary_index::MatchingSecondaryIndex<
        'a,
        Self,
        B,
        ChunkKey,
        Element,
        IndexKeys,
        IndexKey,
    >
    where
        Self: Sized,
        ChunkKey: ValidKey,
        ItemKey: ValidKey,
        Element: Record<ChunkKey, ItemKey>,
        IndexKeys: Clone + Debug + Default + Eq,
        IndexKey: ValidKey + Borrow<B>,
        B: ToOwned + Hash + Eq + ?Sized + 'a,
        &'a B: ValidKey,
        for<'z> &'z IndexKeys: IntoIterator<Item = &'z IndexKey>,
    {
        crate::queries::secondary_index::MatchingSecondaryIndex::new(self, secondary_index, key)
    }
}

impl<'a, Q, ChunkKey, ItemKey, Element> Query<ChunkKey, ItemKey, Element> for &'a Q
where
    Q: Query<ChunkKey, ItemKey, Element>,
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

impl<Q, ChunkKey, ItemKey, Element> Query<ChunkKey, ItemKey, Element> for Rc<Q>
where
    Q: Query<ChunkKey, ItemKey, Element>,
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

impl<Q, ChunkKey, ItemKey, Element> Query<ChunkKey, ItemKey, Element> for Arc<Q>
where
    Q: Query<ChunkKey, ItemKey, Element>,
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
