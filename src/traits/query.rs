use crate::internal::idxset::IdxSet;
use crate::traits::record::Record;
use crate::traits::valid_key::ValidKey;
use crate::types::chunk_storage::ChunkStorage;
use crate::types::storage::Storage;
use std::borrow::Borrow;
use std::hash::Hash;

/// A Query defines a subset of the chunks and a subset of the data elements in each chunk.
pub trait Query<ChunkKey, ItemKey, Element> {
    /// Determine which chunks are part of this Query.
    fn chunk_idxs(&self, storage: &Storage<ChunkKey, ItemKey, Element>) -> IdxSet;

    /// Determine which data elements of a particular chunk are part of this query.
    fn item_idxs(
        &self,
        chunk_key: &ChunkKey,
        chunk_storage: &ChunkStorage<ChunkKey, ItemKey, Element>,
    ) -> IdxSet;

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
    /// use retriever::*;
    /// use retriever::queries::everything::*;
    /// use retriever::queries::secondary_index::*;
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
    /// SecondaryIndex::new_expensive(&storage, |kitten: &Kitten| {
    ///   kitten.colors.clone()
    /// });
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
    /// for kitten in storage.query(&Everything.matching(&mut by_color, "orange")) {
    ///   assert_eq!("furball", &kitten.name);
    /// }
    ///
    /// assert_eq!(2, storage.query(&Everything.matching(&mut by_color, "black")).count());
    ///
    /// # storage.validate();
    /// # by_color.validate(&mut storage);
    /// ```
    fn matching<'a, B, IndexKeys, IndexKey>(
        self,
        secondary_index: &'a mut crate::queries::secondary_index::SecondaryIndex<
            ChunkKey,
            Element,
            IndexKeys,
            IndexKey,
        >,
        key: &'a B,
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
        IndexKeys: ValidKey + Default,
        IndexKey: ValidKey + Default + Borrow<B>,
        B: Hash + Eq + 'a + ?Sized,
        for<'z> &'z IndexKeys: IntoIterator<Item = &'z IndexKey>,
    {
        crate::queries::secondary_index::MatchingSecondaryIndex::new(self, secondary_index, key)
    }
}
