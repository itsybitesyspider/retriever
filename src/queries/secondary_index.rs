use crate::bits::Bitset;
use crate::idxsets::intersection::Intersection;
use crate::internal::mr::rvec::RVec;
use crate::internal::mr::summarize::{Summarize, SummaryRules};
use crate::traits::idxset::IdxSet;
use crate::traits::query::Query;
use crate::traits::record::Record;
use crate::traits::valid_key::ValidKey;
use crate::types::chunk_storage::ChunkStorage;
use crate::types::storage::Storage;
use std::borrow::Borrow;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::RwLock;

/// A Query matching against a `SecondaryIndex`. Construct using `Query::matching`.
///
/// # Type Parameters
///
/// Most of these type parameters match the same parameters of the backing `SecondaryIndex`.
///
/// * `Q`: A `Query`.
/// * `B`: A borrowed form of `IndexKey`. For example, if `IndexKey` is `String` then `B` might be `str`.
/// * `ChunkKey`: Chunk key of the backing `Storage`.
/// * `Element`: Element of the backing `Storage`.
/// * `IndexKeys`: A collection containing elements of type `IndexKey`.
/// * `IndexKey`: The indexing key of the backing `SecondaryIndex`.
///
pub struct MatchingSecondaryIndex<'a, Q, B, ChunkKey, Element, IndexKeys, IndexKey>
where
    B: ToOwned + Hash + Eq + ?Sized + 'a,
    &'a B: ValidKey,
    IndexKey: ValidKey + Borrow<B>,
    IndexKeys: Clone + Debug + Default + Eq,
    for<'y> &'y IndexKeys: IntoIterator<Item = &'y IndexKey>,
{
    query: Q,
    secondary_index: SecondaryIndex<ChunkKey, Element, IndexKeys, IndexKey>,
    index_key: Cow<'a, B>,
}

impl<'a, Q, B, ChunkKey, Element, IndexKeys, IndexKey> Clone
    for MatchingSecondaryIndex<'a, Q, B, ChunkKey, Element, IndexKeys, IndexKey>
where
    B: ToOwned + Hash + Eq + ?Sized + 'a,
    &'a B: ValidKey,
    IndexKey: ValidKey + Borrow<B>,
    IndexKeys: Clone + Debug + Default + Eq,
    for<'y> &'y IndexKeys: IntoIterator<Item = &'y IndexKey>,
    Q: Clone,
    Cow<'a, B>: Clone,
{
    fn clone(&self) -> Self {
        MatchingSecondaryIndex {
            query: self.query.clone(),
            secondary_index: self.secondary_index.clone(),
            index_key: self.index_key.clone(),
        }
    }
}

struct ChunkSecondaryIndex<IndexKey>
where
    IndexKey: ValidKey,
{
    reverse_index: HashMap<IndexKey, Bitset>,
}

/// A secondary index of the records in a `Storage`. You can attach as many `SecondaryIndices`
/// to a given `Storage` as you want. Each `SecondaryIndex` will index each stored element under
/// zero or more key values (but only one key type).
///
/// # Type Parameters
///
/// * `ChunkKey`: The chunk key type of the `Storage`.
/// * `Element`: The element type of the `Storage`.
/// * `IndexKeys`: A collection containing the type parameter `IndexKey`. This could be an `Option`, `HashSet`, etc.
/// * `IndexKey`: The type of the secondary index key. This is the key you'll use to look up `Elements` via this `SecondaryIndex`.
///
/// # How to choose `IndexKeys` and `IndexKey`.
///
/// | Situation                                  | `IndexKeys`          | `IndexKey`         |
/// | ------------------------------------------ | -------------------- | ------------------ |
/// | Index all emails marked "urgent"           | `Option<()>`         | `()`               |
/// | Index automobiles by model year            | `Option<i32>`        | `i32`              |
/// | Index artwork by dominant color            | `HashSet<Color>`     | `Color`            |
///
/// # Stability
///
/// It is likely that the class constraints on `IndexKeys` will change in the future. If so,
/// `Option`, `HashSet`, and `BTreeSet` are relatively safe choices.
///
/// # Panic
///
/// A `SecondaryIndex` is associated with exactly one storage.
/// If you attempt to use a `SecondaryIndex` with a `Storage` other than the one it was
/// initialized with, it will panic.
pub struct SecondaryIndex<ChunkKey, Element, IndexKeys, IndexKey>(
    Arc<RwLock<SecondaryIndexImpl<ChunkKey, Element, IndexKeys, IndexKey>>>,
)
where
    IndexKey: ValidKey,
    IndexKeys: Clone + Debug + Default + Eq,
    for<'x> &'x IndexKeys: IntoIterator<Item = &'x IndexKey>;

impl<ChunkKey, Element, IndexKeys, IndexKey> Clone
    for SecondaryIndex<ChunkKey, Element, IndexKeys, IndexKey>
where
    IndexKey: ValidKey,
    IndexKeys: Clone + Debug + Default + Eq,
    for<'x> &'x IndexKeys: IntoIterator<Item = &'x IndexKey>,
{
    fn clone(&self) -> Self {
        SecondaryIndex(Arc::clone(&self.0))
    }
}

struct SecondaryIndexImpl<ChunkKey, Element, IndexKeys, IndexKey>
where
    IndexKey: ValidKey,
    IndexKeys: Clone + Debug + Default + Eq,
    for<'x> &'x IndexKeys: IntoIterator<Item = &'x IndexKey>,
{
    // parent_id, used to see that this SecondaryIndex isn't suddenly used with a different parent storage
    parent_id: u64,
    // gc_chunk_list, remember the chunks from our last update, so we can remove indices for newly-absent chunks
    gc_chunk_list: RVec<Option<ChunkKey>>,
    // rule for constructing index keys
    rules: Arc<SummaryRules<Element, IndexKeys, ChunkSecondaryIndex<IndexKey>>>,
    // the index itself
    index: HashMap<
        ChunkKey,
        Summarize<Element, IndexKeys, ChunkSecondaryIndex<IndexKey>>,
        crate::internal::hasher::HasherImpl,
    >,
}

impl<ChunkKey, Element, IndexKeys, IndexKey> SecondaryIndex<ChunkKey, Element, IndexKeys, IndexKey>
where
    ChunkKey: ValidKey,
    IndexKey: ValidKey,
    IndexKeys: Clone + Debug + Default + Eq,
    for<'x> &'x IndexKeys: IntoIterator<Item = &'x IndexKey>,
{
    /// Create a new SecondaryIndex of a storage.
    ///
    /// The indexing rule needs to return a collection of 0 or more `IndexKeys` for each `Element`.
    /// Collection types that will work well include: `Option`, `HashSet`, and `BTreeSet`.
    ///
    /// Try to re-use `SecondaryIndices` as much as possible. If you drop a `SecondaryIndex` and then
    /// re-create it, the index has to be rebuilt, which might take a long time.
    pub fn new<I, F>(storage: &Storage<ChunkKey, I, Element>, f: F) -> Self
    where
        I: ValidKey,
        Element: Record<ChunkKey, I>,
        F: Fn(&Element) -> Cow<IndexKeys> + 'static,
    {
        SecondaryIndex(Arc::new(RwLock::new(SecondaryIndexImpl {
            parent_id: storage.id(),
            gc_chunk_list: RVec::default(),
            index: HashMap::with_hasher(crate::internal::hasher::HasherImpl::default()),
            rules: Arc::new(
                SecondaryIndexImpl::<ChunkKey, Element, IndexKeys, IndexKey>::indexing_rules(f),
            ),
        })))
    }

    /// Panic if this storage is malformed or broken in any detectable way.
    /// This is a slow operation and you shouldn't use it unless you suspect a problem.
    pub fn validate<ItemKey>(&self, parent: &Storage<ChunkKey, ItemKey, Element>)
    where
        ItemKey: ValidKey,
        Element: Record<ChunkKey, ItemKey>,
    {
        self.0.write().unwrap().validate(parent);
    }
}

impl<ChunkKey, Element, IndexKeys, IndexKey>
    SecondaryIndexImpl<ChunkKey, Element, IndexKeys, IndexKey>
where
    ChunkKey: ValidKey,
    IndexKey: ValidKey,
    IndexKeys: Clone + Debug + Default + Eq,
    for<'x> &'x IndexKeys: IntoIterator<Item = &'x IndexKey>,
{
    fn indexing_rules<F>(f: F) -> SummaryRules<Element, IndexKeys, ChunkSecondaryIndex<IndexKey>>
    where
        F: Fn(&Element) -> Cow<IndexKeys> + 'static,
    {
        SummaryRules {
            map: Arc::new(move |element, old_index_keys, _internal_idx| {
                let new_index_keys = f(element);

                if old_index_keys != new_index_keys.borrow() {
                    Some(new_index_keys.into_owned())
                } else {
                    None
                }
            }),
            contribute: Arc::new(|new_index_keys, internal_idx, summary| {
                for new_index_key in new_index_keys {
                    let idx_set = summary
                        .reverse_index
                        .entry(new_index_key.clone())
                        .or_insert_with(Bitset::default);

                    idx_set.set(internal_idx);
                }
            }),
            uncontribute: Arc::new(|old_index_keys, internal_idx, summary| {
                for old_index_key in old_index_keys {
                    let mut remove = false;

                    if let Some(idx_set) = summary.reverse_index.get_mut(old_index_key) {
                        idx_set.unset(internal_idx);
                        if idx_set.is_empty() {
                            remove = true;
                        }
                    }

                    if remove {
                        summary.reverse_index.remove(old_index_key);
                    }
                }
            }),
        }
    }

    pub(crate) fn update_chunk<ItemKey>(
        &mut self,
        chunk_key: &ChunkKey,
        chunk_storage: &ChunkStorage<ChunkKey, ItemKey, Element>,
    ) where
        ItemKey: ValidKey,
        Element: Record<ChunkKey, ItemKey>,
    {
        let index = &mut self.index;
        let rules = &self.rules;
        let internal_storage = chunk_storage.internal_rvec();

        index
            .entry(chunk_key.clone())
            .or_insert_with(|| Summarize::new(&internal_storage, Arc::clone(rules)))
            .update(&internal_storage);
    }

    pub(crate) fn gc<ItemKey>(&mut self, parent: &Storage<ChunkKey, ItemKey, Element>)
    where
        ItemKey: ValidKey,
        Element: Record<ChunkKey, ItemKey>,
    {
        parent.gc(&mut self.gc_chunk_list, &mut self.index);
    }

    /// Panic if this storage is malformed or broken in any way.
    /// This is a slow operation and you shouldn't use it unless you suspect a problem.
    pub fn validate<ItemKey>(&mut self, parent: &Storage<ChunkKey, ItemKey, Element>)
    where
        ItemKey: ValidKey,
        Element: Record<ChunkKey, ItemKey>,
    {
        self.gc(parent);

        for chunk_key in self.index.keys() {
            assert!(parent.internal_idx_of(chunk_key).is_some());
        }
    }
}

impl<'a, Q, B, ChunkKey, Element, IndexKeys, IndexKey>
    MatchingSecondaryIndex<'a, Q, B, ChunkKey, Element, IndexKeys, IndexKey>
where
    ChunkKey: ValidKey,
    B: ToOwned + Hash + Eq + ?Sized + 'a,
    &'a B: ValidKey,
    IndexKeys: Clone + Debug + Default + Eq,
    IndexKey: ValidKey + Borrow<B>,
    for<'x> &'x IndexKeys: IntoIterator<Item = &'x IndexKey>,
{
    pub(crate) fn new(
        query: Q,
        secondary_index: &SecondaryIndex<ChunkKey, Element, IndexKeys, IndexKey>,
        index_key: Cow<'a, B>,
    ) -> Self {
        MatchingSecondaryIndex {
            query,
            secondary_index: secondary_index.clone(),
            index_key,
        }
    }
}

impl<'a, Q, B, ChunkKey, ItemKey, Element, IndexKeys, IndexKey> Query<ChunkKey, ItemKey, Element>
    for MatchingSecondaryIndex<'a, Q, B, ChunkKey, Element, IndexKeys, IndexKey>
where
    Q: Query<ChunkKey, ItemKey, Element>,
    B: ToOwned + Hash + Eq + ?Sized + 'a,
    &'a B: ValidKey,
    ChunkKey: ValidKey,
    ItemKey: ValidKey,
    IndexKeys: Clone + Debug + Default + Eq,
    IndexKey: ValidKey + Borrow<B>,
    Element: Record<ChunkKey, ItemKey>,
    for<'z> &'z IndexKeys: IntoIterator<Item = &'z IndexKey>,
{
    type ChunkIdxSet = Q::ChunkIdxSet;
    type ItemIdxSet = Intersection<Q::ItemIdxSet, Option<Bitset>>;

    fn chunk_idxs(&self, storage: &Storage<ChunkKey, ItemKey, Element>) -> Self::ChunkIdxSet {
        let mut secondary_index_impl = self.secondary_index.0.write().unwrap();
        assert_eq!(secondary_index_impl.parent_id, storage.id(), "Id mismatch: a secondary index may only be used with it's parent Storage, never any other Storage");
        let result = self.query.chunk_idxs(storage);

        secondary_index_impl.gc(storage);
        for idx in result.clone().into_idx_iter().flatten() {
            let chunk_idx = secondary_index_impl.gc_chunk_list[idx]
                .as_ref()
                .cloned()
                .expect("gc_chunk_list should not contain None immediately after gc");
            secondary_index_impl.update_chunk(&chunk_idx, &storage.internal_rvec()[idx]);
        }

        result
    }

    fn item_idxs(
        &self,
        chunk_key: &ChunkKey,
        chunk_storage: &ChunkStorage<ChunkKey, ItemKey, Element>,
    ) -> Self::ItemIdxSet {
        let secondary_index_impl = self.secondary_index.0.read().unwrap();
        let parent_idxs = self.query.item_idxs(chunk_key, chunk_storage);
        let ours_idxs: Option<Bitset> = secondary_index_impl
            .index
            .get(chunk_key)
            .and_then(|map_summarize| {
                map_summarize
                    .peek()
                    .reverse_index
                    .get(self.index_key.borrow())
            })
            .cloned();

        IdxSet::intersection(parent_idxs, ours_idxs)
    }

    fn test(&self, element: &Element) -> bool {
        self.query.test(element)
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_kitten_removal() {
        use crate::queries::everything::*;
        use crate::queries::secondary_index::*;
        use std::borrow::Cow;

        struct Kitten {
            name: String,
            colors: Vec<String>,
        }

        impl Record<(), String> for Kitten {
            fn chunk_key(self: &Kitten) -> Cow<()> {
                Cow::Owned(())
            }

            fn item_key(self: &Kitten) -> Cow<String> {
                Cow::Borrowed(&self.name)
            }
        }

        let mut storage: Storage<(), String, Kitten> = Storage::new();
        let by_color: SecondaryIndex<(), Kitten, Vec<String>, String> =
            SecondaryIndex::new(&storage, |kitten: &Kitten| Cow::Borrowed(&kitten.colors));

        storage.add(Kitten {
            name: String::from("mittens"),
            colors: vec![String::from("black"), String::from("white")],
        });

        storage.add(Kitten {
            name: String::from("furball"),
            colors: vec![String::from("orange")],
        });

        storage.add(Kitten {
            name: String::from("midnight"),
            colors: vec![String::from("black")],
        });

        storage.validate();
        by_color.validate(&storage);

        storage.remove(
            &Everything.matching(&by_color, Cow::Borrowed("orange")),
            std::mem::drop,
        );

        storage.validate();
        by_color.validate(&storage);

        storage.remove(
            &Everything.matching(&by_color, Cow::Borrowed("white")),
            std::mem::drop,
        );

        storage.validate();
        by_color.validate(&storage);

        storage.remove(
            &Everything.matching(&by_color, Cow::Borrowed("black")),
            std::mem::drop,
        );

        storage.validate();
        by_color.validate(&storage);
    }
}

impl<IndexKey> Default for ChunkSecondaryIndex<IndexKey>
where
    IndexKey: ValidKey,
{
    fn default() -> Self {
        ChunkSecondaryIndex {
            reverse_index: HashMap::default(),
        }
    }
}
