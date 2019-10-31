use crate::internal::bits::Bitset;
use crate::internal::idxset::IdxSet;
use crate::internal::mr::rvec::RVec;
use crate::internal::mr::summarize::{Summarize, SummaryRules};
use crate::traits::query::Query;
use crate::traits::record::Record;
use crate::traits::valid_key::ValidKey;
use crate::types::chunk_storage::ChunkStorage;
use crate::types::storage::Storage;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::RwLock;

/// A Query matching against a SecondaryIndex. Construct using Query::matching(&SecondaryIndex, &IndexKey).
pub struct MatchingSecondaryIndex<'a, Q, B, ChunkKey, Element, IndexKeys, IndexKey>
where
    IndexKey: ValidKey + Borrow<B>,
    IndexKeys: ValidKey + Default,
    for<'y> &'y IndexKeys: IntoIterator<Item = &'y IndexKey>,
    B: Hash + Eq + ?Sized,
{
    query: Q,
    secondary_index: RwLock<&'a mut SecondaryIndex<ChunkKey, Element, IndexKeys, IndexKey>>,
    index_key: &'a B,
}

struct ChunkSecondaryIndex<IndexKey>
where
    IndexKey: ValidKey,
{
    reverse_index: HashMap<IndexKey, Bitset>,
}

/// An index of the records in a storage.
pub struct SecondaryIndex<ChunkKey, Element, IndexKeys, IndexKey>
where
    IndexKey: ValidKey,
    IndexKeys: ValidKey,
    for<'x> &'x IndexKeys: IntoIterator<Item = &'x IndexKey>,
{
    parent_id: u64,
    gc_chunk_list: RVec<Option<ChunkKey>>,
    rules: Arc<SummaryRules<Element, IndexKeys, ChunkSecondaryIndex<IndexKey>>>,
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
    IndexKeys: ValidKey + Default,
    for<'x> &'x IndexKeys: IntoIterator<Item = &'x IndexKey>,
{
    /// Create a new SecondaryIndex of a storage.
    ///
    /// SecondaryIndex::expensive_new(..) returns immediately. The first time it is used, however,
    /// each new SecondaryIndex will need to fully index every chunk that it queries.
    /// Creating and dropping a lot of SecondaryIndexes is therefore wasteful and pointless.
    ///
    /// Avoid calling SecondaryIndex::expensive_new by caching secondary indices as much as possible.
    pub fn new_expensive<C, I, E, F>(storage: &Storage<C, I, E>, f: F) -> Self
    where
        C: ValidKey,
        I: ValidKey,
        E: Record<C, I>,
        F: Fn(&Element) -> IndexKeys + 'static,
    {
        SecondaryIndex {
            parent_id: storage.id(),
            gc_chunk_list: RVec::default(),
            index: HashMap::with_hasher(crate::internal::hasher::HasherImpl::default()),
            rules: Arc::new(Self::indexing_rules(f)),
        }
    }

    fn indexing_rules<F>(f: F) -> SummaryRules<Element, IndexKeys, ChunkSecondaryIndex<IndexKey>>
    where
        F: Fn(&Element) -> IndexKeys + 'static,
    {
        SummaryRules {
            map: Arc::new(move |element, old_index_keys, _internal_idx| {
                let new_index_keys = f(element);

                if old_index_keys != &new_index_keys {
                    Some(new_index_keys)
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
    B: Hash + Eq + ?Sized + 'a,
    IndexKeys: ValidKey + Default,
    IndexKey: ValidKey + Borrow<B>,
    for<'x> &'x IndexKeys: IntoIterator<Item = &'x IndexKey>,
{
    pub(crate) fn new(
        query: Q,
        secondary_index: &'a mut SecondaryIndex<ChunkKey, Element, IndexKeys, IndexKey>,
        index_key: &'a B,
    ) -> Self {
        MatchingSecondaryIndex {
            query,
            secondary_index: RwLock::new(secondary_index),
            index_key,
        }
    }
}

impl<'a, Q, B, ChunkKey, ItemKey, Element, IndexKeys, IndexKey> Query<ChunkKey, ItemKey, Element>
    for MatchingSecondaryIndex<'a, Q, B, ChunkKey, Element, IndexKeys, IndexKey>
where
    Q: Query<ChunkKey, ItemKey, Element> + 'a,
    ChunkKey: ValidKey,
    ItemKey: ValidKey + 'a,
    B: Hash + Eq + 'a + ?Sized,
    IndexKeys: ValidKey + Default,
    IndexKey: ValidKey + Borrow<B>,
    Element: Record<ChunkKey, ItemKey>,
    for<'z> &'z IndexKeys: IntoIterator<Item = &'z IndexKey>,
{
    fn chunk_idxs(&self, storage: &Storage<ChunkKey, ItemKey, Element>) -> IdxSet {
        let mut secondary_index = self.secondary_index.write().unwrap();
        assert_eq!(secondary_index.parent_id, storage.id(), "Id mismatch: a secondary index may only be used with it's parent Storage, never any other Storage");
        let result = self.query.chunk_idxs(storage);

        secondary_index.gc(storage);
        for idx in result.iter() {
            let chunk_idx = secondary_index.gc_chunk_list[idx]
                .as_ref()
                .cloned()
                .expect("gc_chunk_list should not contain None immediately after gc");
            secondary_index.update_chunk(&chunk_idx, &storage.internal_rvec()[idx]);
        }

        result
    }

    fn item_idxs(
        &self,
        chunk_key: &ChunkKey,
        chunk_storage: &ChunkStorage<ChunkKey, ItemKey, Element>,
    ) -> IdxSet {
        let parent_idxs = self.query.item_idxs(chunk_key, chunk_storage);
        let ours_idxs: Option<Bitset> = self
            .secondary_index
            .read()
            .unwrap()
            .index
            .get(chunk_key)
            .and_then(|map_summarize| map_summarize.peek().reverse_index.get(self.index_key))
            .cloned();

        IdxSet::intersection(parent_idxs, IdxSet::from(ours_idxs))
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
        let mut by_color: SecondaryIndex<(), Kitten, Vec<String>, String> =
            SecondaryIndex::new_expensive(&storage, |kitten: &Kitten| kitten.colors.clone());

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

        storage.remove(Everything.matching(&mut by_color, "orange"), std::mem::drop);

        storage.validate();
        by_color.validate(&storage);

        storage.remove(Everything.matching(&mut by_color, "white"), std::mem::drop);

        storage.validate();
        by_color.validate(&storage);

        storage.remove(Everything.matching(&mut by_color, "black"), std::mem::drop);

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
