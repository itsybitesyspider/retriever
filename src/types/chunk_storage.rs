use super::entry::Entry;
use super::id::Id;
use crate::internal::hasher::HasherImpl;
use crate::internal::mr::rvec::RVec;
use crate::traits::idxset::IdxSet;
use crate::traits::query::Query;
use crate::traits::record::Record;
use crate::traits::valid_key::ValidKey;
use crate::types::editor::Editor;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;

/// A chunk of storage containing all elements with a common primary key.
/// Users should rarely if ever interact with this type.
#[derive(Clone)]
pub struct ChunkStorage<ChunkKey, ItemKey, Element> {
    chunk_key: ChunkKey,
    data: RVec<Element>,
    index: HashMap<ItemKey, usize, HasherImpl>,
}

impl<ChunkKey, ItemKey, Element> ChunkStorage<ChunkKey, ItemKey, Element>
where
    ChunkKey: ValidKey,
    ItemKey: ValidKey,
    Element: Record<ChunkKey, ItemKey>,
{
    pub(crate) fn new(chunk_key: ChunkKey) -> Self {
        ChunkStorage {
            chunk_key,
            data: RVec::default(),
            index: HashMap::with_hasher(crate::internal::hasher::HasherImpl::default()),
        }
    }

    /// True IFF this `ChunkStorage` is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns the number of elements in this `ChunkStorage`.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns the chunk key used by all elements in this `ChunkStorage`.
    pub fn chunk_key(&self) -> &ChunkKey {
        &self.chunk_key
    }

    pub(crate) fn raw(&self) -> &[Element] {
        &self.data
    }

    pub(crate) fn add(&mut self, element: Element) -> usize {
        let chunk_key = element.chunk_key();
        let item_key = element.item_key();
        assert_eq!(&self.chunk_key, chunk_key.borrow());
        let old_key = self.index.insert(item_key.into_owned(), self.data.len());
        assert!(old_key.is_none(), "duplicate item key within chunk");
        let idx = self.data.len();
        self.data.push(element);
        idx
    }

    pub(crate) fn extend<I, K>(&mut self, i: I)
    where
        I: Iterator<Item = K>,
        Element: Borrow<K>,
        K: ToOwned<Owned = Element> + Record<ChunkKey, ItemKey>,
    {
        // TODO: write an efficient version of this
        for e in i {
            self.add(e.to_owned());
        }
    }

    pub(crate) fn get_idx(&self, idx: usize) -> &Element {
        &self.data[idx]
    }

    pub(crate) fn get<R>(&self, unique_id: &R) -> Option<&Element>
    where
        R: Record<ChunkKey, ItemKey>,
    {
        assert_eq!(&self.chunk_key, unique_id.chunk_key().borrow());
        Some(&self.get_idx(*self.index.get(unique_id.item_key().borrow())?))
    }

    pub(crate) fn entry<'a, R>(
        &'a mut self,
        unique_id: R,
    ) -> Entry<'a, R, ChunkKey, ItemKey, Element>
    where
        R: Record<ChunkKey, ItemKey> + 'a,
    {
        let idx = self.index.get(unique_id.item_key().borrow()).cloned();
        assert_eq!(&self.chunk_key, unique_id.chunk_key().as_ref());
        Entry::new(unique_id, idx, self)
    }

    pub(crate) fn get_idx_mut(&mut self, idx: usize) -> &mut Element {
        &mut self.data[idx]
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &Element> {
        self.data.iter()
    }

    pub(crate) fn query<'a, Q>(&'a self, query: Q) -> impl Iterator<Item = &'a Element>
    where
        Q: Query<ChunkKey, ItemKey, Element> + Clone + 'a,
    {
        query
            .item_idxs(&self.chunk_key, &self)
            .into_idx_iter()
            .flatten()
            .map(move |idx| self.get_idx(idx))
            .filter(move |element| query.test(element))
    }

    pub(crate) fn modify<'a, Q, F>(&'a mut self, query: &Q, f: F)
    where
        Q: Query<ChunkKey, ItemKey, Element>,
        F: Fn(Editor<ChunkKey, ItemKey, Element>),
    {
        let chunk_key: ChunkKey = self.chunk_key.clone();

        for idx in query
            .item_idxs(&self.chunk_key, &self)
            .into_idx_iter()
            .flatten()
        {
            let item_key = self.data[idx].item_key().into_owned();

            if !query.test(&self.data[idx]) {
                continue;
            }

            f(Editor::new(Id::new(&chunk_key, &item_key), idx, self));

            assert_eq!(&chunk_key, self.data[idx].chunk_key().borrow());
            assert_eq!(&item_key, self.data[idx].item_key().borrow());
        }
    }

    pub(crate) fn remove<'a, Q, F>(&'a mut self, query: &Q, f: &F)
    where
        F: Fn(Element),
        Q: Query<ChunkKey, ItemKey, Element>,
    {
        let mut last_removed_idx = self.data.len();
        let idxs = query.item_idxs(&self.chunk_key, &self);

        for idx in idxs.into_idx_iter().flatten().rev() {
            if query.test(&self.data[idx]) {
                assert!(idx < last_removed_idx);
                last_removed_idx = idx;
                f(self.remove_idx(idx));
            }
        }
    }

    /// Remove the specified element and return it
    pub(crate) fn remove_idx(&mut self, idx: usize) -> Element {
        let result = self.data.swap_remove(idx);
        self.index.remove(result.item_key().borrow());

        if idx < self.data.len() {
            self.index
                .insert(self.data[idx].item_key().into_owned(), idx);
        }

        result
    }

    pub(crate) fn internal_idx_of<Q>(&self, item_key: &Q) -> Option<usize>
    where
        Q: Hash + Eq,
        ItemKey: Borrow<Q>,
    {
        self.index.get(item_key).cloned()
    }

    pub(crate) fn internal_rvec(&self) -> &RVec<Element> {
        &self.data
    }

    pub(crate) fn validate(&self) {
        for (idx, element) in self.data.iter().enumerate() {
            assert_eq!(
                &self.chunk_key,
                element.chunk_key().borrow(),
                "element chunk_key() does match chunk chunk_key()"
            );
            assert_eq!(
                Some(&idx),
                self.index.get(element.item_key().borrow()),
                "element not indexed"
            );
        }

        for (item_key, idx) in self.index.iter() {
            assert_eq!(
                item_key,
                self.data[*idx].item_key().borrow(),
                "element item_key() does not match index"
            );
        }
    }
}

impl<ChunkKey, ItemKey, Element> Into<Vec<Element>> for ChunkStorage<ChunkKey, ItemKey, Element> {
    fn into(self) -> Vec<Element> {
        self.data.into()
    }
}
