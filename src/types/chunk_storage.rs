use super::entry::Entry;
use super::id::Id;
use crate::internal::hasher::HasherImpl;
use crate::internal::mr::rvec::RVec;
use crate::traits::query::Query;
use crate::traits::record::Record;
use crate::traits::valid_key::ValidKey;
use crate::types::editor::Editor;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;

/// A chunk of storage containing all elements with a common primary key
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

    pub fn len(&self) -> usize {
        self.data.len()
    }

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
        unique_id: &'a R,
    ) -> Entry<'a, ChunkKey, ItemKey, Element>
    where
        R: Record<ChunkKey, ItemKey> + 'a,
    {
        assert_eq!(&self.chunk_key, unique_id.chunk_key().as_ref());
        Entry::new(
            Id::new(unique_id.chunk_key(), unique_id.item_key()),
            self.index.get(unique_id.item_key().borrow()).cloned(),
            self,
        )
    }

    pub(crate) fn get_idx_mut(&mut self, idx: usize) -> &mut Element {
        &mut self.data[idx]
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &Element> {
        self.data.iter()
    }

    pub(crate) fn query<'a, Q>(&'a self, query: &'a Q) -> impl Iterator<Item = &'a Element>
    where
        Q: Query<ChunkKey, ItemKey, Element>,
    {
        query
            .item_idxs(&self.chunk_key, &self)
            .iter()
            .map(move |idx| self.get_idx(idx))
            .filter(move |element| query.test(element))
    }

    pub(crate) fn modify<'a, Q, F>(&'a mut self, query: &'a mut Q, f: F)
    where
        Q: Query<ChunkKey, ItemKey, Element>,
        F: Fn(Editor<ChunkKey, ItemKey, Element>),
    {
        let chunk_key: ChunkKey = self.chunk_key.clone();

        for idx in query.item_idxs(&self.chunk_key, &self).iter() {
            let item_key = self.data[idx].item_key().into_owned();

            if !query.test(&self.data[idx]) {
                continue;
            }

            f(Editor::new(Id::new(&chunk_key, &item_key), idx, self));

            assert_eq!(&chunk_key, self.data[idx].chunk_key().borrow());
            assert_eq!(&item_key, self.data[idx].item_key().borrow());
        }
    }

    pub(crate) fn remove<'a, Q, F>(&'a mut self, query: &'a mut Q, f: &F)
    where
        F: Fn(Element),
        Q: Query<ChunkKey, ItemKey, Element>,
    {
        let mut last_removed_idx = self.data.len();
        let idxs = query.item_idxs(&self.chunk_key, &self);

        if idxs.is_sorted() {
            for idx in idxs.iter().rev() {
                if query.test(&self.data[idx]) {
                    assert!(idx < last_removed_idx);
                    last_removed_idx = idx;
                    f(self.remove_idx(idx));
                }
            }
        } else {
            let mut idxs: Vec<usize> = idxs.iter().collect();
            idxs.sort_unstable();
            for idx in idxs.iter().rev() {
                f(self.remove_idx(*idx));
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
