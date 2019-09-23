use crate::traits::record::Record;
use crate::traits::valid_key::ValidKey;
use crate::types::storage::Storage;
use std::sync::Arc;

/// An Iterator over an Arc containing a Storage.
pub struct ArcIter<ChunkKey, ItemKey, Element> {
    arc: Arc<Storage<ChunkKey, ItemKey, Element>>,
    forward: StorageIdx,
    backward: StorageIdx,
}

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
struct StorageIdx {
    chunk_idx: usize,
    element_idx: usize,
}

impl<ChunkKey, ItemKey, Element> ArcIter<ChunkKey, ItemKey, Element>
where
    ChunkKey: ValidKey,
    ItemKey: ValidKey,
    Element: Record<ChunkKey, ItemKey> + Clone,
{
    pub(crate) fn new(arc: Arc<Storage<ChunkKey, ItemKey, Element>>) -> Self {
        let forward = StorageIdx {
            chunk_idx: 0,
            element_idx: 0,
        };

        let backward = StorageIdx {
            chunk_idx: arc.internal_mrvec().len(),
            element_idx: 0,
        };

        ArcIter {
            arc,
            forward,
            backward,
        }
    }
}

impl<ChunkKey, ItemKey, Element> Iterator for ArcIter<ChunkKey, ItemKey, Element>
where
    ChunkKey: ValidKey,
    ItemKey: ValidKey,
    Element: Record<ChunkKey, ItemKey> + Clone,
{
    type Item = Element;

    #[inline]
    fn next(&mut self) -> Option<Element> {
        if self.forward >= self.backward {
            return None;
        }

        let result = self.forward.take_clone(self.arc.as_ref());

        if result.is_some() {
            self.forward.next(self.arc.as_ref());
        }

        result
    }
}

impl<ChunkKey, ItemKey, Element> DoubleEndedIterator for ArcIter<ChunkKey, ItemKey, Element>
where
    ChunkKey: ValidKey,
    ItemKey: ValidKey,
    Element: Record<ChunkKey, ItemKey> + Clone,
{
    #[inline]
    fn next_back(&mut self) -> Option<Element> {
        self.backward.prev(self.arc.as_ref());

        if self.forward >= self.backward {
            return None;
        }

        self.backward.take_clone(self.arc.as_ref())
    }
}

impl StorageIdx {
    fn next<C, I, E>(&mut self, storage: &Storage<C, I, E>)
    where
        C: ValidKey,
        I: ValidKey,
        E: Record<C, I>,
    {
        self.element_idx += 1;

        if self.element_idx >= storage.internal_mrvec()[self.chunk_idx].len() {
            self.element_idx = 0;
            self.chunk_idx += 1;
        }
    }

    fn prev<C, I, E>(&mut self, storage: &Storage<C, I, E>)
    where
        C: ValidKey,
        I: ValidKey,
        E: Record<C, I>,
    {
        if self.element_idx == 0 {
            if self.chunk_idx == 0 {
                self.chunk_idx = 0;
                self.element_idx = 0;
            } else {
                self.chunk_idx -= 1;
                self.element_idx = storage.internal_mrvec()[self.chunk_idx].len() - 1;
            }
        } else {
            self.element_idx -= 1;
        }
    }

    fn take_clone<C, I, E>(&mut self, storage: &Storage<C, I, E>) -> Option<E>
    where
        C: ValidKey,
        I: ValidKey,
        E: Record<C, I> + Clone,
    {
        if self.chunk_idx >= storage.internal_mrvec().len()
            || self.element_idx
                >= storage.internal_mrvec()[self.chunk_idx]
                    .internal_mrvec()
                    .len()
        {
            None
        } else {
            Some(
                storage.internal_mrvec()[self.chunk_idx].internal_mrvec()[self.element_idx].clone(),
            )
        }
    }
}
