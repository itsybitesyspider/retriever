use crate::traits::record::Record;
use crate::traits::valid_key::ValidKey;
use crate::types::storage::Storage;
use std::sync::Arc;

/// An Iterator over an Arc containing a Storage.
pub struct ArcIter<ChunkKey, ItemKey, Element> {
    arc: Arc<Storage<ChunkKey, ItemKey, Element>>,
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
        ArcIter {
            arc,
            chunk_idx: 0,
            element_idx: 0,
        }
    }

    fn take_clone(&mut self) -> Option<Element>
    {
        if self.chunk_idx >= self.arc.internal_mrvec().len()
            || self.element_idx
                >= self.arc.internal_mrvec()[self.chunk_idx]
                    .internal_mrvec()
                    .len()
        {
            None
        } else {
            Some(
                self.arc.internal_mrvec()[self.chunk_idx].internal_mrvec()[self.element_idx].clone(),
            )
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
        if self.chunk_idx > self.arc.internal_mrvec().len() {
            return None;
        }

        let result = self.take_clone();

        if result.is_some() {
            self.element_idx += 1;

            while self.chunk_idx < self.arc.internal_mrvec().len() &&
                  self.element_idx >= self.arc.internal_mrvec()[self.chunk_idx].internal_mrvec().len() {
                self.chunk_idx += 1;
                self.element_idx = 0;
            }
        }

        result
    }
}
