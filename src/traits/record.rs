use crate::traits::valid_key::ValidKey;
use std::borrow::Cow;

/// A trait for any retrievable `Record`. A `Record` must provide a chunk key and an item key.
/// The combination of chunk key and item key must be unique for each `Record`.
/// If you do not want to use chunking, you can use `()` as the chunk key.
pub trait Record<ChunkKey, ItemKey>
where
    ChunkKey: ToOwned + ?Sized,
    ChunkKey::Owned: Clone,
    ItemKey: ToOwned + ?Sized,
    ItemKey::Owned: Clone,
{
    /// Provide a chunk key for this `Record`. It's normal and expected for many related `Records` to
    /// share the same chunk key. `Records` with the same chunk key are stored physically together.
    /// It is easy to iterate over all `Records` in a single chunk, and it's possible to remove an entire
    /// chunk in constant time.
    fn chunk_key(&self) -> Cow<ChunkKey>;

    /// Provide a item key for this record. The item key must be unique within each chunk.
    fn item_key(&self) -> Cow<ItemKey>;
}

impl<ChunkKey, ItemKey, R> Record<ChunkKey, ItemKey> for &R
where
    ChunkKey: ValidKey,
    ItemKey: ValidKey,
    R: Record<ChunkKey, ItemKey>,
{
    fn chunk_key(&self) -> Cow<ChunkKey> {
        (*self).chunk_key()
    }

    fn item_key(&self) -> Cow<ItemKey> {
        (*self).item_key()
    }
}

impl<ItemKey, R> Record<(), ItemKey> for (ItemKey, R)
where
    ItemKey: ValidKey,
{
    fn chunk_key(&self) -> Cow<()> {
        Cow::Owned(())
    }

    fn item_key(&self) -> Cow<ItemKey> {
        Cow::Borrowed(&self.0)
    }
}

impl<ChunkKey, ItemKey, R> Record<ChunkKey, ItemKey> for (ChunkKey, ItemKey, R)
where
    ChunkKey: ValidKey,
    ItemKey: ValidKey,
{
    fn chunk_key(&self) -> Cow<ChunkKey> {
        Cow::Borrowed(&self.0)
    }

    fn item_key(&self) -> Cow<ItemKey> {
        Cow::Borrowed(&self.1)
    }
}
