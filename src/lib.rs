#![forbid(unsafe_code)]
#![forbid(missing_docs)]
#![warn(clippy::all)]

//! Retriever is an embedded in-memory data store for rust applications.
//!
//! ![](./Callie_the_golden_retriever_puppy.jpg)
//!
//! ## Features:
//!
//! * Document-oriented storage and retrieval.
//! * Index and query by unlimited secondary keys.
//! * Stored or computed (dynamic) keys (using Cow).
//! * Map-reduce-style queries, if you want them.
//! * Chunking: all records belonging to the same chunk are stored together in the same Vec.
//! * 100% safe Rust with no default dependencies.
//! * Lots of full-featured examples to get started!
//!
//! ## Retriever does not have:
//!
//! * Any built-in persistance. However, you can easily access the raw data for any or all chunks
//!   and pass it to serde for serialization.
//!
//! ## ToDo: (I want these features, but they aren't yet implemented)
//! * Parallelism (probably via rayon)
//! * Range queries
//! * Boolean queries (union, intersection, difference, etc -- note: you can perform intersection
//!   queries now just by chaining query operators)
//! * External mutable iterators (currently only internal iteration is supported for mutation)
//! * Retriever needs rigorous testing to ensure it does not have space leaks; currently it has
//!   at least one known space leak; probably my first priority going forward.
//! * It's likely that there's a vastly superior bitset implementation out there somewhere,
//!   and I'd like to incorporate it.
//! * Theoretically, I expect retriever's performance to break down beyond about
//!   16 million chunks of 16 million elements; I would eventually like retriever to
//!   scale up to "every electron in the universe."
//!
//! ## Comparison to other databases (SQL, MongoDB, etc)
//!
//! Unlike most databases, retriever stores your data as a plain old rust struct inside process memory.
//! It doesn't support access over a network from multiple clients.
//!
//! Like a traditional database, retriever has a flexible indexing and query system.
//!
//! ## Comparison to ECS (entity-component-system) frameworks
//!
//! Retriever can be used as a servicable component store, since records that share the same keys
//! are easy to cross-reference with each other. If chunks are small enough to fit in cache,
//! then this might even offer comparable performance, a hypothesis I float here without evidence.
//!
//! Retriever seeks to exploit performance opportunities from high-cardinality data
//! (i.e., every data or index element is unique).
//! My sense is that ECSs exist to exploit performance opportunities from low-cardinality data
//! (i.e. there are thousands of instances of 13 types of monster in a dungeon and even those
//! 13 types share many overlapping qualities).
//!
//! ## Getting started:
//!
//! 1. Create a rust struct or enum that will represents a data item that you want to store.
//! 2. Choose a *chunk key* and *item key* for each instance of your record.
//!   * Many records can share the same chunk key.
//!   * No two records in the same chunk may have the same item key.
//!   * A record is therefore uniquely identified by it's (ChunkKey,ItemKey) pair.
//!   * Retriever uses Cow to get the keys for each record, so a key can be
//!     borrowed from the record *or* a key can be dynamically computed.
//!   * All keys must be `Clone + Debug + Eq + Hash + Ord`.
//!   * If you don't want to use chunking or aren't sure what to types of chunk key to choose,
//!     use () as the chunk key.
//! 3. Implement the Record<ChunkKey,ItemKey> trait for your choice of record, chunk key, and item
//!    key types.
//! 4. Create a new empty Storage object using `Storage::new()`.
//! 5. If you want, create some secondary indexes using `SecondaryIndex::new_expensive()`. Define
//!    secondary indexes by writing a single closure that maps records into zero or more secondary
//!    keys.
//! 6. Create some reductions using `Reduction::new_expensive()`. Define reductions by writing two
//!    closures: A map from the record type to a summary types, and a reduction (or fold) of
//!    several summaries into a single summary.
//! 7. Keep the Storage, SecondaryIndexes, and Reductions together for later use.
//!    Avoid dropping SecondaryIndexes or Reductions, because they are expensive to re-compute.
//! 8. Use `Storage::add()`, `Storage::iter()`, `Storage::query()`, `Storage::modify()`, and
//!    `Storage::remove()` to implement CRUD operations on your storage.
//! 9. Use `Reduction::summarize()` to reduce your entire storage down to a single summary object.
//!
//! ### More about how to choose a good chunk key
//!
//!  * A good chunk key will keep related records together; even a complex series of queries
//!    should hopefully access only a few chunks at a time.
//!  * If the total size of a chunk is small enough, then the entire chunk and its indices
//!    might fit in cache, improving performance.
//!  * A good chunk key is predictable; you should hopefully know which chunk
//!    a record will be in before you look for it, or at least be able to narrow it down to a
//!    resonably small range of chunks, so that you don't have to search a lot of chunks to find it.
//!  * A good chunk key might correspond to persistant storage, such as a single file, or a
//!    corresponding grouping in an upstream database. It's easy to load and unload chunks as a
//!    block.
//!  * For stores that represent geographical information, a good chunk key might represent
//!    a map grid or other kinds of fenced mutually-exclusive areas.
//!  * For a time-series database, a good chunk key might represent a time interval.
//!

mod internal;
/// Various ways to query storage.
pub mod queries;
/// Various ways to summarize storage.
pub mod summaries;
mod traits;
mod types;

pub use crate::traits::*;
pub use crate::types::*;

//
// Puppy is from: https://commons.wikimedia.org/wiki/File:Callie_the_golden_retriever_puppy.jpg
//

// Remainder of this file is unit tests.

#[cfg(test)]
mod test {
    use crate::queries::chunks::Chunks;
    use crate::queries::everything::Everything;
    use crate::queries::secondary_index::SecondaryIndex;
    use crate::summaries::reduction::Reduction;
    use crate::*;
    use std::borrow::Cow;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct X(u64, u64);

    impl Record<u64, u64> for X {
        fn chunk_key(&self) -> Cow<u64> {
            Cow::Owned((self.0 & 0x00F0) >> 4)
        }

        fn item_key(&self) -> Cow<u64> {
            Cow::Borrowed(&self.0)
        }
    }

    #[test]
    fn test_remove_and_replace_chunk_with_secondary_index() {
        let mut storage: Storage<u64, u64, X> = Storage::new();
        let mut index: SecondaryIndex<u64, X, Option<u64>, u64> =
            SecondaryIndex::new_expensive(&storage, |x: &X| Some(x.1 & 0x1));

        storage.add(X(0x101, 0x101));
        storage.add(X(0x102, 0x102));
        storage.add(X(0x103, 0x103));
        storage.add(X(0x104, 0x104));
        storage.add(X(0x105, 0x105));
        storage.add(X(0x106, 0x106));
        storage.add(X(0x107, 0x107));
        storage.add(X(0x108, 0x108));

        assert_eq!(
            4,
            storage.query(&Everything.matching(&mut index, &0)).count()
        );

        storage.remove_chunk(&0);

        storage.add(X(0x101, 0x101));
        storage.add(X(0x102, 0x102));
        storage.add(X(0x103, 0x103));
        storage.add(X(0x104, 0x104));
        storage.add(X(0x105, 0x105));
        storage.add(X(0x106, 0x106));
        storage.add(X(0x107, 0x107));
        storage.add(X(0x108, 0x108));

        assert_eq!(
            4,
            storage.query(&Everything.matching(&mut index, &0)).count()
        );
    }

    #[test]
    fn test_editor() {
        let mut storage: Storage<u64, u64, X> = Storage::new();
        storage.add(X(0x101, 0x101));
        storage.add(X(0x202, 0x101));
        storage.add(X(0x111, 0x101));

        storage.modify(Id(0x0, 0x202), |mut editor| {
            assert_eq!(&Id(&0x0, &0x202), editor.id());
            assert_eq!(&X(0x202, 0x101), editor.get());
            editor.get_mut().1 = 0x102;
            assert_eq!(&X(0x202, 0x102), editor.get());
        });
    }

    #[test]
    fn test_filter() {
        let mut storage: Storage<u64, u64, X> = Storage::new();
        storage.add(X(0x101, 0x101));
        storage.add(X(0x202, 0x999));
        storage.add(X(0x111, 0x111));

        storage.remove(Chunks(&[0x0]).filter(|x: &X| x.1 == 0x999), std::mem::drop);
        assert_eq!(2, storage.iter().count());
        assert!(storage.get(&Id(0x0, 0x101)).is_some());
        assert!(storage.get(&Id(0x1, 0x111)).is_some());
        assert!(storage.get(&Id(0x0, 0x202)).is_none());
    }

    #[test]
    fn test_query_by_id() {
        let mut storage: Storage<u64, u64, X> = Storage::new();

        let mut even_odd: SecondaryIndex<u64, X, Option<bool>, bool> =
            SecondaryIndex::new_expensive(&storage, |x: &X| Some(x.1 % 2 == 1));

        storage.add(X(0x000, 0x000));
        storage.add(X(0x101, 0x111));
        storage.add(X(0x202, 0x222));

        assert_eq!(
            Some(&X(0x101, 0x111)),
            storage.query(&Id(0x0, 0x101)).next()
        );
        assert_eq!(
            Some(&X(0x101, 0x111)),
            storage
                .query(&Id(0x0, 0x101).matching(&mut even_odd, &true))
                .next()
        );
        assert_eq!(
            None,
            storage
                .query(&Id(0x0, 0x101).matching(&mut even_odd, &false))
                .next()
        );
    }

    #[test]
    fn test_query_by_chunks() {
        let mut storage: Storage<u64, u64, X> = Storage::new();

        let mut even_odd: SecondaryIndex<u64, X, Option<bool>, bool> =
            SecondaryIndex::new_expensive(&storage, |x: &X| Some(x.1 % 2 == 1));

        storage.add(X(0x000, 0x000));
        storage.add(X(0x101, 0x111));
        storage.add(X(0x202, 0x222));
        storage.add(X(0x010, 0x000));
        storage.add(X(0x111, 0x111));
        storage.add(X(0x212, 0x222));
        storage.add(X(0x020, 0x000));
        storage.add(X(0x121, 0x111));
        storage.add(X(0x222, 0x222));

        let odd_items_even_chunks: Vec<X> = storage
            .query(&Chunks(&[0x0, 0x2]).matching(&mut even_odd, &true))
            .cloned()
            .collect();
        assert_eq!(
            &[X(0x101, 0x111), X(0x121, 0x111)],
            odd_items_even_chunks.as_slice()
        );
    }

    #[test]
    fn test_index_intersections() {
        let mut storage: Storage<u64, u64, X> = Storage::new();

        let mut even_odd: SecondaryIndex<u64, X, Option<bool>, bool> =
            SecondaryIndex::new_expensive(&storage, |x: &X| Some(x.1 % 2 == 1));

        let mut small: SecondaryIndex<u64, X, Option<bool>, bool> =
            SecondaryIndex::new_expensive(&storage, |x: &X| Some(x.1 < 0x600));

        storage.add(X(0x000, 0x000));
        storage.add(X(0x101, 0x111));
        storage.add(X(0x202, 0x222));
        storage.add(X(0x303, 0x333));
        storage.add(X(0x404, 0x444));
        storage.add(X(0x505, 0x555));
        storage.add(X(0x606, 0x666));
        storage.add(X(0x707, 0x777));

        let small_odds: Vec<X> = storage
            .query(
                &Everything
                    .matching(&mut even_odd, &true)
                    .matching(&mut small, &true),
            )
            .cloned()
            .collect();

        assert_eq!(3, small_odds.len());
        assert!(small_odds.contains(&X(0x101, 0x111)));
        assert!(small_odds.contains(&X(0x303, 0x333)));
        assert!(small_odds.contains(&X(0x505, 0x555)));
        assert!(!small_odds.contains(&X(0x202, 0x222)));
        assert!(!small_odds.contains(&X(0x707, 0x777)));
    }

    #[test]
    fn test_random_edits() {
        use rand::Rng;

        let mut storage: Storage<u64, u64, X> = Storage::new();
        let mut reduction: Reduction<u64, X, u64> = Reduction::new_expensive(
            &storage,
            16,
            |x: &X, was| {
                if x.1 != *was {
                    Some(x.1)
                } else {
                    None
                }
            },
            |xs: &[u64], was| {
                let total = xs.iter().cloned().sum::<u64>();

                if total != *was {
                    Some(total)
                } else {
                    None
                }
            },
        );
        let mut index: SecondaryIndex<u64, X, Option<u64>, u64> =
            SecondaryIndex::new_expensive(&storage, |x: &X| Some(x.1));

        let k = 100_000;

        for i in 0..k {
            storage.add(X(i, rand::thread_rng().gen_range(0, k / 10)));
        }

        for _ in 0..k {
            let id = rand::thread_rng().gen_range(0, k);
            storage
                .entry(&X(id, 0))
                .and_modify(|x| {
                    x.1 = rand::thread_rng().gen_range(0, k / 10);
                })
                .or_panic();

            storage
                .query(&Everything.matching(&mut index, &rand::thread_rng().gen_range(0, 10)))
                .count();
            reduction.summarize(&storage);
        }
    }
}
