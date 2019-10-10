#![forbid(unsafe_code)]
#![forbid(missing_docs)]
#![warn(clippy::all)]

//! Retriever is an embedded in-memory data store for rust applications.
//!
//! ![Image of Callie, a golden retriever puppy, by Wikimedia Commons user MichaelMcPhee. Creative Commons Attribution 3.0 Unported.](./Callie_the_golden_retriever_puppy.jpg)
//!
//! ## Retriever supports:
//!
//! * Key-value style storage and retrieval.
//! * Index and query by unlimited secondary keys.
//! * Stored or computed (dynamic) keys (using Cow).
//! * Cached map-reduce style queries (if you want them).
//! * Chunking: records belonging to the same chunk are stored together physically
//!   and can be removed in O(1) time.
//! * Puppies!
//! * Lots and lots of full-featured examples to get started!
//!
//! ## Retriever does not have:
//!
//! * Any dependencies, except std (by default -- non-default feature flags may pull dependencies)
//! * Novelty (there's nothing in retriever that I wouldn't like explaining to a CS undergrad)
//! * Persistance (although retriever includes "escape hatches" to access raw data and implement your own)
//!
//! ## ToDo: (I want these features, but they aren't yet implemented)
//! * Parallelism (probably via rayon)
//! * Range queries
//! * Boolean queries (union, intersection, difference, etc -- note: you can perform intersection
//!   queries now just by chaining query operators)
//!
//! ## Comparison to other databases (SQL, MongoDB, etc)
//!
//! Unlike a database, retriever stores your data as rust structs inside process memory.
//! It doesn't support access over a network from multiple users. Retriever does have a flexible
//! indexing and query system that approaches the functionality of a traditional database.
//!
//! ## Comparison to ECS (entity-component-system) frameworks
//!
//! Unlike an ECS, retriever prioritizes general-purpose flexibility and scalability over the
//! kind of raw performance expected by video games. That said, retriever is servicable as a
//! component store. Cross-referencing different records of different types from different
//! storages, but with the same primary key, is easy and reasonably fast.
//!
//! ## Getting started:
//!
//! 1. Create a rust struct or enum that will represents a data item that you want to store.
//! 2. Choose a *chunk key* and *item key* for each instance of your record.
//!   * Many records can share the same chunk key.
//!   * No two records in the same chunk may have the same item key.
//!   * A record is therefore uniquely identified by it's (ChunkKey,ItemKey) pair.
//!   * Retriever uses Cow to get the keys for each record, so a key can be a value that is part
//!     borrowed from the record *or* a key can be dynamically computed.
//!   * All keys must be `Clone + Debug + Eq + Hash + Ord`.
//!   * If you don't want to use chunking or aren't sure what to choose, use () as the chunk key.
//!     * A good chunk key will keep related records together, so that most queries will access
//!       only a few chunks.
//!     * A good chunk key is predictable; you should always know which chunk (or a range of chunks)
//!       a record will be in before you look for it.
//!     * A good chunk key might correspond to persistant storage, such as a single file, or a
//!       corresponding chunk in an upstream database. It's easy to load and unload chunks as a
//!       block.
//!     * A good chunk key might correspond a physical region (i.e., a map grid) or time interval
//!       if you know that all searches will be bounded by these dimensions.
//!     * You can use the Everything query as the starting point for querying all chunks in storage,
//!       but this always O(n) in the number of chunks even if you discover 0 results.
//! 3. Implement the Record<ChunkKey,ItemKey> trait for your choice of record, chunk key, and item
//!    key types.
//! 4. Create a new empty Storage object using `Storage::new()`.
//! 5. Create some secondary indexes using `SecondaryIndex::new_expensive()`. Define secondary
//!    indexes by writing a single closure that maps records into zero or more secondary keys.
//! 6. Create some reductions using `Reduction::new_expensive()`. Define reductions by writing two
//!    closures: A map from the record type to a summary types, and a reduction (or fold) of
//!    several summaries into a single summary.
//! 7. Keep the Storage, SecondaryIndexes, and Reductions together in a struct for later use.
//!    Don't drop SecondaryIndexes or Reductions, because they are expensive to re-compute.
//! 8. Use `Storage::add()`, `Storage::iter()`, `Storage::query()`, `Storage::modify()`, and
//!    `Storage::remove()` to implement CRUD operations on your storage.
//! 9. Use `Reduction::summarize()` to reduce your entire storage down to a single summary object.
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

// Remainder of this file is unit tests written to hit code coverage goals.

#[cfg(test)]
mod test {
    use crate::summaries::reduction::Reduction;
    use crate::queries::chunks::Chunks;
    use crate::queries::everything::Everything;
    use crate::queries::secondary_index::SecondaryIndex;
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
        let mut reduction: Reduction<u64, X, u64> = Reduction::new_expensive(&storage,
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
          });
        let mut index: SecondaryIndex<u64, X, Option<u64>, u64> = SecondaryIndex::new_expensive(
          &storage,
          |x: &X| {
            Some(x.1)
          });

        let k = 100_000;

        for i in 0..k {
           storage.add(X(i, rand::thread_rng().gen_range(0,k/10)));
        }

        for _ in 0..k {
          let id = rand::thread_rng().gen_range(0,k);
          storage.entry(&X(id,0))
            .and_modify(|x| {
              x.1 = rand::thread_rng().gen_range(0,k/10);
            })
            .or_panic();

          storage.query(&Everything.matching(&mut index, &rand::thread_rng().gen_range(0,10))).count();
          reduction.summarize(&storage);
        }
    }
}
