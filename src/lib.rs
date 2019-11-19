#![forbid(unsafe_code)]
#![forbid(missing_docs)]
#![warn(clippy::all)]

//! Retriever is an embedded, in-memory, document-oriented data store for rust applications.
//! It stores ordinary rust data types in a similar manner as a NoSQL database.
//!
//! Retriever is ideal when you need to index a collection by multiple properties,
//! you need a variety of relations between elements in a collection, or
//! or you need to maintain summary statistics about a collection.
//! Retriever can make your application data more easily discoverable, searchable, and auditable
//! compared to a "big jumble of plain old rust types."
//! Retriever can help reduce data redundancy and establish a single source of truth
//! for all values.
//!
//! ![](./Callie_the_golden_retriever_puppy.jpg)
//!
//! ## Features:
//!
//! * Document-oriented storage and retrieval.
//! * Index by unlimited secondary keys.
//! * Create indexes at will and drop them when no longer need them.
//! * Lazy indexing. Pay re-indexing costs when you query the index, not before.
//! * Choice of borrowed or computed (dynamic) keys (using [Cow](https://doc.rust-lang.org/std/borrow/enum.Cow.html)).
//! * Map-reduce-style operations, if you want them.
//! * Chunking: all records belonging to the same chunk are stored together in the same Vec.
//! * 100% safe Rust with no default dependencies, not that I'm religious about it.
//! * Over 60 tests, doc-tests and benchmarks (need more)
//! * Lots of full-featured examples to get started!
//!
//! ## Retriever does not have:
//!
//! * Parallelism. See "To Do" section.
//! * Persistence. You can access the raw data for any chunk
//!   and pass it to serde for serialization.
//! * Networking. Retriever is embedded in your application like any other crate.
//!
//! ## To Do: (I want these features, but they aren't yet implemented)
//! * Parallelism (will probably be implemented behind a rayon feature flag)
//! * Sorted indexes / range queries
//! * Boolean queries (union, intersection, difference, etc -- note: you can perform intersection
//!   queries now just by chaining query operators)
//! * External mutable iterators (currently only internal iteration is supported for modify)
//! * More small vector optimization in some places where I expect it to matter
//! * Need rigorous testing for space leaks (currently no effort is made to shrink storage
//!   OR index vectors, this is priority #1 right now)
//! * Theoretically, I expect retriever's performance to break down beyond about
//!   16 million chunks of 16 million elements, and secondary indexes are simply not scalable
//!   for low-cardinality data. I would eventually like retriever to
//!   scale up to "every electron in the universe" if someone somehow ever legally acquires
//!   that tier of hardware.
//!
//! ## Getting started:
//!
//! ```
//! use retriever::prelude::*;
//! use std::borrow::Cow;    // Cow is wonderful and simple to use but not widely known.
//!                          // It's just an enum with Cow::Owned(T) or Cow::Borrowed(&T).
//! use chrono::prelude::*;  // Using rust's Chrono crate to handle date/time
//!                          // (just for this example, you don't need it)
//! use std::collections::HashSet;
//!
//! // This example is going to be about a puppy rescue agency
//! struct Puppy {
//!   name: String,
//!   rescued_date: Date<Utc>,
//!   adopted_date: Option<Date<Utc>>,
//!   breed: HashSet<String>,
//!   parents: HashSet<Id<i32,String>>,
//! }
//!
//! // Some convenience functions for describing puppies in source code
//! impl Puppy {
//!   fn new(name: &str, rescued_date: Date<Utc>) -> Puppy {
//!     Puppy {
//!       name: String::from(name),
//!       rescued_date,
//!       adopted_date: None,
//!       breed: HashSet::default(),
//!       parents: HashSet::default(),
//!     }
//!   }
//!
//!   fn adopted(mut self, adopted_date: Date<Utc>) -> Puppy {
//!     self.adopted_date = Some(adopted_date);
//!     self
//!   }
//!
//!   fn breeds(mut self, breeds: &[&str]) -> Puppy {
//!     self.breed.extend(breeds.iter().map(|breed| String::from(*breed)));
//!     self
//!   }
//!
//!   fn parent(mut self, year: i32, name: &str) -> Puppy {
//!     self.parents.insert(ID.chunk(year).item(String::from(name)));
//!     self
//!   }
//! }
//!
//! // We need to implement Record for our Puppy type.
//! // Because of this design, we can never have two puppies with same name
//! // rescued in the same year. They would have the same Id.
//! impl Record<i32,String> for Puppy {
//!   fn chunk_key(&self) -> Cow<i32> {
//!     Cow::Owned(self.rescued_date.year())
//!   }
//!
//!   fn item_key(&self) -> Cow<String> {
//!     Cow::Borrowed(&self.name)
//!   }
//! }
//!
//! // Let's create a storage of puppies.
//! let mut storage : Storage<i32,String,Puppy> = Storage::new();
//!
//! // Add some example puppies to work with
//! storage.add(
//!   Puppy::new("Spot", Utc.ymd(2019, 1, 9))
//!     .breeds(&["labrador","dalmation"])
//!     .parent(2010, "Yeller")
//! );
//!
//! storage.add(
//!   Puppy::new("Lucky", Utc.ymd(2019, 3, 27))
//!     .adopted(Utc.ymd(2019, 9, 13))
//!     .breeds(&["dachshund","poodle"])
//! );
//!
//! storage.add(
//!   Puppy::new("JoJo", Utc.ymd(2018, 9, 2))
//!     .adopted(Utc.ymd(2019, 5, 1))
//!     .breeds(&["labrador","yorkie"])
//!     .parent(2010, "Yeller")
//! );
//!
//! storage.add(
//!   Puppy::new("Yeller", Utc.ymd(2010, 8, 30))
//!     .adopted(Utc.ymd(2013, 12, 24))
//!     .breeds(&["labrador"])
//! );
//!
//! // Get all puppies rescued in 2019:
//! let q = Chunks([2019]);
//! let mut rescued_2019 : Vec<_> = storage.query(&q)
//!   .map(|puppy: &Puppy| &puppy.name).collect();
//! rescued_2019.sort();  // can't depend on iteration order!
//! assert_eq!(vec!["Lucky","Spot"], rescued_2019);
//!
//! // Get all puppies rescued in the last 3 years:
//! let q = Chunks(2017..=2019);
//! let mut rescued_recently : Vec<_> = storage.query(&q)
//!   .map(|puppy: &Puppy| &puppy.name).collect();
//! rescued_recently.sort();
//! assert_eq!(vec!["JoJo","Lucky","Spot"], rescued_recently);
//!
//! // Get all puppies rescued in march:
//! // This is an inefficient query, because the 'filter' operation tests every record.
//! let q = Everything.filter(|puppy: &Puppy| puppy.rescued_date.month() == 3);
//! let mut rescued_in_march : Vec<_> = storage.query(&q)
//!   .map(|puppy| &puppy.name).collect();
//! rescued_in_march.sort();
//! assert_eq!(vec!["Lucky"], rescued_in_march);
//!
//! // Fix spelling of "dalmatian" on all puppies:
//! storage.modify(&Everything, |mut editor| {
//!   if editor.get().breed.contains("dalmation") {
//!     editor.get_mut().breed.remove("dalmation");
//!     editor.get_mut().breed.insert(String::from("dalmatian"));
//!   }
//! });
//!
//! // Set up an index of puppies by their parent.
//! // In SecondaryIndexes, we always return a collection of secondary keys.
//! // (In this case, a HashSet containing the Ids of the parents.)
//! let mut by_parents = SecondaryIndex::new(&storage,
//!   |puppy: &Puppy| Cow::Borrowed(&puppy.parents));
//!
//! // Use an index to search for all children of Yeller:
//! let yeller_id = ID.chunk(2010).item(String::from("Yeller"));
//! let q = Everything.matching(&mut by_parents, Cow::Borrowed(&yeller_id));
//! let mut children_of_yeller : Vec<_> = storage.query(&q)
//!   .map(|puppy: &Puppy| &puppy.name).collect();
//! children_of_yeller.sort();
//! assert_eq!(vec!["JoJo","Spot"], children_of_yeller);
//!
//! // Remove puppies who have been adopted more than five years ago.
//! let q = Chunks(0..2014).filter(|puppy: &Puppy|
//!   puppy.adopted_date.map(|date| date.year() <= 2014).unwrap_or(false));
//! assert!(storage.get(&yeller_id).is_some());
//! storage.remove(&q, std::mem::drop);
//! assert!(storage.get(&yeller_id).is_none());
//! ```
//!
//! ## Comparison to other databases (SQL, MongoDB, etc)
//!
//! Unlike most databases, retriever stores your data as a plain old rust data type inside heap memory.
//! (Specifically, each chunk has a Vec that stores all of the data for that chunk.)
//! It doesn't support access over a network from multiple clients.
//!
//! Like a traditional database, retriever has a flexible indexing and query system and can model
//! many-to-many relationships between records.
//!
//! ## Comparison to ECS (entity-component-system) frameworks
//!
//! Retriever can be used as a servicable component store, because records that share the same keys
//! are easy to cross-reference with each other.
//!
//! Retriever seeks to exploit performance opportunities from high-cardinality data
//! (i.e., every record has a unique or mostly-unique key).
//! My sense is that ECSs exist to exploit performance opportunities from low-cardinality data
//! (i.e. there are thousands of instances of 13 types of monster in a dungeon and even those
//! 13 types share many overlapping qualities). If you need to use [Data Oriented Design](http://www.dataorienteddesign.com/dodmain.pdf)
//! then you should an ECS like [specs](https://crates.io/crates/specs).
//!
//! ## Getting started:
//!
//! 1. Create a rust struct or enum that represents a data item that you want to store.
//! 2. Choose a *chunk key* and *item key* for each instance of your record.
//!   * Many records can share the same chunk key.
//!   * No two records in the same chunk may have the same item key.
//!   * A record is uniquely identified by it's (ChunkKey,ItemKey) pair.
//!   * Retriever uses Cow to get the chunk key and item key for each record, so a key can be
//!     borrowed from the record *or* a key can be dynamically computed.
//!   * All keys must be `Clone + Debug + Eq + Hash + Ord`. See `ValidKey`.
//!   * If you don't want to use chunking or aren't sure what to types of chunk key to choose,
//!     use () as the chunk key. Chunking is a feature that exists to help you --
//!     you don't have to use it.
//! 3. Implement the Record<ChunkKey,ItemKey> trait for your choice of record, chunk key, and item
//!    key types.
//! 4. Create a new empty Storage object using `Storage::new()`.
//! 5. If you want, create some secondary indexes using `SecondaryIndex::new()`. Define
//!    secondary indexes by writing a single closure that maps records into zero or more secondary
//!    keys.
//! 6. Create some reductions using `Reduction::new()`. Define reductions by writing two
//!    closures: (1) A map from the record type to a summary type, and (2) a reduction (or fold) of
//!    several summary objects into a single summary. The `Reduction` performs these reduction
//!    steps recursively until only single summary remains for the entire data store, and caches
//!    all intermediate steps so that recalculating after a change is fast.
//! 7. Keep the `Storage`, `SecondaryIndices`, and `Reductions` together for later use.
//!    Avoid dropping `SecondaryIndices` or `Reductions`, because they are expensive to re-compute.
//! 8. Use `Storage::add()`, `Storage::iter()`, `Storage::query()`, `Storage::modify()`, and
//!    `Storage::remove()` to implement CRUD operations on your storage.
//! 9. Use `Reduction::reduce()` to reduce your entire storage to a single summary object, or
//!    `Reduction::reduce_chunk()` to reduce a single chunk to a single summary object.
//!
//! ### More about how to choose a good chunk key
//!
//!  * A good chunk key will keep related records together; queries should usually just operate
//!    on a handful of chunks at a time.
//!  * A good chunk key is predictable; you should always know what chunks you need to search
//!    to find a record.
//!  * A good chunk key might correspond to persistant storage, such as a single file in the file
//!    file system. It's easy to load and unload chunks as a block.
//!  * For stores that represent geographical or spatial information information, a good chunk key
//!    might represent grid square or some other subdivision strategy.
//!  * For a time-series database, a good chunk key might represent a time interval.
//!  * In a GUI framework, each window might have its own chunk, and each widget might be a record
//!    in that chunk.
//!  * If you want to perform reductions on only part of your storage, then that part must be defined
//!    as a single chunk. In the future, I want to implement convolutional reductions that map onto
//!    multiple chunks, but I haven't yet imagined a reduction scheme that would somehow operate
//!    on partial chunks (nor have I imagined a motivation for doing this).
//!  * If chunks are small enough, then the entire chunk and it's index might fit into cache.
//!

/// Module that implements a sparse, compact `Bitset` implementation.
pub mod bits;
/// Module containing various `IdxSet` implementations.
pub mod idxsets;
mod internal;
/// Module exporting the most commonly-used features of Retriever.
pub mod prelude;
/// Module containing various strategies to query storage.
pub mod queries;
/// Module containing various strategies to reduce a storage to a single value.
pub mod reductions;
/// Module containing various traits.
pub mod traits;
/// Module containing various types.
pub mod types;

//
// Puppy is from: https://commons.wikimedia.org/wiki/File:Callie_the_golden_retriever_puppy.jpg
//

// Remainder of this file is unit tests.

#[cfg(test)]
mod test {
    use crate::prelude::*;
    use crate::types::reduction::Reduction;
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
        let index: SecondaryIndex<u64, X, Option<u64>, u64> =
            SecondaryIndex::new(&storage, |x: &X| Cow::Owned(Some(x.1 & 0x1)));

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
            storage
                .query(&Everything.matching(&index, Cow::Owned(0)))
                .count()
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
            storage
                .query(&Everything.matching(&index, Cow::Owned(0)))
                .count()
        );
    }

    #[test]
    fn test_editor() {
        let mut storage: Storage<u64, u64, X> = Storage::new();
        storage.add(X(0x101, 0x101));
        storage.add(X(0x202, 0x101));
        storage.add(X(0x111, 0x101));

        storage.modify(&Id(0x0, 0x202), |mut editor| {
            assert_eq!(&Id(&0x0, &0x202), editor.id());
            assert_eq!(&X(0x202, 0x101), editor.get());
            editor.get_mut().1 = 0x102;
            assert_eq!(&X(0x202, 0x102), editor.get());
        });

        storage.validate();
    }

    #[test]
    fn test_filter() {
        let mut storage: Storage<u64, u64, X> = Storage::new();
        storage.add(X(0x101, 0x101));
        storage.add(X(0x202, 0x999));
        storage.add(X(0x111, 0x111));

        storage.remove(&Chunks([0x0]).filter(|x: &X| x.1 == 0x999), std::mem::drop);
        assert_eq!(2, storage.iter().count());
        assert!(storage.get(&Id(0x0, 0x101)).is_some());
        assert!(storage.get(&Id(0x1, 0x111)).is_some());
        assert!(storage.get(&Id(0x0, 0x202)).is_none());

        storage.validate();
    }

    #[test]
    fn test_query_by_id() {
        let mut storage: Storage<u64, u64, X> = Storage::new();

        let even_odd: SecondaryIndex<u64, X, Option<bool>, bool> =
            SecondaryIndex::new(&storage, |x: &X| Cow::Owned(Some(x.1 % 2 == 1)));

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
                .query(&Id(0x0, 0x101).matching(&even_odd, Cow::Owned(true)))
                .next()
        );
        assert_eq!(
            None,
            storage
                .query(&Id(0x0, 0x101).matching(&even_odd, Cow::Owned(false)))
                .next()
        );

        storage.validate();
        even_odd.validate(&storage);
    }

    #[test]
    fn test_query_by_chunks() {
        let mut storage: Storage<u64, u64, X> = Storage::new();

        let even_odd: SecondaryIndex<u64, X, Option<bool>, bool> =
            SecondaryIndex::new(&storage, |x: &X| Cow::Owned(Some(x.1 % 2 == 1)));

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
            .query(&Chunks([0x0, 0x2]).matching(&even_odd, Cow::Owned(true)))
            .cloned()
            .collect();
        assert_eq!(
            &[X(0x101, 0x111), X(0x121, 0x111)],
            odd_items_even_chunks.as_slice()
        );

        storage.validate();
        even_odd.validate(&storage);
    }

    #[test]
    fn test_index_intersections() {
        let mut storage: Storage<u64, u64, X> = Storage::new();

        let even_odd: SecondaryIndex<u64, X, Option<bool>, bool> =
            SecondaryIndex::new(&storage, |x: &X| Cow::Owned(Some(x.1 % 2 == 1)));

        let small: SecondaryIndex<u64, X, Option<bool>, bool> =
            SecondaryIndex::new(&storage, |x: &X| Cow::Owned(Some(x.1 < 0x600)));

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
                    .matching(&even_odd, Cow::Owned(true))
                    .matching(&small, Cow::Owned(true)),
            )
            .cloned()
            .collect();

        assert_eq!(3, small_odds.len());
        assert!(small_odds.contains(&X(0x101, 0x111)));
        assert!(small_odds.contains(&X(0x303, 0x333)));
        assert!(small_odds.contains(&X(0x505, 0x555)));
        assert!(!small_odds.contains(&X(0x202, 0x222)));
        assert!(!small_odds.contains(&X(0x707, 0x777)));
        storage.validate();
        even_odd.validate(&storage);
        small.validate(&storage);
    }

    #[test]
    fn test_random_edits() {
        use rand::Rng;

        let mut storage: Storage<u64, u64, X> = Storage::new();
        let mut reduction: Reduction<u64, X, u64> = Reduction::new(
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
        let index: SecondaryIndex<u64, X, Option<u64>, u64> =
            SecondaryIndex::new(&storage, |x: &X| Cow::Owned(Some(x.1)));

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
                .query(
                    &Everything.matching(&index, Cow::Owned(rand::thread_rng().gen_range(0, 10))),
                )
                .count();
            reduction.reduce(&storage);
        }

        storage.validate();
        index.validate(&storage);
    }

    #[test]
    fn test_chunk_chaos() {
        use rand::Rng;

        let mut storage: Storage<u8, u8, (u8, u8, u8)> = Storage::new();
        let k = 255;

        for i in 0..k {
            storage.add((i, 0, 0));
        }

        for i in 0..k {
            if rand::thread_rng().gen() {
                storage.remove(ID.chunk(i).item(0), std::mem::drop);
            }
        }

        for i in 0..k {
            if rand::thread_rng().gen() {
                // this is likely to panic if the chunk index is broken
                storage.add((i, 1, 0));
            }
        }

        storage.validate();
    }
}
