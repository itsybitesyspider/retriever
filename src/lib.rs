#![forbid(unsafe_code)]
#![forbid(missing_docs)]
#![warn(clippy::all)]

//!
//! [![Crates.io](https://img.shields.io/crates/v/retriever.svg)](https://crates.io/crates/retriever)
//! [![Docs.rs](https://docs.rs/retriever/badge.svg)](https://docs.rs/retriever/latest/)
//!
//! # What is it?
//!
//! Retriever stores ordinary rust data types in a similar manner as a NoSQL database. It supports
//! relationships (including circular relationships) among elements, multiple-indexing, and
//! map-reduce-like summaries.
//!
//! ![Image of cute dog.](https://raw.githubusercontent.com/itsybitesyspider/retriever/master/doc/nami.jpg)
//!
//! (Image of [Nami](https://twitter.com/nami_num_nums), a project admirer.)
//!
//! ## Features:
//!
//! * Document-oriented storage and retrieval.
//! * Index by unlimited secondary keys.
//! * Create indexes at will and drop them when you no longer need them.
//! * Lazy indexing. Pay re-indexing costs when you query the index, not before.
//! * Choice of borrowed or computed (dynamic) keys (using [Cow](https://doc.rust-lang.org/std/borrow/enum.Cow.html)).
//! * Map-reduce-style summaries, if you want them.
//! * Chunking: (optional) all records belonging to the same chunk are stored together in the same Vec.
//! * 100% safe Rust with no default dependencies.
//! * Over 60 tests, doc-tests and benchmarks (need more)
//! * Lots of full-featured examples to get started!
//!
//! ## Retriever does not have:
//!
//! * Parallelism. This is a "to-do".
//! * Persistence. You can access the raw data for any chunk
//!   and pass it to serde for serialization. See `Storage::raw()` for an example.
//! * Networking. Retriever is embedded in your application like any other crate. It doesn't
//!   access anything over the network, nor can it be accessed over a network.
//! * Novelty. I've tried to make Retriever as simple and obvious as possible, and I hope people
//!   will be able to pick it up and use it (and even contribute to it) with little learning curve.
//!   Where there are a lot of type parameters, I try to demystify them with appropriate documentation.
//!
//! ## Quick Docs:
//!
//! Quick links to key API documentation:
//!
//! [Storage](https://docs.rs/retriever/latest/retriever/types/storage/struct.Storage.html)
//! |
//! [Query](https://docs.rs/retriever/latest/retriever/traits/query/trait.Query.html)
//! |
//! [SecondaryIndex](https://docs.rs/retriever/latest/retriever/queries/secondary_index/struct.SecondaryIndex.html)
//! |
//! [Reduction](https://docs.rs/retriever/latest/retriever/types/reduction/struct.Reduction.html)
//!
//! ## Basic Example
//!
//! In this example, perform some basic operations on puppies from old American comic strips.
//!
//! ```
//! use retriever::prelude::*;
//! use std::borrow::Cow;
//!
//! // Each Puppy has a name and age.
//! struct Puppy {
//!   name: String,
//!   age: u64,
//! }
//!
//! // Use the Puppy's name as it's key.
//! // Using () as the ChunkKey effectively disables chunking;
//! // this is recommended if you aren't sure what chunk key to use.
//! impl Record<(),str> for Puppy {
//!   fn chunk_key(&self) -> Cow<()> {
//!     Cow::Owned(())
//!   }
//!
//!   fn item_key(&self) -> Cow<str> {
//!     Cow::Borrowed(&self.name)
//!   }
//! }
//!
//! let mut storage : Storage<(),str,Puppy> = Storage::new();
//!
//! storage.add(Puppy {
//!   name: "Snoopy".to_string(),
//!   age: 70
//! });
//!
//! storage.add(Puppy {
//!   name: "Odie".to_string(),
//!   age: 52,
//! });
//!
//! storage.add(Puppy {
//!   name: "Marmaduke".to_string(),
//!   age: 66
//! });
//!
//! // Look up the age of a Puppy
//! assert_eq!(
//!   52,
//!   storage.get(&ID.item("Odie")).unwrap().age
//! );
//!
//! // Count the number of puppies older than 60 years.
//! assert_eq!(
//!   2,
//!   storage.query(Everything.filter(|puppy: &Puppy| puppy.age > 60)).count()
//! );
//!
//! ```
//!
//! ## Summarizing a storage with a Reduction
//!
//! In this example, each puppy has some number of bones, tennis balls, and squeaks.
//! Use a `Reduction` to maintain a total count of these items. A `Reduction` can efficiently
//! recalculate these totals whenever the `Storage` changes.
//!
//! ```
//! use retriever::prelude::*;
//! use std::borrow::Cow;
//!
//! struct Puppy {
//!   name: String,
//!   toys: Toys,
//! }
//!
//! #[derive(Clone,Copy,Debug,Default,Eq,PartialEq)]
//! struct Toys {
//!   bones: u64,
//!   tennis_balls: u64,
//!   squeaks: u64,
//! }
//!
//! impl Record<(),str> for Puppy {
//!   fn chunk_key(&self) -> Cow<()> {
//!     Cow::Owned(())
//!   }
//!
//!   fn item_key(&self) -> Cow<str> {
//!     Cow::Borrowed(&self.name)
//!   }
//! }
//!
//! let mut storage : Storage<(),str,Puppy> = Storage::new();
//! let mut reduction : Reduction<(),Puppy,Toys> = Reduction::new(
//!   &storage,
//!   2,
//!   |puppy: &Puppy, _| Some(puppy.toys),
//!   |toys: &[Toys], _| Some(Toys {
//!     bones: toys.iter().map(|toys| toys.bones).sum::<u64>(),
//!     tennis_balls: toys.iter().map(|toys| toys.tennis_balls).sum::<u64>(),
//!     squeaks: toys.iter().map(|toys| toys.squeaks).sum::<u64>(),
//!   })
//! );
//!
//! storage.add(Puppy {
//!   name: "Lazy".to_string(),
//!   toys: Toys { bones: 3, tennis_balls: 0, squeaks: 1 }
//! });
//!
//! storage.add(Puppy {
//!   name: "Toby".to_string(),
//!   toys: Toys { bones: 0, tennis_balls: 9, squeaks: 0 }
//! });
//!
//! storage.add(Puppy {
//!   name: "Ralph".to_string(),
//!   toys: Toys { bones: 0, tennis_balls: 0, squeaks: 3 }
//! });
//!
//! storage.add(Puppy {
//!   name: "Larry".to_string(),
//!   toys: Toys { bones: 1, tennis_balls: 0, squeaks: 2 }
//! });
//!
//! assert_eq!(
//!   &Toys { bones: 4, tennis_balls: 9, squeaks: 6 },
//!   reduction.reduce(&storage).unwrap()
//! );
//!
//! ```
//!
//! ## Extended Example
//!
//! ```
//! use retriever::prelude::*;
//! use std::borrow::Cow;
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
//! // Some convenience functions for describing puppies
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
//!   fn with_adopted_date(mut self, adopted_date: Date<Utc>) -> Puppy {
//!     self.adopted_date = Some(adopted_date);
//!     self
//!   }
//!
//!   fn with_breeds(mut self, breeds: &[&str]) -> Puppy {
//!     self.breed.extend(breeds.iter().map(|breed| String::from(*breed)));
//!     self
//!   }
//!
//!   fn with_parent(mut self, year: i32, name: &str) -> Puppy {
//!     self.parents.insert(ID.chunk(year).item(String::from(name)));
//!     self
//!   }
//! }
//!
//! // We need to implement Record for our Puppy type.
//! // We choose the year the puppy was rescued as the chunk key,
//! // and the name of the puppy as the item key.
//! // Because of this design, we can never have two puppies with same name
//! // rescued in the same year. They would have the same Id.
//! impl Record<i32,str> for Puppy {
//!   fn chunk_key(&self) -> Cow<i32> {
//!     Cow::Owned(self.rescued_date.year())
//!   }
//!
//!   fn item_key(&self) -> Cow<str> {
//!     Cow::Borrowed(&self.name)
//!   }
//! }
//!
//! // Let's create a storage of puppies.
//! let mut storage : Storage<i32,str,Puppy> = Storage::new();
//!
//! // Add some example puppies to work with
//! storage.add(
//!   Puppy::new("Lucky", Utc.ymd(2019, 3, 27))
//!     .with_adopted_date(Utc.ymd(2019, 9, 13))
//!     .with_breeds(&["beagle"])
//! );
//!
//! storage.add(
//!   Puppy::new("Spot", Utc.ymd(2019, 1, 9))
//!     .with_breeds(&["labrador", "dalmation"])  // See below for correct spelling.
//!     .with_parent(2010, "Yeller")
//! );
//!
//! storage.add(
//!   Puppy::new("JoJo", Utc.ymd(2018, 9, 2))
//!     .with_adopted_date(Utc.ymd(2019, 5, 1))
//!     .with_breeds(&["labrador","shepherd"])
//!     .with_parent(2010, "Yeller")
//! );
//!
//! storage.add(
//!   Puppy::new("Yeller", Utc.ymd(2010, 8, 30))
//!     .with_adopted_date(Utc.ymd(2013, 12, 24))
//!     .with_breeds(&["labrador"])
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
//! let q = Everything.filter(|puppy: &Puppy| puppy.rescued_date.month() == 3);
//! let mut rescued_in_march : Vec<_> = storage.query(&q)
//!   .map(|puppy| &puppy.name).collect();
//! rescued_in_march.sort();
//! assert_eq!(vec!["Lucky"], rescued_in_march);
//!
//! // Fix spelling of "dalmatian" on all puppies:
//! let q = Everything.filter(|puppy : &Puppy| puppy.breed.contains("dalmation"));
//! storage.modify(&q, |mut editor| {
//!   let puppy = editor.get_mut();
//!   puppy.breed.remove("dalmation");
//!   puppy.breed.insert(String::from("dalmatian"));
//! });
//! assert_eq!(0, storage.iter().filter(|x| x.breed.contains("dalmation")).count());
//! assert_eq!(1, storage.iter().filter(|x| x.breed.contains("dalmatian")).count());
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
//! Retriever can be used as a serviceable component store, because records that share the same keys
//! are easy to cross-reference with each other. But Retriever is not designed specifically for
//! game projects, and it tries to balance programmer comfort with reliability and performance.
//!
//! ECSs use low-cardinality indexes to do an enormous amount of work very quickly.
//! Retriever uses high-cardinality indexes to avoid as much work as possible.
//!
//! If you know you need to use [Data Oriented Design](http://www.dataorienteddesign.com/dodmain.pdf)
//! then you might consider an ECS like [specs](https://crates.io/crates/specs) or
//! [legion](https://crates.io/crates/legion).
//!
//! ## Getting started:
//!
//! 1. Create a rust struct or enum that represents a data item that you want to store.
//! 2. Choose a *chunk key* and *item key* for each instance of your record.
//!    * Many records can share the same chunk key.
//!    * No two records in the same chunk may have the same item key.
//!    * All keys must be `Clone + Debug + Eq + Hash + Ord`. See `ValidKey`.
//!    * If you don't want to use chunking or aren't sure what to types of chunk key to choose,
//!      use () as the chunk key. Chunking is a feature that exists to help you --
//!      you don't have to use it.
//! 3. Implement the Record<ChunkKey,ItemKey> trait for your choice of record, chunk key, and item
//!    key types.
//! 4. Create a new empty Storage object using `Storage::new()`.
//! 5. Use `Storage::add()`, `Storage::iter()`, `Storage::query()`, `Storage::modify()`, and
//!    `Storage::remove()` to implement CRUD operations on your storage.
//! 6. If you want, create some secondary indexes using `SecondaryIndex::new()`. Define
//!    secondary indexes by writing a single closure that maps records into zero or more secondary
//!    keys.
//! 7. If you want, create some reductions using `Reduction::new()`. Define reductions by writing
//!    two closures: (1) A map from the record to a summary, and (2) a fold
//!    of several summaries into a single summary.
//!    Use `Reduction::reduce()` to reduce an entire storage to a single summary, or
//!    `Reduction::reduce_chunk()` to reduce a single chunk to a single summary.
//!
//! ### More about how to choose a good chunk key:
//!
//!  * A good chunk key will keep related records together; queries should usually just operate
//!    on a handful of chunks at a time.
//!  * A good chunk key is predictable; ideally you know what chunk a record is in before you
//!    go looking for it.
//!  * A good chunk key might correspond to persistent storage, such as a single file in the file
//!    system. It's easy to load and unload chunks as a block.
//!  * For stores that represent geographical or spatial information, a good chunk key
//!    might represent a grid square or some other subdivision strategy.
//!  * For a time-series database, a good chunk key might represent a time interval.
//!  * In a GUI framework, each window might have its own chunk, and each widget might be a record
//!    in that chunk.
//!  * If you want to perform a `Reduction` on only part of your storage, then that part must be defined
//!    as a single chunk. In the future, I want to implement convolutional reductions that map onto
//!    zero or more chunks.
//!
//! ### About Cow
//!
//! Retriever makes heavy use of [Cow](https://doc.rust-lang.org/std/borrow/enum.Cow.html)
//! to represent various kinds of index keys. Using `Cow` allows retriever to bridge a wide
//! range of use cases.
//!
//! A `Cow<T>` is usually either `Cow::Owned(T)` or `Cow::Borrowed(&T)`. The generic parameter refers
//! to the borrowed form, so `Cow<str>` is either `Cow::Owned(String)` or `Cow::Borrowed(&str)`.
//! Whenever you see a generic parameter like `ChunkKey`, `ItemKey`, or `IndexKey`,
//! these keys should also be borrowed forms.
//!
//! These are good:
//!
//! * `Record<i64,str>`
//! * `Record<i64,&'static str>`
//! * `Record<i64,Arc<String>>`
//!
//! This will work for the most part but it's weird:
//!
//! * `Record<i64,String>`
//!
//! ## License
//!
//! Retriever is licensed under your choice of either the
//! [ISC license](https://opensource.org/licenses/ISC)
//! (a permissive license) or the
//! [AGPL v3.0 or later](https://opensource.org/licenses/agpl-3.0)
//! (a strong copyleft license).
//!
//! The photograph of the puppy is by Wikimedia Commons user MichaelMcPhee.
//! [Creative Commons Attribution 3.0 Unported](https://creativecommons.org/licenses/by/3.0/).
//! ([Source](https://commons.wikimedia.org/wiki/File:Callie_the_golden_retriever_puppy.jpg))
//!
//! ### Contributing
//!
//! Unless you explicitly state otherwise, any contribution intentionally submitted for
//! inclusion in retriever by you, shall be licensed as ISC OR AGPL-3.0-or-later,
//! without any additional terms or conditions.
//!
//! ## How to Help
//!
//! At this stage, any bug reports or questions about unclear documentation are highly valued.
//! Please be patient if I'm not able to respond immediately.
//! I'm also interested in any suggestions that would help further simplify the code base.
//!
//! ## To Do: (I want these features, but they aren't yet implemented)
//! * Parallelism (will probably be implemented behind a rayon feature flag)
//! * Sorted indexes / range queries
//! * Boolean queries (union, intersection, difference, etc -- note: you can perform intersection
//!   queries now just by chaining query operators)
//! * External mutable iterators (currently only internal iteration is supported for modify)
//! * More small vector optimization in some places where I expect it to matter
//! * Need rigorous testing for space usage (currently no effort is made to shrink storage
//!   or index vectors, this is probably priority #1 right now)
//! * Lazy item key indexing or opt-out for item keys is a potential performance win.
//! * Convolutional reductions summarizing zero or more source chunks.
//! * Idea: data elements could be stored in a [persistent data structure](https://en.wikipedia.org/wiki/Persistent_data_structure)
//!   which might make it possible to iterate over elements while separately mutating them. This idea needs research.
//! * Theoretically, I expect retriever's performance to break down beyond about
//!   16 million chunks of 16 million elements, and secondary indexes are simply not scalable
//!   for low-cardinality data. I would eventually like retriever to
//!   scale up to "every electron in the universe" if someone somehow ever legally acquires
//!   that tier of hardware.

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

    static_assertions::assert_impl_all!(Storage<u64,u64,(u64,u64,u64)>: Send, Sync);
    static_assertions::assert_impl_all!(Reduction<u64, (u64,u64,u64), u64>: Send, Sync);
    static_assertions::assert_impl_all!(SecondaryIndex<u64, (u64,u64,u64), std::collections::HashSet<u64>, u64>: Send, Sync);

    #[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
    struct X(u64, u64);

    impl Record<u64, u64> for X {
        fn chunk_key(&self) -> Cow<u64> {
            Cow::Owned((self.0 & 0x00F0) >> 4)
        }

        fn item_key(&self) -> Cow<u64> {
            Cow::Borrowed(&self.0)
        }
    }

    #[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
    struct S(String, String, String);

    impl Record<str, str> for S {
        fn chunk_key(&self) -> Cow<str> {
            Cow::Borrowed(&self.0)
        }

        fn item_key(&self) -> Cow<str> {
            Cow::Borrowed(&self.1)
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

        let mut small_odds: Vec<X> = storage
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

        // Reverse the order of the intersection to get the same result
        let mut odd_smalls: Vec<X> = storage
            .query(
                &Everything
                    .matching(&small, Cow::Owned(true))
                    .matching(&even_odd, Cow::Owned(true)),
            )
            .cloned()
            .collect();

        assert_eq!(3, small_odds.len());
        assert!(odd_smalls.contains(&X(0x101, 0x111)));
        assert!(odd_smalls.contains(&X(0x303, 0x333)));
        assert!(odd_smalls.contains(&X(0x505, 0x555)));
        assert!(!odd_smalls.contains(&X(0x202, 0x222)));
        assert!(!odd_smalls.contains(&X(0x707, 0x777)));

        small_odds.sort();
        odd_smalls.sort();
        assert_eq!(small_odds, odd_smalls);

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

    #[test]
    fn test_entry() {
        let mut storage: Storage<u64, u64, X> = Storage::new();

        storage
            .entry(&ID.chunk(0).item(0))
            .or_insert_with(|| X(0, 0));
        storage
            .entry(&ID.chunk(0).item(0))
            .or_insert_with(|| X(0, 0))
            .1 += 1;
        storage.entry(&ID.chunk(0).item(0)).and_modify(|x| {
            x.1 += 10;
        });
        storage
            .entry(&ID.chunk(0).item(0))
            .or_insert_with(|| X(0, 0))
            .1 += 1;
        assert_eq!(Some(&X(0, 12)), storage.entry(&ID.chunk(0).item(0)).get());
        storage.entry(&ID.chunk(0).item(0)).remove_if(|x| x.1 != 12);
        storage.entry(&ID.chunk(0).item(0)).or_panic();
        storage.entry(&ID.chunk(0).item(0)).remove_if(|x| x.1 == 12);
        assert_eq!(None, storage.entry(&ID.chunk(0).item(0)).get());
    }

    #[test]
    #[should_panic]
    fn test_entry_with_bogus_chunk() {
        let mut storage: Storage<u64, u64, X> = Storage::new();

        storage
            .entry(&ID.chunk(0).item(16))
            .or_insert_with(|| X(16, 0));
    }

    #[test]
    #[should_panic]
    fn test_entry_with_bogus_item() {
        let mut storage: Storage<u64, u64, X> = Storage::new();

        storage
            .entry(&ID.chunk(0).item(16))
            .or_insert_with(|| X(1, 0));
    }

    #[test]
    fn test_duplicate_clean() {
        let mut storage: Storage<u64, u64, X> = Storage::new();

        storage.add(X(0, 0));

        storage.entry(&ID.chunk(0).item(0));
        storage.remove(&ID.chunk(0).item(0), std::mem::drop);

        storage.validate();
    }

    #[test]
    fn test_str() {
        let mut storage: Storage<str, str, S> = Storage::new();

        storage.add(S(
            String::from("broberts"),
            String::from("name"),
            String::from("Bob Roberts"),
        ));
        storage.add(S(
            String::from("broberts"),
            String::from("password"),
            String::from("password1"),
        ));
        storage.add(S(
            String::from("ssmith"),
            String::from("name"),
            String::from("Sue Smith"),
        ));
        storage.add(S(
            String::from("ssmith"),
            String::from("password"),
            String::from("1234"),
        ));

        assert_eq!(
            Some("Bob Roberts"),
            storage
                .get(&ID.chunk("broberts").item("name"))
                .map(|s| s.2.as_str())
        );
        assert_eq!(
            Some("Bob Roberts"),
            storage
                .get(&ID.chunk(String::from("broberts")).item("name"))
                .map(|s| s.2.as_str())
        );
        assert_eq!(
            Some("Bob Roberts"),
            storage
                .get(&ID.chunk("broberts").item(String::from("name")))
                .map(|s| s.2.as_str())
        );
        assert_eq!(
            Some("Bob Roberts"),
            storage
                .get(
                    &ID.chunk(String::from("broberts"))
                        .item(String::from("name"))
                )
                .map(|s| s.2.as_str())
        );
        assert_eq!(
            Some("Bob Roberts"),
            storage
                .get(
                    &ID.chunk(Cow::Borrowed("broberts"))
                        .item(String::from("name"))
                )
                .map(|s| s.2.as_str())
        );
        assert_eq!(
            Some("Bob Roberts"),
            storage
                .get(
                    &ID.chunk(Cow::Owned(String::from("broberts")))
                        .item(Cow::Borrowed("name"))
                )
                .map(|s| s.2.as_str())
        );
        assert_eq!(
            Some("Bob Roberts"),
            storage
                .get(
                    &ID.chunk(Cow::Owned(String::from("broberts")))
                        .item(Cow::Owned(String::from("name")))
                )
                .map(|s| s.2.as_str())
        );

        assert_eq!(
            2,
            storage
                .query(Chunks(vec![String::from("broberts")]))
                .count()
        );
        assert_eq!(
            2,
            storage
                .query(Chunks(vec![Cow::Borrowed("broberts")]))
                .count()
        );
        assert_eq!(2, storage.query(Chunks(vec!["broberts"])).count());
    }
}
