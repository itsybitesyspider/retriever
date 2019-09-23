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
