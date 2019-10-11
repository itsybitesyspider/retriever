# retriever

Retriever is an embedded in-memory data store for rust applications.

![](./Callie_the_golden_retriever_puppy.jpg)

### Features:

* Document-oriented storage and retrieval.
* Index and query by unlimited secondary keys.
* Stored or computed (dynamic) keys (using Cow).
* Map-reduce-style queries, if you want them.
* Chunking: all records belonging to the same chunk are stored together in the same Vec.
* 100% safe Rust with no default dependencies.
* Lots of full-featured examples to get started!

### Retriever does not have:

* Any built-in persistance. However, you can easily access the raw data for any or all chunks
  and pass it to serde for serialization.

### ToDo: (I want these features, but they aren't yet implemented)
* Range queries
* Parallelism (probably via rayon)
* Boolean queries (union, intersection, difference, etc -- note: you can perform intersection
  queries now just by chaining query operators)
* External mutable iterators (currently only internal iteration is supported for mutation)

### Comparison to other databases (SQL, MongoDB, etc)

Unlike most databases, retriever stores your data as a plain old rust struct inside process memory.
It doesn't support access over a network from multiple clients.

Like a traditional database, retriever has a flexible indexing and query system.

### Comparison to ECS (entity-component-system) frameworks

Retriever can be used as a servicable component store, since records that share the same keys
are easy to cross-reference with each other. If chunks are small enough to fit in cache,
then this might even offer comparable performance, a hypothesis I float here without evidence.

Retriever seeks to exploit performance opportunities from high-cardinality data
(i.e., every data or index element is unique).
My sense is that ECSs exist to exploit performance opportunities from low-cardinality data
(i.e. there are thousands of instances of 13 types of monster in a dungeon and even those
13 types share many overlapping qualities).

### Getting started:

1. Create a rust struct or enum that will represents a data item that you want to store.
2. Choose a *chunk key* and *item key* for each instance of your record.
  * Many records can share the same chunk key.
  * No two records in the same chunk may have the same item key.
  * A record is therefore uniquely identified by it's (ChunkKey,ItemKey) pair.
  * Retriever uses Cow to get the keys for each record, so a key can be
    borrowed from the record *or* a key can be dynamically computed.
  * All keys must be `Clone + Debug + Eq + Hash + Ord`.
  * If you don't want to use chunking or aren't sure what to types of chunk key to choose,
    use () as the chunk key.
3. Implement the Record<ChunkKey,ItemKey> trait for your choice of record, chunk key, and item
   key types.
4. Create a new empty Storage object using `Storage::new()`.
5. If you want, create some secondary indexes using `SecondaryIndex::new_expensive()`. Define
   secondary indexes by writing a single closure that maps records into zero or more secondary
   keys.
6. Create some reductions using `Reduction::new_expensive()`. Define reductions by writing two
   closures: A map from the record type to a summary types, and a reduction (or fold) of
   several summaries into a single summary.
7. Keep the Storage, SecondaryIndexes, and Reductions together for later use.
   Avoid dropping SecondaryIndexes or Reductions, because they are expensive to re-compute.
8. Use `Storage::add()`, `Storage::iter()`, `Storage::query()`, `Storage::modify()`, and
   `Storage::remove()` to implement CRUD operations on your storage.
9. Use `Reduction::summarize()` to reduce your entire storage down to a single summary object.

#### More about how to choose a good chunk key

 * A good chunk key will keep related records together; even a complex series of queries
   should hopefully access only a few chunks at a time.
 * If the total size of a chunk is small enough, then the entire chunk and its indices
   might fit in cache, improving performance.
 * A good chunk key is predictable; you should hopefully know which chunk
   a record will be in before you look for it, or at least be able to narrow it down to a
   resonably small range of chunks, so that you don't have to search a lot of chunks to find it.
 * A good chunk key might correspond to persistant storage, such as a single file, or a
   corresponding grouping in an upstream database. It's easy to load and unload chunks as a
   block.
 * For stores that represent geographical information, a good chunk key might represent
   a map grid or other kinds of fenced mutually-exclusive areas.
 * For a time-series database, a good chunk key might represent a time interval.

