# retriever

Retriever is an embedded, in-memory, document-oriented data store for rust applications.
It stores ordinary rust data types in a similar manner as a NoSQL database.

Retriever is ideal when you need to index a collection by multiple properties,
you need a variety of relations between elements in a collection, or
or you need to maintain summary statistics about a collection.

![](./Callie_the_golden_retriever_puppy.jpg)

### Features:

* Document-oriented storage and retrieval.
* Index by unlimited secondary keys.
* Create indexes at will and drop them when you no longer need them.
* Lazy indexing. Pay re-indexing costs when you query the index, not before.
* Choice of borrowed or computed (dynamic) keys (using [Cow](https://doc.rust-lang.org/std/borrow/enum.Cow.html)).
* Map-reduce-style operations, if you want them.
* Chunking: all records belonging to the same chunk are stored together in the same Vec.
* 100% safe Rust with no default dependencies.
* Over 60 tests, doc-tests and benchmarks (need more)
* Lots of full-featured examples to get started!

### (Cargo) Features:

* `fnv`: Index using the [fnv](https://crates.io/crates/fnv) hasher, which is faster but has
  security caveats.
* `log`: [Log](https://crates.io/crates/log) non-critical problems and performance caveats.
* `smallvec`: Support [small vectors](https://crates.io/crates/smallvec) in certain positions
  and also use small vector optimizations internally.

### Retriever does not have:

* Parallelism. This is a "to-do".
* Persistence. You can access the raw data for any chunk
  and pass it to serde for serialization. See `Storage::raw()` for an example.
* Networking. Retriever is embedded in your application like any other crate. It doesn't
  access anything over the network, nor can be accessed over a network.

### Cow

Retriever makes heavy use of [Cow](https://doc.rust-lang.org/std/borrow/enum.Cow.html)
to represent various kinds of index keys. Using `Cow` allows retriever to bridge a wide
range of use cases. A `Cow<T>` is either `Cow::Owned(T)` or `Cow::Borrowed(&T)`.

### Getting started:

```rust
use retriever::prelude::*;
use std::borrow::Cow;
use chrono::prelude::*;  // Using rust's Chrono crate to handle date/time
                         // (just for this example, you don't need it)
use std::collections::HashSet;

// This example is going to be about a puppy rescue agency
struct Puppy {
  name: String,
  rescued_date: Date<Utc>,
  adopted_date: Option<Date<Utc>>,
  breed: HashSet<String>,
  parents: HashSet<Id<i32,String>>,
}

// Some convenience functions for describing puppies in source code
impl Puppy {
  fn new(name: &str, rescued_date: Date<Utc>) -> Puppy {
    Puppy {
      name: String::from(name),
      rescued_date,
      adopted_date: None,
      breed: HashSet::default(),
      parents: HashSet::default(),
    }
  }

  fn adopted(mut self, adopted_date: Date<Utc>) -> Puppy {
    self.adopted_date = Some(adopted_date);
    self
  }

  fn breeds(mut self, breeds: &[&str]) -> Puppy {
    self.breed.extend(breeds.iter().map(|breed| String::from(*breed)));
    self
  }

  fn parent(mut self, year: i32, name: &str) -> Puppy {
    self.parents.insert(ID.chunk(year).item(String::from(name)));
    self
  }
}

// We need to implement Record for our Puppy type.
// Because of this design, we can never have two puppies with same name
// rescued in the same year. They would have the same Id.
impl Record<i32,String> for Puppy {
  fn chunk_key(&self) -> Cow<i32> {
    Cow::Owned(self.rescued_date.year())
  }

  fn item_key(&self) -> Cow<String> {
    Cow::Borrowed(&self.name)
  }
}

// Let's create a storage of puppies.
let mut storage : Storage<i32,String,Puppy> = Storage::new();

storage.add(
  Puppy::new("Lucky", Utc.ymd(2019, 3, 27))
    .adopted(Utc.ymd(2019, 9, 13))
    .breeds(&["beagle"])
);

// Add some example puppies to work with
storage.add(
  Puppy::new("Spot", Utc.ymd(2019, 1, 9))
    .breeds(&["dalmation"])
    .parent(2010, "Yeller")
);

storage.add(
  Puppy::new("JoJo", Utc.ymd(2018, 9, 2))
    .adopted(Utc.ymd(2019, 5, 1))
    .breeds(&["labrador","shepherd"])
    .parent(2010, "Yeller")
);

storage.add(
  Puppy::new("Yeller", Utc.ymd(2010, 8, 30))
    .adopted(Utc.ymd(2013, 12, 24))
    .breeds(&["labrador"])
);

// Get all puppies rescued in 2019:
let q = Chunks([2019]);
let mut rescued_2019 : Vec<_> = storage.query(&q)
  .map(|puppy: &Puppy| &puppy.name).collect();
rescued_2019.sort();  // can't depend on iteration order!
assert_eq!(vec!["Lucky","Spot"], rescued_2019);

// Get all puppies rescued in the last 3 years:
let q = Chunks(2017..=2019);
let mut rescued_recently : Vec<_> = storage.query(&q)
  .map(|puppy: &Puppy| &puppy.name).collect();
rescued_recently.sort();
assert_eq!(vec!["JoJo","Lucky","Spot"], rescued_recently);

// Get all puppies rescued in march:
let q = Everything.filter(|puppy: &Puppy| puppy.rescued_date.month() == 3);
let mut rescued_in_march : Vec<_> = storage.query(&q)
  .map(|puppy| &puppy.name).collect();
rescued_in_march.sort();
assert_eq!(vec!["Lucky"], rescued_in_march);

// Fix spelling of "dalmatian" on all puppies:
let q = Everything.filter(|puppy : &Puppy| puppy.breed.contains("dalmation"));
storage.modify(&q, |mut editor| {
  let puppy = editor.get_mut();
  puppy.breed.remove("dalmation");
  puppy.breed.insert(String::from("dalmatian"));
});
assert_eq!(0, storage.iter().filter(|x| x.breed.contains("dalmation")).count());
assert_eq!(1, storage.iter().filter(|x| x.breed.contains("dalmatian")).count());

// Set up an index of puppies by their parent.
// In SecondaryIndexes, we always return a collection of secondary keys.
// (In this case, a HashSet containing the Ids of the parents.)
let mut by_parents = SecondaryIndex::new(&storage,
  |puppy: &Puppy| Cow::Borrowed(&puppy.parents));

// Use an index to search for all children of Yeller:
let yeller_id = ID.chunk(2010).item(String::from("Yeller"));
let q = Everything.matching(&mut by_parents, Cow::Borrowed(&yeller_id));
let mut children_of_yeller : Vec<_> = storage.query(&q)
  .map(|puppy: &Puppy| &puppy.name).collect();
children_of_yeller.sort();
assert_eq!(vec!["JoJo","Spot"], children_of_yeller);

// Remove puppies who have been adopted more than five years ago.
let q = Chunks(0..2014).filter(|puppy: &Puppy|
  puppy.adopted_date.map(|date| date.year() <= 2014).unwrap_or(false));
assert!(storage.get(&yeller_id).is_some());
storage.remove(&q, std::mem::drop);
assert!(storage.get(&yeller_id).is_none());
```

### Comparison to other databases (SQL, MongoDB, etc)

Unlike most databases, retriever stores your data as a plain old rust data type inside heap memory.
(Specifically, each chunk has a Vec that stores all of the data for that chunk.)
It doesn't support access over a network from multiple clients.

Like a traditional database, retriever has a flexible indexing and query system and can model
many-to-many relationships between records.

### Comparison to ECS (entity-component-system) frameworks

Retriever can be used as a servicable component store, because records that share the same keys
are easy to cross-reference with each other.

Retriever seeks to exploit performance opportunities from high-cardinality data
(i.e., every record has a unique or mostly-unique key).
My sense is that ECSs exist to exploit performance opportunities from low-cardinality data
(i.e. there are thousands of instances of 13 types of monster in a dungeon and even those
13 types share many overlapping qualities). If you need to use [Data Oriented Design](http://www.dataorienteddesign.com/dodmain.pdf)
then you might consider an ECS like [specs](https://crates.io/crates/specs) or [legion](https://crates.io/crates/legion).

### Getting started:

1. Create a rust struct or enum that represents a data item that you want to store.
2. Choose a *chunk key* and *item key* for each instance of your record.
  * Many records can share the same chunk key.
  * No two records in the same chunk may have the same item key.
  * All keys must be `Clone + Debug + Eq + Hash + Ord`. See `ValidKey`.
  * If you don't want to use chunking or aren't sure what to types of chunk key to choose,
    use () as the chunk key. Chunking is a feature that exists to help you --
    you don't have to use it.
3. Implement the Record<ChunkKey,ItemKey> trait for your choice of record, chunk key, and item
   key types.
4. Create a new empty Storage object using `Storage::new()`.
5. Use `Storage::add()`, `Storage::iter()`, `Storage::query()`, `Storage::modify()`, and
   `Storage::remove()` to implement CRUD operations on your storage.
6. If you want, create some secondary indexes using `SecondaryIndex::new()`. Define
   secondary indexes by writing a single closure that maps records into zero or more secondary
   keys.
7. If you want, create some reductions using `Reduction::new()`. Define reductions by writing
   two closures: (1) A map from the record type to a summary type, and (2) a reduction
   (or fold) of several summary objects into a single summary.
8. Use `Reduction::reduce()` to reduce your entire storage to a single summary object, or
   `Reduction::reduce_chunk()` to reduce a single chunk to a single summary object.

#### More about how to choose a good chunk key:

 * A good chunk key will keep related records together; queries should usually just operate
   on a handful of chunks at a time.
 * A good chunk key is predictable; you should always know what chunks you need to search
   to find a record.
 * A good chunk key might correspond to persistant storage, such as a single file in the file
   file system. It's easy to load and unload chunks as a block.
 * For stores that represent geographical or spatial information, a good chunk key
   might represent grid square or some other subdivision strategy.
 * For a time-series database, a good chunk key might represent a time interval.
 * In a GUI framework, each window might have its own chunk, and each widget might be a record
   in that chunk.
 * If you want to perform reductions on only part of your storage, then that part must be defined
   as a single chunk. In the future, I want to implement convolutional reductions that map onto
   zero or more chunks.
 * If chunks are small enough, then the entire chunk and it's index might fit into cache.

### License

### To Do: (I want these features, but they aren't yet implemented)
* Parallelism (will probably be implemented behind a rayon feature flag)
* Sorted indexes / range queries
* Boolean queries (union, intersection, difference, etc -- note: you can perform intersection
  queries now just by chaining query operators)
* External mutable iterators (currently only internal iteration is supported for modify)
* More small vector optimization in some places where I expect it to matter
* Need rigorous testing for space usage (currently no effort is made to shrink storage
  OR index vectors, this is priority #1 right now)
* Lazy item key indexing is a potential performance win.
* Convolution reductions mapping zero or more source chunks onto one destination chunk.
* Idea: data elements could be stored in a [persistant data structure](https://en.wikipedia.org/wiki/Persistent_data_structure)
  which might make it possible to iterate over elements while seperately mutating them. This idea needs research.
* Theoretically, I expect retriever's performance to break down beyond about
  16 million chunks of 16 million elements, and secondary indexes are simply not scalable
  for low-cardinality data. I would eventually like retriever to
  scale up to "every electron in the universe" if someone somehow ever legally acquires
  that tier of hardware.

License: ISC OR GPL-3.0-or-later
