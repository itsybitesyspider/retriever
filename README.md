# retriever


[![Crates.io](https://img.shields.io/crates/v/retriever.svg)](https://crates.io/crates/retriever)
[![Docs.rs](https://docs.rs/retriever/badge.svg)](https://docs.rs/retriever/latest/)

## What is it?

Retriever stores ordinary rust data types in a similar manner as a NoSQL database. It supports
relationships (including circular relationships) among elements, multiple-indexing, and
map-reduce-like summaries.

![Image of cute dog.](https://raw.githubusercontent.com/itsybitesyspider/retriever/master/doc/nami.jpg)

(Image of [Nami](https://twitter.com/nami_num_nums), a project admirer.)

### Features:

* Document-oriented storage and retrieval.
* Index by unlimited secondary keys.
* Create indexes at will and drop them when you no longer need them.
* Lazy indexing. Pay re-indexing costs when you query the index, not before.
* Choice of borrowed or computed (dynamic) keys (using [Cow](https://doc.rust-lang.org/std/borrow/enum.Cow.html)).
* Map-reduce-style summaries, if you want them.
* Chunking: (optional) all records belonging to the same chunk are stored together in the same Vec.
* 100% safe Rust with no default dependencies.
* Over 60 tests, doc-tests and benchmarks (need more)
* Lots of full-featured examples to get started!

### Retriever does not have:

* Parallelism. This is a "to-do".
* Persistence. You can access the raw data for any chunk
  and pass it to serde for serialization. See `Storage::raw()` for an example.
* Networking. Retriever is embedded in your application like any other crate. It doesn't
  access anything over the network, nor can it be accessed over a network.
* Novelty. I've tried to make Retriever as simple and obvious as possible, and I hope people
  will be able to pick it up and use it (and even contribute to it) with little learning curve.
  Where there are a lot of type parameters, I try to demystify them with appropriate documentation.

### Quick Docs:

Quick links to key API documentation:

[Storage](https://docs.rs/retriever/latest/retriever/types/storage/struct.Storage.html)
|
[Query](https://docs.rs/retriever/latest/retriever/traits/query/trait.Query.html)
|
[SecondaryIndex](https://docs.rs/retriever/latest/retriever/queries/secondary_index/struct.SecondaryIndex.html)
|
[Reduction](https://docs.rs/retriever/latest/retriever/types/reduction/struct.Reduction.html)

### Basic Example

In this example, we create a Storage of puppies from old American comic strips.

```rust
use retriever::prelude::*;
use std::borrow::Cow;

struct Puppy {
  name: String,
  age: u64,
}

impl Record<(),str> for Puppy {
  fn chunk_key(&self) -> Cow<()> {
    Cow::Owned(())
  }

  fn item_key(&self) -> Cow<str> {
    Cow::Borrowed(&self.name)
  }
}

let mut storage : Storage<(),str,Puppy> = Storage::new();

storage.add(Puppy {
  name: "Snoopy".to_string(),
  age: 70
});

storage.add(Puppy {
  name: "Odie".to_string(),
  age: 52,
});

storage.add(Puppy {
  name: "Marmaduke".to_string(),
  age: 66
});

assert_eq!(
  Some(52),
  storage.get(&ID.item("Odie")).map(|puppy| puppy.age)
);

assert_eq!(
  3,
  storage.query(Everything).count()
);

assert_eq!(
  2,
  storage.query(Everything.filter(|puppy: &Puppy| puppy.age > 60)).count()
);

```

### Extended Example

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

// Some convenience functions for describing puppies
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

  fn with_adopted_date(mut self, adopted_date: Date<Utc>) -> Puppy {
    self.adopted_date = Some(adopted_date);
    self
  }

  fn with_breeds(mut self, breeds: &[&str]) -> Puppy {
    self.breed.extend(breeds.iter().map(|breed| String::from(*breed)));
    self
  }

  fn with_parent(mut self, year: i32, name: &str) -> Puppy {
    self.parents.insert(ID.chunk(year).item(String::from(name)));
    self
  }
}

// We need to implement Record for our Puppy type.
// We choose the year the puppy was rescued as the chunk key,
// and the name of the puppy as the item key.
// Because of this design, we can never have two puppies with same name
// rescued in the same year. They would have the same Id.
impl Record<i32,str> for Puppy {
  fn chunk_key(&self) -> Cow<i32> {
    Cow::Owned(self.rescued_date.year())
  }

  fn item_key(&self) -> Cow<str> {
    Cow::Borrowed(&self.name)
  }
}

// Let's create a storage of puppies.
let mut storage : Storage<i32,str,Puppy> = Storage::new();

// Add some example puppies to work with
storage.add(
  Puppy::new("Lucky", Utc.ymd(2019, 3, 27))
    .with_adopted_date(Utc.ymd(2019, 9, 13))
    .with_breeds(&["beagle"])
);

storage.add(
  Puppy::new("Spot", Utc.ymd(2019, 1, 9))
    .with_breeds(&["labrador", "dalmation"])  // See below for correct spelling.
    .with_parent(2010, "Yeller")
);

storage.add(
  Puppy::new("JoJo", Utc.ymd(2018, 9, 2))
    .with_adopted_date(Utc.ymd(2019, 5, 1))
    .with_breeds(&["labrador","shepherd"])
    .with_parent(2010, "Yeller")
);

storage.add(
  Puppy::new("Yeller", Utc.ymd(2010, 8, 30))
    .with_adopted_date(Utc.ymd(2013, 12, 24))
    .with_breeds(&["labrador"])
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

Retriever can be used as a serviceable component store, because records that share the same keys
are easy to cross-reference with each other. But Retriever is not designed specifically for
game projects, and it tries to balance programmer comfort with reliability and performance.

ECSs use low-cardinality indexes to do an enormous amount of work very quickly.
Retriever uses high-cardinality indexes to avoid as much work as possible.

If you know you need to use [Data Oriented Design](http://www.dataorienteddesign.com/dodmain.pdf)
then you might consider an ECS like [specs](https://crates.io/crates/specs) or
[legion](https://crates.io/crates/legion).

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
   two closures: (1) A map from the record to a summary, and (2) a fold
   of several summaries into a single summary.
   Use `Reduction::reduce()` to reduce an entire storage to a single summary, or
   `Reduction::reduce_chunk()` to reduce a single chunk to a single summary.

#### More about how to choose a good chunk key:

 * A good chunk key will keep related records together; queries should usually just operate
   on a handful of chunks at a time.
 * A good chunk key is predictable; ideally you know what chunk a record is in before you
   go looking for it.
 * A good chunk key might correspond to persistent storage, such as a single file in the file
   system. It's easy to load and unload chunks as a block.
 * For stores that represent geographical or spatial information, a good chunk key
   might represent a grid square or some other subdivision strategy.
 * For a time-series database, a good chunk key might represent a time interval.
 * In a GUI framework, each window might have its own chunk, and each widget might be a record
   in that chunk.
 * If you want to perform a `Reduction` on only part of your storage, then that part must be defined
   as a single chunk. In the future, I want to implement convolutional reductions that map onto
   zero or more chunks.

#### About Cow

Retriever makes heavy use of [Cow](https://doc.rust-lang.org/std/borrow/enum.Cow.html)
to represent various kinds of index keys. Using `Cow` allows retriever to bridge a wide
range of use cases.

A `Cow<T>` is usually either `Cow::Owned(T)` or `Cow::Borrowed(&T)`. The generic parameter refers
to the borrowed form, so `Cow<str>` is either `Cow::Owned(String)` or `Cow::Borrowed(&str)`.
Whenever you see a generic parameter like `ChunkKey`, `ItemKey`, or `IndexKey`,
these keys should also be borrowed forms.

These are good:

* `Record<i64,str>`
* `Record<i64,&'static str>`
* `Record<i64,Arc<String>>`

This will work for the most part but it's weird:

* `Record<i64,String>`

### License

Retriever is licensed under your choice of either the
[ISC license](https://opensource.org/licenses/ISC)
(a permissive license) or the
[AGPL v3.0 or later](https://opensource.org/licenses/agpl-3.0)
(a strong copyleft license).

The photograph of the puppy is by Wikimedia Commons user MichaelMcPhee.
[Creative Commons Attribution 3.0 Unported](https://creativecommons.org/licenses/by/3.0/).
([Source](https://commons.wikimedia.org/wiki/File:Callie_the_golden_retriever_puppy.jpg))

#### Contributing

Unless you explicitly state otherwise, any contribution intentionally submitted for
inclusion in retriever by you, shall be licensed as ISC OR AGPL-3.0-or-later,
without any additional terms or conditions.

### How to Help

At this stage, any bug reports or questions about unclear documentation are highly valued.
Please be patient if I'm not able to respond immediately.
I'm also interested in any suggestions that would help further simplify the code base.

### To Do: (I want these features, but they aren't yet implemented)
* Parallelism (will probably be implemented behind a rayon feature flag)
* Sorted indexes / range queries
* Boolean queries (union, intersection, difference, etc -- note: you can perform intersection
  queries now just by chaining query operators)
* External mutable iterators (currently only internal iteration is supported for modify)
* More small vector optimization in some places where I expect it to matter
* Need rigorous testing for space usage (currently no effort is made to shrink storage
  or index vectors, this is probably priority #1 right now)
* Lazy item key indexing or opt-out for item keys is a potential performance win.
* Convolutional reductions summarizing zero or more source chunks.
* Idea: data elements could be stored in a [persistent data structure](https://en.wikipedia.org/wiki/Persistent_data_structure)
  which might make it possible to iterate over elements while separately mutating them. This idea needs research.
* Theoretically, I expect retriever's performance to break down beyond about
  16 million chunks of 16 million elements, and secondary indexes are simply not scalable
  for low-cardinality data. I would eventually like retriever to
  scale up to "every electron in the universe" if someone somehow ever legally acquires
  that tier of hardware.

License: ISC OR AGPL-3.0-or-later
