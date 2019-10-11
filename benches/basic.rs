#[macro_use]
extern crate criterion;

use criterion::{BatchSize, Criterion, Throughput};
use retriever::queries::chunks::Chunks;
use retriever::queries::everything::Everything;
use retriever::queries::secondary_index::SecondaryIndex;
use retriever::summaries::reduction::Reduction;
use retriever::{Id, Query, Record, Storage};
use std::borrow::Cow;
use std::collections::HashMap;

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

fn bench_add_integers_baseline() {
    let mut storage = Vec::new();

    for i in 0..0x9999 {
        storage.push(i);
    }

    assert_eq!(storage[0x100], 0x100);
}

fn bench_hash_integers_baseline() {
    let mut storage = HashMap::new();

    for i in 0..0x9999 {
        storage.insert(i, i);
    }

    assert_eq!(storage.get(&0x100), Some(&0x100));
}

fn bench_add_integers() -> Storage<u64, u64, X> {
    let mut storage: Storage<u64, u64, X> = Storage::new();

    for i in 0..0x9999 {
        storage.add(X(i, i));
    }

    assert!(storage.get(&Id(0x0, 0x100)).is_some());
    assert!(storage.get(&Id(0x0, 0x1000000)).is_none());

    storage
}

fn bench_add_integers_single_chunk() -> Storage<u64, u64, X> {
    let mut storage: Storage<u64, u64, X> = Storage::new();

    for i in 0..0x9999 {
        storage.add(X(i << 8, i));
    }

    assert!(storage.get(&Id(0x0, 0x100)).is_some());
    assert!(storage.get(&Id(0x0, 0x1000000)).is_none());

    storage
}

fn bench_get_integers(storage: &Storage<u64, u64, X>) {
    assert!(storage.get(&Id(0x00, 0x100)).is_some());
    assert!(storage.get(&Id(0x00, 0x0)).is_some());
    assert!(storage.get(&Id(0x01, 0x15)).is_some());
    assert!(storage.get(&Id(0x08, 0x87)).is_some());
    assert!(storage.get(&Id(0x03, 0x39)).is_some());
    assert!(storage.get(&Id(0x09, 0x98)).is_some());
    assert!(storage.get(&Id(0x00, 0x2)).is_some());
    assert!(storage.get(&Id(0x07, 0x178)).is_some());
    assert!(storage.get(&Id(0x01, 0x9213)).is_some());
    assert!(storage.get(&Id(0x00, 0x1000000)).is_none());
}

fn bench_iter_integers(storage: &Storage<u64, u64, X>) {
    let sum = storage.iter().copied().map(|x| x.0).sum::<u64>();

    assert_eq!(773050860, sum);
}

fn bench_query_integers(storage: &Storage<u64, u64, X>) {
    let sum = storage
        .query(&Everything)
        .copied()
        .map(|x| x.0)
        .sum::<u64>();

    assert_eq!(773050860, sum);
}

fn bench_query_even_integers(storage: &Storage<u64, u64, X>) {
    let sum = storage
        .query(&Everything.filter(|x: &X| x.0 % 2 == 0))
        .copied()
        .map(|x| x.0)
        .sum::<u64>();

    assert_eq!(386535260, sum); // I have no verified this sum is correct
}

fn bench_query_even_integers_in_chunks(storage: &Storage<u64, u64, X>) {
    let chunks: Vec<_> = (0x00..=0x0F).into_iter().collect();
    let sum = storage
        .query(&Chunks(&chunks).filter(|x: &X| x.0 % 2 == 0))
        .copied()
        .map(|x| x.0)
        .sum::<u64>();

    assert_eq!(386535260, sum);
}

fn bench_modify_even_integers(storage: &mut Storage<u64, u64, X>) {
    storage.modify(Everything.filter(|x: &X| x.1 % 2 == 0), |mut editor| {
        editor.get_mut().1 += 1;
    });

    let result: Vec<X> = storage
        .query(&Everything.filter(|x: &X| x.1 % 2 == 0))
        .copied()
        .collect();

    assert_eq!(result.as_slice(), &[]);
}

fn bench_remove_even_integers(storage: &mut Storage<u64, u64, X>) {
    storage.remove(Everything.filter(|x: &X| x.1 % 2 == 0), std::mem::drop);

    let result: Vec<X> = storage
        .query(&Everything.filter(|x: &X| x.1 % 2 == 0))
        .copied()
        .collect();
    assert_eq!(result.as_slice(), &[]);
}

fn bench_discard_even_integers(storage: &mut Storage<u64, u64, X>) {
    storage.remove(Everything.filter(|x: &X| x.1 % 2 == 0), std::mem::drop);

    let result: Vec<X> = storage
        .query(&Everything.filter(|x: &X| x.1 % 2 == 0))
        .copied()
        .collect();
    assert_eq!(result.as_slice(), &[]);
}

fn bench_build_secondary_index(
    storage: &Storage<u64, u64, X>,
) -> SecondaryIndex<u64, X, Option<()>, ()> {
    SecondaryIndex::new_expensive(
        storage,
        |x: &X| {
            if x.1 % 0x1101 == 0 {
                Some(())
            } else {
                None
            }
        },
    )
}

fn bench_build_secondary_index_first_time(
    storage: &Storage<u64, u64, X>,
) -> SecondaryIndex<u64, X, Option<()>, ()> {
    let mut secondary = bench_build_secondary_index(storage);

    let count = storage
        .query(&Everything.matching(&mut secondary, &()))
        .count();
    assert_eq!(10, count);

    secondary
}

fn bench_query_secondary_index_next_time(
    storage: &Storage<u64, u64, X>,
    mut secondary: SecondaryIndex<u64, X, Option<()>, ()>,
) {
    let count = storage
        .query(&Everything.matching(&mut secondary, &()))
        .count();
    assert_eq!(10, count);
}

fn bench_make_changes(storage: &mut Storage<u64, u64, X>) {
    storage.entry(&Id(0x00, 0x101)).and_modify(|x| {
        x.1 = 0x202;
    });

    storage.entry(&Id(0x03, 0x232)).and_modify(|x| {
        x.1 = 0x101;
    });
}

fn bench_rebuild_secondary_index_after_change(
    storage: Storage<u64, u64, X>,
    mut secondary: SecondaryIndex<u64, X, Option<()>, ()>,
) {
    let count = storage
        .query(&Everything.matching(&mut secondary, &()))
        .count();
    assert_eq!(10, count);
}

fn bench_build_reduction_first_time(storage: &Storage<u64, u64, X>) -> Reduction<u64, X, u64> {
    let mut reduction = Reduction::new_expensive(
        storage,
        16,
        |x: &X, was| {
            if &x.1 == was {
                None
            } else {
                Some(x.1)
            }
        },
        |xs, was| {
            let sum = xs.iter().sum::<u64>();
            if &sum == was {
                None
            } else {
                Some(sum)
            }
        },
    );

    assert_eq!(Some(&773050860), reduction.summarize(storage));

    reduction
}

fn bench_rebuild_reduction_after_change(
    mut storage: Storage<u64, u64, X>,
    mut reduction: Reduction<u64, X, u64>,
) {
    assert_eq!(Some(&773050860), reduction.summarize(&mut storage));
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut everything_group = c.benchmark_group("Benchmarks that iterate over 0x9999 elements");
    everything_group.sample_size(20);
    everything_group.throughput(Throughput::Elements(0x9999));

    everything_group.bench_function(
        "bench_add_integers_baseline (baseline of adding integers to a Vec)",
        |b| b.iter(|| bench_add_integers_baseline()),
    );

    everything_group.bench_function(
        "bench_hash_integers_baseline (baseline of adding integers to a HashMap)",
        |b| b.iter(|| bench_hash_integers_baseline()),
    );

    everything_group.bench_function("bench_add_integers (39321 add() operations)", |b| {
        b.iter(|| bench_add_integers())
    });

    everything_group.bench_function("bench_add_integers_single_chunk (39321 add() operations, but all values happen to be in the same chunk)", |b| b.iter(|| bench_add_integers_single_chunk()));

    everything_group.bench_function(
        "bench_iter_integers (1 iter() operation over 39321 elements)",
        |b| {
            let storage = bench_add_integers();
            b.iter(|| bench_iter_integers(&storage))
        },
    );

    everything_group.bench_function(
        "bench_query_integers (1 query(&Everything) operation over 39321 elements)",
        |b| {
            let storage = bench_add_integers();
            b.iter(|| bench_query_integers(&storage))
        },
    );

    everything_group.bench_function("bench_query_even_integers (1 query(&Everything.filter(..)) operation over every other of 39321 elements)", |b| {
        let storage = bench_add_integers();
        b.iter(|| bench_query_even_integers(&storage))
    });

    everything_group.bench_function("bench_query_even_integers_in_chunks (1 query(&Chunks(&[..]).filter(..)) operation over every other of 39321 elements)", |b| {
        let storage = bench_add_integers();
        b.iter(|| bench_query_even_integers_in_chunks(&storage))
    });

    everything_group.bench_function("bench_modify_even_integers (1 modify(Everything.filter(..)) operation over every other of 39321 elements)", |b| {
        b.iter_batched(
            || bench_add_integers(),
            |mut storage| bench_modify_even_integers(&mut storage),
            BatchSize::LargeInput
        )
    });

    everything_group.bench_function("bench_remove_even_integers (1 remove(Everything.filter(..)) operation over every other of 39321 elements)", |b| {
    b.iter_batched(
     || bench_add_integers(),
     |mut storage| bench_remove_even_integers(&mut storage),
     BatchSize::LargeInput
    )
  });

    everything_group.bench_function("bench_discard_even_integers (1 discard(Everything.filter(..)) operation over every other of 39321 elements)", |b| {
  b.iter_batched(
   || bench_add_integers(),
   |mut storage| bench_discard_even_integers(&mut storage),
   BatchSize::LargeInput
  )
});

    everything_group.bench_function("bench_build_secondary_index_first_time", |b| {
        let storage = bench_add_integers();
        b.iter(|| bench_build_secondary_index_first_time(&storage))
    });

    everything_group.bench_function("bench_build_reduction_first_time", |b| {
        let storage = bench_add_integers();
        b.iter(|| bench_build_reduction_first_time(&storage))
    });

    everything_group.bench_function("bench_rebuild_reduction_after_change", |b| {
        b.iter_batched(
            || {
                let mut storage = bench_add_integers();
                let reduction = bench_build_reduction_first_time(&storage);
                bench_make_changes(&mut storage);
                (storage, reduction)
            },
            |(storage, reduction)| bench_rebuild_reduction_after_change(storage, reduction),
            BatchSize::LargeInput,
        )
    });

    everything_group.finish();

    let mut ten_group = c.benchmark_group("Benchmarks that visit 10 elements");
    ten_group.throughput(Throughput::Elements(10));

    ten_group.bench_function(
        "bench_get_integers (10 get() operations with hot cache)",
        |b| {
            let storage = bench_add_integers();
            b.iter(|| bench_get_integers(&storage))
        },
    );

    ten_group.bench_function("bench_query_secondary_index_next_time", |b| {
        let storage = bench_add_integers();

        b.iter_batched(
            || bench_build_secondary_index_first_time(&storage),
            |secondary_index| bench_query_secondary_index_next_time(&storage, secondary_index),
            BatchSize::LargeInput,
        )
    });

    ten_group.bench_function("bench_rebuild_secondary_index_after_change", |b| {
        b.iter_batched(
            || {
                let mut storage = bench_add_integers();
                let secondary_index = bench_build_secondary_index_first_time(&storage);
                bench_make_changes(&mut storage);
                (storage, secondary_index)
            },
            |(storage, secondary_index)| {
                bench_rebuild_secondary_index_after_change(storage, secondary_index)
            },
            BatchSize::LargeInput,
        )
    });

    ten_group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
