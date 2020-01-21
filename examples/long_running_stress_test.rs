use chrono::{DateTime, Local};
use rand::seq::SliceRandom;
/// This is a long-running stress test designed to identify memory leaks.
use retriever::prelude::*;
use retriever::traits::memory_usage::MemoryUser;
use std::borrow::Cow;

fn main() {
    #[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
    struct X(u64);

    impl Record<u64, u64> for X {
        fn chunk_key(&self) -> Cow<u64> {
            let chunk = self.0 >> 16;
            Cow::Owned(chunk)
        }

        fn item_key(&self) -> Cow<u64> {
            Cow::Owned(self.0)
        }
    }

    let mut storage: Storage<u64, u64, X> = Storage::new();
    let mut sum: Reduction<u64, X, u64> = Reduction::new(
        &storage,
        16,
        |x: &X, _| Some(x.0),
        |xs, _| Some(xs.iter().sum::<u64>()),
    );
    let mut unlucky: SecondaryIndex<u64, X, Option<()>, ()> =
        SecondaryIndex::new(&storage, |x: &X| {
            if x.0 % 1313 == 0 {
                Cow::Owned(Some(()))
            } else {
                Cow::Owned(None)
            }
        });

    let mut duplicate_storage: Vec<X> = Vec::new();
    let mut join_storage = Vec::new();

    let mut i = 0;

    while i < 10_000_000 {
        let x = X(i);

        storage.add(x);
        join_storage.push(x);

        i += 1;
    }

    loop {
        duplicate_storage.append(&mut join_storage);
        duplicate_storage.shuffle(&mut rand::thread_rng());

        for _ in 0..1000 {
            for _j in 0..1000 {
                let x = X(i);

                storage.add(x);
                join_storage.push(x);

                i += 1;
            }

            for _j in 0..1000 {
                let x = duplicate_storage.pop().unwrap();
                storage.entry(x).remove();
            }

            sum.reduce(&storage);
            storage
                .query(Everything.matching(&unlucky, Cow::Owned(())))
                .count();
        }

        storage.shrink();
        sum.shrink();
        unlucky.shrink();

        let now: DateTime<Local> = Local::now();
        println!("{}", now);
        println!("{:?}", storage.memory_usage());
        println!("{:?}", sum.memory_usage());
        println!("{:?}", unlucky.memory_usage());
    }
}
