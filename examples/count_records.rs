/// Count the number of records in all chunks or one chunk that meet some criteria.
///
/// In this example, count the number of spacecraft with people aboard using a `Reduction`.
///
/// When recalculating the count over a large number of records, retriever caches previous
/// results and only recalculates blocks of records that have changed.
use retriever::prelude::*;
use std::borrow::Cow;

fn main() {
    struct Spacecraft {
        location: String,
        id: u128,
        #[allow(dead_code)]
        name: String,
        crew: u64,
    }

    impl Record<str, u128> for Spacecraft {
        fn chunk_key(&self) -> Cow<str> {
            Cow::Borrowed(&self.location)
        }

        fn item_key(&self) -> Cow<u128> {
            Cow::Owned(self.id)
        }
    }

    let mut storage: Storage<str, u128, Spacecraft> = Storage::new();

    let mut number_of_crewed_spacecraft: Reduction<str, Spacecraft, u64> = Reduction::new(
        &storage,
        16,
        |spacecraft: &Spacecraft, _previous_count| Some(if spacecraft.crew > 0 { 1 } else { 0 }),
        |counts, _previous_count| Some(counts.iter().sum::<u64>()),
    );

    storage.add(Spacecraft {
        location: String::from("Earth"),
        id: 0,
        name: String::from("Sputnik"),
        crew: 0,
    });

    storage.add(Spacecraft {
        location: String::from("Earth"),
        id: 1,
        name: String::from("International Space Station"),
        crew: 6,
    });

    storage.add(Spacecraft {
        location: String::from("Earth"),
        id: 2,
        name: String::from("Soyuz"),
        crew: 3,
    });

    storage.add(Spacecraft {
        location: String::from("Mars"),
        id: 3,
        name: String::from("Mars Reconnaissance Orbiter"),
        crew: 0,
    });

    storage.add(Spacecraft {
        location: String::from("Moon"),
        id: 4,
        name: String::from("Apollo 13"),
        crew: 3,
    });

    assert_eq!(
        Some(&2),
        number_of_crewed_spacecraft.reduce_chunk(&storage, "Earth")
    );
    assert_eq!(
        Some(&0),
        number_of_crewed_spacecraft.reduce_chunk(&storage, "Mars")
    );
    assert_eq!(
        Some(&1),
        number_of_crewed_spacecraft.reduce_chunk(&storage, "Moon")
    );
    assert_eq!(Some(&3), number_of_crewed_spacecraft.reduce(&storage));
}
