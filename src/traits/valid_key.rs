use std::fmt::Debug;
use std::hash::Hash;

/// This trait defines the constraints for valid retriever keys.
/// All valid keys automatically implement this trait; you don't need to implement it manually.
pub trait ValidKey: Clone + Debug + Eq + Hash + Ord {}

impl<T> ValidKey for T where T: Clone + Debug + Eq + Hash + Ord {}
