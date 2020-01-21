/// Module for a trait that represents internal index sets.
pub mod idxset;
/// Module for a trait that measures memory usage and provides for cleanup of unused allocation.
pub mod memory_usage;
/// Module for a trait that defines various ways of querying stored data.
pub mod query;
/// Module for a trait that makes any type capable of being inserted into storage.
pub mod record;
/// Module for an automatically-derived trait for every type suitable to be used as a chunk key or item key.
pub mod valid_key;
