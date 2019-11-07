/// Module for a data type representing the storage for a single chunk.
pub mod chunk_storage;
/// Module for an interface to edit stored values.
pub mod editor;
/// Module for an interface to edit stored values that may or may not exist.
pub mod entry;
/// Module for a data type that serves as reference to a stored value by it's chunk key and item key.
pub mod id;
/// Module for an interface to reduce a large number of collected values down to a single value.
pub mod reduction;
/// Module for the primary Storage type.
pub mod storage;
