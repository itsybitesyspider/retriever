#[cfg(not(feature = "fnv"))]
use std::collections::hash_map::DefaultHasher;
#[cfg(not(feature = "fnv"))]
use std::hash::BuildHasherDefault;

#[cfg(feature = "fnv")]
use fnv::FnvBuildHasher;

#[cfg(feature = "fnv")]
pub type HasherImpl = FnvBuildHasher;

#[cfg(not(feature = "fnv"))]
pub type HasherImpl = BuildHasherDefault<DefaultHasher>;
