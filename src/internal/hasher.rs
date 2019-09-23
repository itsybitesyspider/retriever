#[cfg(not(feature = "use_fnv"))]
use std::collections::hash_map::DefaultHasher;
#[cfg(not(feature = "use_fnv"))]
use std::hash::BuildHasherDefault;

#[cfg(feature = "use_fnv")]
use fnv::FnvBuildHasher;

#[cfg(feature = "use_fnv")]
pub type HasherImpl = FnvBuildHasher;

#[cfg(not(feature = "use_fnv"))]
pub type HasherImpl = BuildHasherDefault<DefaultHasher>;
