use std::fmt::Debug;
use std::hash::Hash;

/// This trait defines the constraints for valid retriever keys.
/// All valid keys automatically implement this trait; you never need to implement it manually.
pub trait ValidKey: Clone + Debug + Eq + Hash + Ord {}
impl<T> ValidKey for T where T: Clone + Debug + Eq + Hash + Ord {}

/// This trait defines the constraints for borrowed retriever keys.
/// All valid borrowed keys automatically implement this trait; you never need to implement it manually.
pub trait BorrowedKey: Debug + Eq + Hash + Ord + ToOwned
where
    Self::Owned: ValidKey,
{
}
impl<T> BorrowedKey for T
where
    T: Debug + Eq + Hash + Ord + ToOwned + ?Sized,
    T::Owned: ValidKey,
{
}
