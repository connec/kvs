mod kvs;
mod sled;

use crate::error::Result;

pub use self::kvs::Store as KvStore;
pub use self::sled::Db as SledKvStore;

/// Defines the storage interface used from [`server::Server`].
///
/// [`server::Server`]:
pub trait Engine {
    /// Get the value of a key.
    fn get(&mut self, key: String) -> Result<Option<String>>;

    /// Set a key to a given value.
    fn set(&mut self, key: String, value: String) -> Result<()>;

    /// Remove a key (and its value).
    fn remove(&mut self, key: String) -> Result<()>;
}
