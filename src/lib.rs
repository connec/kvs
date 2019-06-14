//! A tiny library implementing a simple key value store (see [`KvStore`]).
//!
//! [`KvStore`]: struct.KvStore.html

#![deny(missing_docs)]

use std::collections::HashMap;

/// A simple key value store.
///
/// This is currently a very this (and limited) wrapper over a `HashMap<String, String>`.
///
/// ```
/// use kvs::KvStore;
/// let store = KvStore::new();
/// ```
#[derive(Default)]
pub struct KvStore(HashMap<String, String>);

impl KvStore {
    /// Construct a new, empty, key value store.
    ///
    /// ```
    /// # use kvs::KvStore;
    /// let store = KvStore::new();
    /// ```
    pub fn new() -> KvStore {
        KvStore(HashMap::new())
    }

    /// Get the value of a key in a store.
    ///
    /// ```
    /// # use kvs::KvStore;
    /// let store = KvStore::new();
    /// store.get("foo".to_owned()).unwrap_or_else(|| "<unset>".to_owned());
    /// ```
    pub fn get(&self, key: String) -> Option<String> {
        self.0.get(&key).map(|s| s.to_owned())
    }

    /// Set a key to a value in a store.
    ///
    /// ```
    /// # use kvs::KvStore;
    /// let mut store = KvStore::new();
    /// store.set("foo".to_owned(), "bar".to_owned());
    /// ```
    pub fn set(&mut self, key: String, value: String) {
        self.0.insert(key, value);
    }

    /// Remove a key (and its value) from a store.
    ///
    /// ```
    /// # use kvs::KvStore;
    /// let mut store = KvStore::new();
    /// store.remove("foo".to_owned());
    /// ```
    pub fn remove(&mut self, key: String) {
        self.0.remove(&key);
    }
}
