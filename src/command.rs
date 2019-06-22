use serde::{Deserialize, Serialize};

/// An enum representing the available KvStore commands.
#[derive(Debug, Deserialize, Serialize)]
pub enum Command {
    /// Set a given `key` to a given `value`.
    ///
    /// **Note:** the field ordering is important here as it ensures the value is serialized before
    /// the key. This allows [`Reader`] to read values from disk without having to first read keys
    /// (e.g. when the location is known from an index).
    Set { value: String, key: String },

    /// Remove a given `key`.
    Remove { key: String },
}
