//! A tiny library implementing a simple key value store (see [`KvStore`]).
//!
//! [`KvStore`]: struct.KvStore.html

#![deny(missing_docs)]

mod command;
mod engine;
mod error;
mod log;
mod protocol;
mod server;
mod sled;
mod store;

pub use crate::sled::Sled;
pub use engine::{Engine, Engine as KvsEngine};
pub use error::{Error, Result};
pub use protocol::{Request, Response};
pub use server::Server;
pub use store::{Store, Store as KvStore};

/// The default address for a KVS server.
pub const DEFAULT_ADDRESS: &'static str = "127.0.0.1:4001";
