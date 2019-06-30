//! A tiny library implementing a simple key value store (see [`KvStore`]).
//!
//! [`KvStore`]: struct.KvStore.html

#![deny(missing_docs)]

mod client;
mod engine;
mod error;
mod protocol;
mod server;

pub use client::Client;
pub use engine::{Engine as KvsEngine, KvStore, SledKvStore};
pub use error::{Error, Result};
pub use protocol::{Request, Response};
pub use server::Server;

/// The default address for a KVS server.
pub const DEFAULT_ADDRESS: &'static str = "127.0.0.1:4001";
