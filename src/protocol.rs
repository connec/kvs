use crate::error::Error;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

/// An enum representing a request to a server.
#[derive(Debug, Deserialize, Serialize)]
pub enum Request {
    /// Retrieve the value of a given key from a kvs server.
    ///
    /// The server will respond with either [`NotFound`], if the key is not in the store, or
    /// [`Found`], if the key is in the store (or [`Err`]).
    Get {
        /// The key whose value to get.
        key: String
    },

    /// Set a given to to a given value.
    ///
    /// The server will respond with [`Ok`] (or [`Err`]).
    Set {
        /// The key whose value to set.
        key: String,

        /// The value to set for the key.
        value: String
    },

    /// Remove a given key from the store.
    ///
    /// The server will respond with [`Ok`] (or [`Err`]).
    Remove {
        /// The key to remove.
        key: String
    },
}

/// An enum representing a response from a server.
#[derive(Debug, Deserialize, Serialize)]
pub enum Response {
    /// Indicates that a request succeeded with no value.
    Ok,

    /// Indicates that the key in a [`Get`] request was not found in the store.
    NotFound,

    /// Indicates that the key in a [`Get`] request was found in the store.
    Found {
        /// The value stored for the key.
        value: String
    },

    /// Indicates that an error occurred whilst attempting to process a request.
    Err {
        /// The kind of error that occurred.
        kind: ErrorKind,

        /// An error message.
        message: String
    },
}

/// An enum representing response error kinds.
#[derive(Debug, Deserialize, Serialize)]
pub enum ErrorKind {
    /// Indicates that a valid request could not be decoded.
    InvalidRequest,

    /// Indicates an error occurred in the storage engine.
    EngineError,
}

impl From<std::io::Error> for Response {
    fn from(err: std::io::Error) -> Self {
        Response::Err {
            kind: ErrorKind::EngineError,
            message: format!("{}", err),
        }
    }
}

impl From<rmp_serde::decode::Error> for Response {
    fn from(err: rmp_serde::decode::Error) -> Self {
        Response::Err {
            kind: ErrorKind::InvalidRequest,
            message: format!("{}", err),
        }
    }
}

impl TryFrom<Error> for Response {
    type Error = Error;

    fn try_from(error: Error) -> Result<Self, Self::Error> {
        match error {
            Error::Io(err) => Ok(err.into()),
            Error::Decode(err) => Ok(err.into()),
            Error::KeyNotFound => Ok(Response::NotFound),
            err => Err(err),
        }
    }
}
