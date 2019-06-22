/// An enum representing the errors that can occur when interacting with a KvStore.
#[derive(Debug)]
pub enum Error {
    /// Wraps IO errors that occur when trying to read or write log files.
    Io(std::io::Error),

    /// Wraps decoding errors that occur when trying to read log files.
    Decode(rmp_serde::decode::Error),

    /// Wraps encoding errors that occur when trying to write to log files.
    Encode(rmp_serde::encode::Error),

    /// Indicates that a key could not be found.
    KeyNotFound,
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::Io(err) => write!(f, "Database IO error: {}", err),
            Error::Decode(err) => write!(f, "Command decode error: {}", err),
            Error::Encode(err) => write!(f, "Command encode error: {}", err),
            Error::KeyNotFound => write!(f, "Key not found"),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<rmp_serde::decode::Error> for Error {
    fn from(err: rmp_serde::decode::Error) -> Error {
        Error::Decode(err)
    }
}

impl From<rmp_serde::encode::Error> for Error {
    fn from(err: rmp_serde::encode::Error) -> Error {
        Error::Encode(err)
    }
}

/// A convenience `Result` alias that pins the error to our own.
pub type Result<V> = std::result::Result<V, Error>;
