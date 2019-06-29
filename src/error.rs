/// An enum representing the errors that can occur when interacting with a KvStore.
#[derive(Debug)]
pub enum Error {
    /// Wraps IO errors that occur when trying to read or write log files.
    Io(std::io::Error),

    /// Wraps decoding errors that occur when trying to read log files.
    Decode(rmp_serde::decode::Error),

    /// Wraps encoding errors that occur when trying to write to log files.
    Encode(rmp_serde::encode::Error),

    /// Wraps a sled error.
    ///
    /// This is awful and I hate it.
    Sled(sled::Error),

    /// Indicates that a key could not be found.
    KeyNotFound,

    /// Indicates that a DB was loaded with the wrong engine.
    WrongEngine,
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            Error::Io(ref err) => Some(err),
            Error::Decode(ref err) => Some(err),
            Error::Encode(ref err) => Some(err),
            Error::Sled(ref err) => Some(err),
            _ => None
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::Io(err) => write!(f, "Database IO error: {}", err),
            Error::Decode(err) => write!(f, "Decode error: {}", err),
            Error::Encode(err) => write!(f, "Encode error: {}", err),
            Error::Sled(err) => write!(f, "Sled error: {}", err),
            Error::KeyNotFound => write!(f, "Key not found"),
            Error::WrongEngine => write!(f, "Wrong engine"),
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

impl From<sled::Error> for Error {
    fn from(err: sled::Error) -> Error {
        Error::Sled(err)
    }
}

/// A convenience `Result` alias that pins the error to our own.
pub type Result<V> = std::result::Result<V, Error>;
