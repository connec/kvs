//! A tiny library implementing a simple key value store (see [`KvStore`]).
//!
//! [`KvStore`]: struct.KvStore.html

#![deny(missing_docs)]

use rmp_serde::decode::{Error::InvalidMarkerRead, ReadReader};
use rmp_serde::encode::StructArrayWriter;
use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Seek, SeekFrom};
use std::mem;
use std::path::{Path, PathBuf};
use tempfile::tempfile;

/// The offset of the value in a serialized Command.
///
/// This is an (probably premature) optimisation for reading values from the log. By serializing the
/// `value` of [`Command::Set`] first we know where the value will be relative to the offset of the
/// serialized `Command`. Enum variants are serialized as a fixed array with two values - an integer
/// representing the variant's index and the serialized contents of the variant. The contents are
/// serialized as fixed arrays based on the number of contained values. This means the value for
/// `Command::Set` is serialized after:
/// - The `fixarray` format marker for the whole variant.
/// - The `positive fixint` format marker for the variant's index.
/// - The `fixarray` format marker for the number of fields in the variant.
const VALUE_OFFSET: u64 = 3;

/// The offset at which to try compacting.
///
/// Note: This drives a pretty broken compaction implementation where we rewrite a single log file
/// to remove duplicate commands.
const COMPACTION_OFFSET: u64 = 1000000;

/// A simple log-based key value store.
///
/// ```
/// # use std::path::PathBuf;
/// use kvs::{KvStore, Result};
///
/// # fn check() -> Result<()> {
/// # let path = PathBuf::new();
/// let mut store = KvStore::open(path)?;
///
/// store.set("hello".to_owned(), "world".to_owned())?;
/// assert_eq!(store.get("hello".to_owned())?, Some("workd".to_owned()));
///
/// store.remove("hello".to_owned())?;
/// assert_eq!(store.get("hello".to_owned())?, None);
/// # Ok(())
/// # }
/// ```
pub struct KvStore {
    path: PathBuf,
    writer: Serializer<File, StructArrayWriter>,
    reader: Deserializer<ReadReader<File>>,
    index: HashMap<String, u64>,
}

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
    NotFound(String),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::Io(err) => write!(f, "Database IO error: {}", err),
            Error::Decode(err) => write!(f, "Command decode error: {}", err),
            Error::Encode(err) => write!(f, "Command encode error: {}", err),
            Error::NotFound(key) => write!(f, "Key not found: {}", key),
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

/// An enum representing the available KvStore commands.
#[derive(Debug, Deserialize, Serialize)]
enum Command {
    // The field ordering is very important here. We rely on serializing the value first so we know
    // what offset to read it back from.
    Set { value: String, key: String },
    Remove { key: String },
}

impl KvStore {
    /// Construct a KvStore from an existing, persisted log.
    ///
    /// ```
    /// # use std::path::PathBuf;
    /// # use kvs::{KvStore, Result};
    /// # fn check() -> Result<()> {
    /// # let path = PathBuf::new();
    /// let store = KvStore::open(path)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = if path.as_ref().is_dir() {
            path.as_ref().join("1")
        } else {
            path.as_ref().to_path_buf()
        };

        let mut writer = Serializer::new(OpenOptions::new().create(true).write(true).open(&path)?);
        writer.get_mut().seek(SeekFrom::End(0))?;

        let mut reader = Deserializer::new(OpenOptions::new().read(true).open(&path)?);
        let mut index = HashMap::new();
        let mut offset = 0;
        while let Some(command) = KvStore::replay_command(&mut reader)? {
            match command {
                Command::Set { key, value: _ } => index.insert(key, offset + VALUE_OFFSET),
                Command::Remove { key } => index.remove(&key),
            };
            offset = reader.get_mut().seek(SeekFrom::Current(0))?;
        }

        Ok(KvStore {
            path,
            writer,
            reader,
            index,
        })
    }

    /// Get the value of a key in a store.
    ///
    /// ```
    /// # use std::path::PathBuf;
    /// # use kvs::{KvStore, Result};
    /// # fn check() -> Result<()> {
    /// # let path = PathBuf::new();
    /// let mut store = KvStore::open(path)?;
    /// store.get("foo".to_owned())?.unwrap_or_else(|| "<unset>".to_owned());
    /// # Ok(())
    /// # }
    /// ```
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let offset = match self.index.get(&key) {
            Some(offset) => *offset,
            None => return Ok(None),
        };

        self.reader.get_mut().seek(SeekFrom::Start(offset))?;
        Ok(Some(Deserialize::deserialize(&mut self.reader)?))
    }

    /// Set a key to a value in a store.
    ///
    /// ```
    /// # use std::path::PathBuf;
    /// # use kvs::{KvStore, Result};
    /// # fn check() -> Result<()> {
    /// # let path = PathBuf::new();
    /// let mut store = KvStore::open(path)?;
    /// store.set("foo".to_owned(), "bar".to_owned())?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = Command::Set {
            key: key.clone(),
            value: value.clone(),
        };
        let mut offset = self.writer.get_mut().seek(SeekFrom::Current(0))?;

        if offset > COMPACTION_OFFSET {
            offset = self.compact()?;
        }

        command.serialize(&mut self.writer)?;
        self.index.insert(key, offset + VALUE_OFFSET);

        Ok(())
    }

    /// Remove a key (and its value) from a store.
    ///
    /// ```
    /// # use std::path::PathBuf;
    /// # use kvs::{KvStore, Result};
    /// # fn check() -> Result<()> {
    /// # let path = PathBuf::new();
    /// let mut store = KvStore::open(path)?;
    /// store.remove("foo".to_owned())?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn remove(&mut self, key: String) -> Result<()> {
        if !self.index.contains_key(&key) {
            return Err(Error::NotFound(key));
        }

        let command = Command::Remove { key: key.clone() };
        command.serialize(&mut self.writer)?;
        self.index.remove(&key);
        Ok(())
    }

    fn replay_command(
        deserializer: &mut Deserializer<ReadReader<File>>,
    ) -> Result<Option<Command>> {
        Deserialize::deserialize(deserializer)
            .map(Some)
            .or_else(|err| match err {
                InvalidMarkerRead(_) => Ok(None),
                err => Err(err.into()),
            })
    }

    /// Perform an extremely dumb log compaction operation.
    ///
    /// This will truncate the log file and refill it with a minimal set of commands.
    fn compact(&mut self) -> Result<u64> {
        let mut rewriter = Serializer::new(tempfile()?);
        let mut new_offset = 0;
        for (key, offset) in self.index.iter_mut() {
            self.reader.get_mut().seek(SeekFrom::Start(*offset))?;
            let value = Deserialize::deserialize(&mut self.reader)?;
            Command::Set {
                key: key.to_owned(),
                value,
            }
            .serialize(&mut rewriter)?;
            *offset = new_offset + VALUE_OFFSET;
            new_offset = rewriter.get_mut().seek(SeekFrom::Current(0))?;
        }
        rewriter.get_mut().seek(SeekFrom::Start(0))?;

        fs::remove_file(&self.path)?;
        let mut new_writer = Serializer::new(File::create(&self.path)?);
        io::copy(rewriter.get_mut(), new_writer.get_mut())?;

        let new_reader = Deserializer::new(File::open(&self.path)?);

        mem::replace(&mut self.writer, new_writer);
        mem::replace(&mut self.reader, new_reader);

        Ok(new_offset)
    }
}
