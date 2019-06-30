mod log;

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::mem;
use std::path::{Path, PathBuf};

use crate::engine::Engine;
use crate::error::{Error, Result};
use self::log::{Command, Offset, Reader, Writer};

/// The offset at which to try compacting.
///
/// Note: This drives a pretty broken compaction implementation where we rewrite a single log file
/// to remove duplicate commands.
const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// A simple log-based key value store.
///
/// The log is persisted to disk as files in a given directory. These files will be named for a
/// monotonically increasing 'log index' with a `.log` extension. The contents of the files should
/// be considered opaque (currently they contain a sequence of [MessagePack]-encoded 'commands', the
/// details of which are private to the crate).
///
/// ```
/// # use std::path::PathBuf;
/// use kvs::{KvsEngine, KvStore, Result};
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
pub struct Store {
    path: PathBuf,
    log_index: u64,
    writer: Writer,
    readers: HashMap<u64, Reader>,
    index: HashMap<String, IndexEntry>,
    uncompacted: u64,
}

/// An entry in a command index.
#[derive(Debug)]
struct IndexEntry {
    log_index: u64,
    offset: Offset,
    length: u64,
}

impl Store {
    /// Construct a Store from an existing, persisted log.
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
    pub fn open<P: Into<PathBuf>>(path: P) -> Result<Self> {
        let path = path.into();
        fs::create_dir_all(&path)?;

        let mut uncompacted = 0;
        let mut index: HashMap<String, IndexEntry> = HashMap::new();
        let mut readers = HashMap::new();

        let log_indices = find_log_indices(&path)?;

        for &log_index in &log_indices {
            let mut reader = open_reader(&path, log_index)?;
            for entry in reader.load()? {
                uncompacted += open_entry(log_index, &mut index, entry?);
            }
            readers.insert(log_index, reader);
        }

        let write_index = *log_indices.last().unwrap_or(&0);
        let writer = open_writer(&path, write_index)?;
        if readers.is_empty() {
            readers.insert(write_index, open_reader(&path, write_index)?);
        }

        Ok(Store {
            path,
            log_index: write_index,
            writer,
            readers,
            index,
            uncompacted,
        })
    }

    /// Compact the log directory to a single file.
    ///
    /// This will dump the keys and values currently in the index into a new log file and advance
    /// the current log index/writer to another new log file. It also resets the `uncompacted` count
    /// as the log will be minimal once `compact` completes.
    fn compact(&mut self) -> Result<()> {
        // Set up a file for the compacted log.
        let compaction_index = self.log_index + 1;
        let mut compaction_writer = open_writer(&self.path, compaction_index)?;
        self.readers.insert(compaction_index, open_reader(&self.path, compaction_index)?);

        // Set up a file for future commands.
        let write_index = compaction_index + 1;
        let writer = open_writer(&self.path, write_index)?;
        self.log_index = write_index;
        self.writer = writer;
        self.readers.insert(self.log_index, open_reader(&self.path, write_index)?);

        // Go through the index and write out a `Command::Set` for each value. The resulting log
        // file will be free from `Remove` commands or duplicate `Set`s for the same key, making it
        // minimal.
        for (key, entry) in self.index.iter_mut() {
            let reader = self.readers.get_mut(&entry.log_index).expect("Missing reader");
            let value = reader.read_value(&entry.offset)?;
            let command = Command::Set { key: key.to_owned(), value };
            let (offset, length) = compaction_writer.write(&command)?;

            // Update the index in-place with the new details.
            *entry = IndexEntry {
                log_index: compaction_index,
                offset,
                length
            };
        }

        // Delete the log files that are now redundant.
        let old_log_indices: Vec<_> = self
            .readers
            .keys()
            .filter(|&&log_index| log_index < compaction_index)
            .cloned()
            .collect();
        for old_index in old_log_indices {
            fs::remove_file(log_path(&self.path, old_index))?;
            self.readers.remove(&old_index);
        }

        // Reset the number of uncompacted bytes (if we don't do this `compact` will be called on
        // every subsequent call to `set` - not good).
        self.uncompacted = 0;

        Ok(())
    }
}

impl Engine for Store {
    /// Get the value of a key in a store.
    ///
    /// ```
    /// # use std::path::PathBuf;
    /// # use kvs::{KvsEngine, KvStore, Result};
    /// # fn check() -> Result<()> {
    /// # let path = PathBuf::new();
    /// let mut store = KvStore::open(path)?;
    /// store.get("foo".to_owned())?.unwrap_or_else(|| "<unset>".to_owned());
    /// # Ok(())
    /// # }
    /// ```
    fn get(&mut self, key: String) -> Result<Option<String>> {
        let entry = match self.index.get(&key) {
            Some(entry) => entry,
            None => return Ok(None),
        };

        let reader = self.readers.get_mut(&entry.log_index).expect("Missing reader");
        Ok(Some(reader.read_value(&entry.offset)?))
    }

    /// Set a key to a value in a store.
    ///
    /// ```
    /// # use std::path::PathBuf;
    /// # use kvs::{KvsEngine, KvStore, Result};
    /// # fn check() -> Result<()> {
    /// # let path = PathBuf::new();
    /// let mut store = KvStore::open(path)?;
    /// store.set("foo".to_owned(), "bar".to_owned())?;
    /// # Ok(())
    /// # }
    /// ```
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = Command::Set {
            key: key.clone(),
            value: value.clone(),
        };

        let (offset, length) = self.writer.write(&command)?;
        let new_entry = IndexEntry {
            log_index: self.log_index,
            offset,
            length,
        };
        if let Some(old_entry) = self.index.insert(key, new_entry) {
            self.uncompacted += old_entry.length;
        }

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }

        Ok(())
    }

    /// Remove a key (and its value) from a store.
    ///
    /// ```
    /// # use std::path::PathBuf;
    /// # use kvs::{KvsEngine, KvStore, Result};
    /// # fn check() -> Result<()> {
    /// # let path = PathBuf::new();
    /// let mut store = KvStore::open(path)?;
    /// store.remove("foo".to_owned())?;
    /// # Ok(())
    /// # }
    /// ```
    fn remove(&mut self, key: String) -> Result<()> {
        if !self.index.contains_key(&key) {
            return Err(Error::KeyNotFound);
        }

        let command = Command::Remove { key: key.clone() };
        self.writer.write(&command)?;
        let old_entry = self.index.remove(&key).expect("Key not found after check");
        self.uncompacted += old_entry.length;
        Ok(())
    }
}

fn open_writer<P: AsRef<Path>>(path: P, log_index: u64) -> Result<Writer> {
    Writer::init(OpenOptions::new().create(true).write(true).open(log_path(path, log_index))?)
}

fn open_reader<P: AsRef<Path>>(path: P, log_index: u64) -> Result<Reader> {
    Ok(Reader::new(File::open(log_path(path, log_index))?))
}

fn find_log_indices<P: AsRef<Path>>(path: P) -> Result<Vec<u64>> {
    let mut log_indices: Vec<_> = fs::read_dir(&path)?
        .flat_map(|entry| -> Result<_> { Ok(entry?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| path.file_stem().and_then(|s| s.to_str()).map(str::parse))
        .flatten()
        .collect();
    log_indices.sort_unstable();
    Ok(log_indices)
}

fn open_entry(
    log_index: u64,
    index: &mut HashMap<String, IndexEntry>,
    (command, offset, length): (Command, Offset, u64),
) -> u64 {
    match command {
        Command::Set { key, .. } => {
            let new_entry = IndexEntry {
                log_index,
                offset,
                length,
            };
            if let Some(old_entry) = index.get_mut(&key) {
                mem::replace(old_entry, new_entry);
                old_entry.length
            } else {
                index.insert(key, new_entry);
                0
            }
        },
        Command::Remove { key } => {
            length + index.remove(&key).map(|e| e.length).unwrap_or(0)
        }
    }
}

fn log_path<P: AsRef<Path>>(dir: P, index: u64) -> PathBuf {
    dir.as_ref().join(format!("{}.log", index))
}
