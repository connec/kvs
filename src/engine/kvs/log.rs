use rmp_serde::decode::{Error::InvalidMarkerRead, from_read as read_mp};
use rmp_serde::encode::write as write_mp;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{self, Seek, SeekFrom};

use crate::error::Result;

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


/// A marker struct indicating that the contained value is a valid log offset.
#[derive(Debug)]
pub struct Offset(u64);

impl std::ops::Deref for Offset {
  type Target = u64;
  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

impl std::convert::From<u64> for Offset {
  fn from(offset: u64) -> Self {
    Offset(offset + VALUE_OFFSET)
  }
}

#[derive(Debug)]
pub struct Reader {
  file: File,
}

impl Reader {
  pub fn new(file: File) -> Reader {
    Reader { file }
  }

  pub fn read_value(&mut self, offset: &Offset) -> Result<String> {
    self.file.seek(SeekFrom::Start(**offset))?;
    Ok(read_mp(&mut self.file)?)
  }

  pub fn load(&mut self) -> Result<ReaderIterator<&mut File>> {
    Ok(ReaderIterator::init(&mut self.file)?)
  }
}

pub struct ReaderIterator<R: io::Read + Seek> {
  reader: R,
  offset: u64,
}

impl<R: io::Read + Seek> ReaderIterator<R> {
  fn init(mut reader: R) -> Result<Self> {
    reader.seek(SeekFrom::Start(0))?;
    Ok(ReaderIterator { reader, offset: 0 })
  }
}

impl<R: io::Read + Seek> Iterator for ReaderIterator<R> {
  type Item = Result<(Command, Offset, u64)>;

  fn next(&mut self) -> Option<Self::Item> {
    let offset = self.offset;
    match read_mp(&mut *self) {
      Ok(command) => {
        Some(Ok((command, offset.into(), self.offset - offset)))
      },
      Err(InvalidMarkerRead(_)) => None,
      Err(err) => Some(Err(err.into())),
    }
  }
}

impl<R: io::Read + Seek> io::Read for ReaderIterator<R> {
  /// Wrap `reader`'s `read`, but also update `offset`.
  fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
    let result = self.reader.read(buf)?;
    self.offset += result as u64;
    Ok(result)
  }
}

/// A Write + Seek implementor that tracks its offset.
pub struct Writer {
    file: File,
    offset: u64,
}

impl Writer {
    pub fn init(mut file: File) -> Result<Writer> {
        let offset = file.seek(SeekFrom::End(0))?;
        Ok(Writer { file, offset })
    }

    pub fn write(&mut self, command: &Command) -> Result<(Offset, u64)> {
        let offset = self.offset;
        write_mp(self, command)?;
        let length = self.offset - offset;
        Ok((offset.into(), length))
    }
}

impl io::Write for Writer {
    /// Wrap `file`'s `write`, but also update the offset.
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let result = self.file.write(buf)?;
        self.offset += result as u64;
        Ok(result)
    }

    /// Wrap `file`'s `flush`.
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl io::Seek for Writer {
    /// Wrap `file`'s `seek`, but also update the offset.
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.offset = self.file.seek(pos)?;
        Ok(self.offset)
    }
}
