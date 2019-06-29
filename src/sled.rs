use sled::Db;
use std::fs::{self, OpenOptions};
use std::path::Path;

use crate::engine::Engine;
use crate::error::{Error, Result};

const ENGINE_ID: &[u8] = b"sled";

/// Sled.
pub struct Sled {
    db: sled::Db,
}

impl Sled {
    /// Open.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        fs::create_dir_all(&path)?;

        check_engine(&path)?;

        Ok(Sled {
            db: Db::start_default(path)?
        })
    }
}

impl Engine for Sled {
    fn get(&mut self, key: String) -> Result<Option<String>> {
        Ok(self.db.get(key)?.map(|ivec| String::from_utf8_lossy(ivec.as_ref()).into_owned()))
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.db.set(key, value.as_bytes())?;
        self.db.flush()?;
        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if self.db.del(key)? == None {
            return Err(Error::KeyNotFound);
        }
        self.db.flush()?;
        Ok(())
    }
}

fn check_engine<P: AsRef<Path>>(path: P) -> Result<()> {
    use std::io::{Read, Write};

    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path.as_ref().join(".engine"))?;
    let mut contents = vec![];
    file.read_to_end(&mut contents)?;
    if contents.is_empty() {
        file.write(&ENGINE_ID)?;
        return Ok(())
    }
    if contents != ENGINE_ID {
        return Err(Error::WrongEngine);
    }
    Ok(())
}
