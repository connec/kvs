use sled::Tree;

use crate::engine::Engine;
use crate::error::{Error, Result};

pub use sled::Db;

impl Engine for Db {
    fn get(&mut self, key: String) -> Result<Option<String>> {
        Ok(Tree::get(self, key)?.map(|ivec| String::from_utf8_lossy(ivec.as_ref()).into_owned()))
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        Tree::set(self, key, value.as_bytes())?;
        self.flush()?;
        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if Tree::del(self, key)? == None {
            return Err(Error::KeyNotFound);
        }
        self.flush()?;
        Ok(())
    }
}
