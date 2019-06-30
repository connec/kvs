use rmp_serde::decode::from_read as read_mp;
use rmp_serde::encode::write as write_mp;
use std::net::{TcpStream, ToSocketAddrs};

use crate::error::{Error, Result};
use crate::protocol::{Request, Response};

/// Implements a client for a key-value server.
pub struct Client {
    stream: TcpStream,
}

impl Client {
    /// Connact to a server.
    pub fn connect<A: ToSocketAddrs>(address: A) -> Result<Client> {
        let stream = TcpStream::connect(address)?;
        Ok(Client { stream })
    }

    /// Get the value of a key.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let request = Request::Get { key };
        write_mp(&mut self.stream, &request)?;
        let response = read_mp(&self.stream)?;

        match response {
            Response::Found { value } => Ok(Some(value)),
            Response::NotFound => Ok(None),
            response => Err(Error::ProtocolError(request, response)),
        }
    }

    /// Set the value of a key.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let request = Request::Set { key, value };
        write_mp(&mut self.stream, &request)?;
        let response = read_mp(&self.stream)?;

        match response {
            Response::Ok => Ok(()),
            Response::NotFound => Err(Error::KeyNotFound),
            response => Err(Error::ProtocolError(request, response)),
        }
    }

    /// Remove a key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        let request = Request::Remove { key };
        write_mp(&mut self.stream, &request)?;
        let response = read_mp(&self.stream)?;

        match response {
            Response::Ok => Ok(()),
            Response::NotFound => Err(Error::KeyNotFound),
            response => Err(Error::ProtocolError(request, response)),
        }
    }
}
