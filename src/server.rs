use rmp_serde::decode::from_read as read_mp;
use rmp_serde::encode::write as write_mp;
use slog::{debug, info, o, warn};
use std::convert::TryFrom;
use std::net::{TcpListener, ToSocketAddrs, TcpStream};

use crate::engine::Engine;
use crate::error::Result;
use crate::protocol::{Request, Response};

/// Implements a key-value server with a swappable storage engine.
pub struct Server<E> {
    log: slog::Logger,
    engine: E,
    listener: TcpListener,
}

impl<E: Engine> Server<E> {
    /// Start the server.
    pub fn start<A: ToSocketAddrs>(log: slog::Logger, engine: E, address: A) -> Result<Self> {
        info!(log, "Starting server");
        Ok(Server {
            log,
            engine,
            listener: TcpListener::bind(address)?,
        })
    }

    /// Run the server, accepting connections forever.
    pub fn run(&mut self) -> ! {
        loop {
            let connection = self.listener.accept();
            match connection {
                Ok((stream, peer_addr)) => {
                    let log = self.log.new(o!("peer_addr" => peer_addr));
                    if let Err(error) = self.handle_stream(log, stream) {
                        warn!(self.log, "Connection error: {}", error; "peer_addr" => peer_addr);
                    }
                },
                Err(error) => {
                    warn!(self.log, "Failed connection due to: {}", error);
                }
            }
        }
    }

    fn handle_stream(&mut self, log: slog::Logger, mut stream: TcpStream) -> Result<()> {
        debug!(log, "Client connected");

        let request = match read_mp(&stream) {
            Ok(request) => request,
            Err(error) => {
                warn!(log, "Invalid request: {}", error);
                let response: Response = error.into();
                write_mp(&mut stream, &response)?;
                return Ok(())
            }
        };

        match self.handle_request(request) {
            Ok(response) => write_mp(&mut stream, &response)?,
            Err(error) => write_mp(&mut stream, &Response::try_from(error)?)?,
        };

        debug!(log, "Closing connection");

        Ok(())
    }

    fn handle_request(&mut self, request: Request) -> Result<Response> {
        match request {
            Request::Get { key } => {
                Ok(self.engine.get(key)?
                    .map(|value| Response::Found { value })
                    .unwrap_or(Response::NotFound))
            },
            Request::Set { key, value } => {
                self.engine.set(key, value)?;
                Ok(Response::Ok)
            },
            Request::Remove { key } => {
                self.engine.remove(key)?;
                Ok(Response::Ok)
            }
        }
    }
}
