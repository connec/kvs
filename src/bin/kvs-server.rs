#[macro_use]
extern crate clap;
#[macro_use]
extern crate slog;

use clap::Arg;
use slog::Drain;
use std::env;
use std::fs;
use std::io::ErrorKind::NotFound;
use std::path::PathBuf;
use std::process;

use kvs::{DEFAULT_ADDRESS, Error, KvsEngine, KvStore, Result, Server, SledKvStore};

const VALID_ENGINES: &[&'static str] = &["kvs", "sled"];
const DEFAULT_ENGINE: &'static str = "kvs";

fn main() {
    if let Err(err) = run() {
        eprintln!("Error: {}", err);
        process::exit(1);
    }
}

fn run() -> Result<()> {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let root = slog::Logger::root(drain, o!());

    let matches = app_from_crate!()
        .arg(Arg::with_name("engine").long("engine").takes_value(true).possible_values(VALID_ENGINES))
        .arg(Arg::with_name("address").long("addr").takes_value(true))
        .get_matches();

    let engine = matches.value_of("engine").unwrap_or(DEFAULT_ENGINE);
    let path = env::current_dir()?;
    let address = matches.value_of("address").unwrap_or(DEFAULT_ADDRESS);

    check_engine(&path, engine)?;

    info!(root, "Starting engine";
        "version" => crate_version!(),
        "engine" => engine,
        "path" => path.to_str());

    match engine {
        "kvs" => {
            let mut server = make_server(root, address, KvStore::open(path)?)?;
            server.run()
        },
        "sled" => {
            let mut server = make_server(root, address, SledKvStore::start_default(path)?)?;
            server.run()
        },
        _ => panic!("Invalid engine: {}", engine),
    }
}

fn check_engine(path: &PathBuf, engine: &str) -> Result<()> {
    let path = path.join("engine");
    match fs::read_to_string(&path) {
        Ok(ref contents) if contents == engine  => Ok(()),
        Ok(_) => Err(Error::WrongEngine),
        Err(ref err) if err.kind() == NotFound => {
            fs::write(&path, engine)?;
            Ok(())
        },
        Err(err) => Err(err.into()),
    }
}

fn make_server<E: KvsEngine>(root: slog::Logger, address: &str, engine: E) -> Result<Server<E>> {
    Server::start(
        root.new(o!("address" => address.to_string())),
        engine,
        address
    )
}
