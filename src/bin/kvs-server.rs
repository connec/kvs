#[macro_use]
extern crate clap;
#[macro_use]
extern crate slog;

use clap::Arg;
use slog::Drain;
use std::env;
use std::process;

use kvs::{DEFAULT_ADDRESS, KvsEngine, Result, Server, Sled, Store};

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

    info!(root, "Starting engine";
        "version" => crate_version!(),
        "engine" => engine,
        "path" => path.to_str());

    match engine {
        "kvs" => {
            let mut server = make_server(root, Store::open(path)?, address)?;
            server.run()
        },
        "sled" => {
            let mut server = make_server(root, Sled::open(path)?, address)?;
            server.run()
        },
        _ => panic!("Invalid engine: {}", engine),
    }
}

fn make_server<E: KvsEngine>(root: slog::Logger, engine: E, address: &str) -> Result<Server<E>> {
    Server::start(
        root.new(o!("address" => address.to_string())),
        engine,
        address
    )
}
