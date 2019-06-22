#[macro_use]
extern crate clap;

use clap::{AppSettings, Arg, SubCommand};
use std::env;
use std::process;

use kvs::{Error, KvStore, Result};

fn run() -> Result<()> {
    let matches = app_from_crate!()
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .setting(AppSettings::VersionlessSubcommands)
        .subcommand(
            SubCommand::with_name("get")
                .about("Get the value of a given key")
                .arg(Arg::with_name("key").required(true)),
        )
        .subcommand(
            SubCommand::with_name("set")
                .about("Set the value of a given key to a given value")
                .arg(Arg::with_name("key").required(true))
                .arg(Arg::with_name("value").required(true)),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("Remove a given key")
                .arg(Arg::with_name("key").required(true)),
        )
        .get_matches();

    match matches.subcommand() {
        ("get", Some(args)) => {
            let key = args
                .value_of("key")
                .expect("Missing value for required arg: key");

            let mut kvs = KvStore::open(env::current_dir()?)?;
            match kvs.get(key.to_owned())? {
                Some(value) => println!("{}", value),
                None => println!("Key not found"),
            };
        }
        ("set", Some(args)) => {
            let key = args
                .value_of("key")
                .expect("Missing value for required arg: key");
            let value = args
                .value_of("value")
                .expect("Missing value for required arg: value");

            let mut kvs = KvStore::open(env::current_dir()?)?;
            kvs.set(key.to_owned(), value.to_owned())?;
        }
        ("rm", Some(args)) => {
            let key = args
                .value_of("key")
                .expect("Missing value for required arg: key");

            let mut kvs = KvStore::open(env::current_dir()?)?;
            kvs.remove(key.to_owned()).or_else(|err| match err {
                Error::KeyNotFound => {
                    println!("{}", err);
                    process::exit(1);
                }
                err => Err(err),
            })?;
        }
        _ => unreachable!(),
    }

    Ok(())
}

fn main() {
    if let Err(err) = run() {
        eprintln!("Error: {}", err);
        process::exit(1);
    }
}
