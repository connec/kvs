#[macro_use]
extern crate clap;

use clap::{AppSettings, Arg, SubCommand};
use std::process;

use kvs::{DEFAULT_ADDRESS, Client, Result};

fn run() -> Result<()> {
    let matches = app_from_crate!()
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .setting(AppSettings::VersionlessSubcommands)
        .subcommand(
            SubCommand::with_name("get")
                .about("Get the value of a given key")
                .arg(Arg::with_name("key").required(true))
                .arg(Arg::with_name("address").long("addr").takes_value(true)),
        )
        .subcommand(
            SubCommand::with_name("set")
                .about("Set the value of a given key to a given value")
                .arg(Arg::with_name("key").required(true))
                .arg(Arg::with_name("value").required(true))
                .arg(Arg::with_name("address").long("addr").takes_value(true)),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("Remove a given key")
                .arg(Arg::with_name("key").required(true))
                .arg(Arg::with_name("address").long("addr").takes_value(true)),
        )
        .get_matches();

    match matches.subcommand() {
        ("get", Some(args)) => {
            let key = args
                .value_of("key")
                .expect("Missing value for required arg: key");
            let address = args.value_of("address").unwrap_or(DEFAULT_ADDRESS);

            let mut client = Client::connect(address)?;
            match client.get(key.to_owned())? {
                Some(value) => println!("{}", value),
                None => println!("Key not found"),
            }
        }
        ("set", Some(args)) => {
            let key = args
                .value_of("key")
                .expect("Missing value for required arg: key");
            let value = args
                .value_of("value")
                .expect("Missing value for required arg: value");
            let address = args.value_of("address").unwrap_or(DEFAULT_ADDRESS);

            let mut client = Client::connect(address)?;
            client.set(key.to_owned(), value.to_owned())?;
        }
        ("rm", Some(args)) => {
            let key = args
                .value_of("key")
                .expect("Missing value for required arg: key");
            let address = args.value_of("address").unwrap_or(DEFAULT_ADDRESS);

            let mut client = Client::connect(address)?;
            client.remove(key.to_owned())?;
        }
        _ => unreachable!(),
    }

    Ok(())
}

fn main() {
    if let Err(err) = run() {
        use std::error::Error;

        eprintln!("Error: {}", err);

        let mut source: &dyn Error = &err;
        while let Some(err) = source.source() {
            eprintln!("Caused by: {}", err);
            source = err;
        }
        process::exit(1);
    }
}
