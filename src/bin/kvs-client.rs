#[macro_use]
extern crate clap;

use clap::{AppSettings, Arg, SubCommand};
use rmp_serde::decode::from_read as read_mp;
use rmp_serde::encode::write as write_mp;
use std::process;
use std::net::TcpStream;

use kvs::{DEFAULT_ADDRESS, Result};
use kvs::{Request, Response};

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

            let mut stream = TcpStream::connect(address)?;
            let request = Request::Get { key: key.to_owned() };
            write_mp(&mut stream, &request)?;
            let response = read_mp(&stream)?;
            drop(stream);

            match response {
                Response::Found { value } => println!("{}", value),
                Response::NotFound => println!("Key not found"),
                _ => {
                    eprintln!("Protocol error: server replied {:?} to {:?}", response, request);
                    process::exit(2);
                }
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

            let mut stream = TcpStream::connect(address)?;
            let request = Request::Set { key: key.to_owned(), value: value.to_owned() };
            write_mp(&mut stream, &request)?;
            let response = read_mp(&stream)?;
            drop(stream);

            match response {
                Response::Ok => {},
                Response::NotFound => {
                    eprintln!("Key not found");
                    process::exit(1);
                },
                _ => {
                    eprintln!("Protocol error: server replied {:?} to {:?}", response, request);
                    process::exit(2);
                }
            }
        }
        ("rm", Some(args)) => {
            let key = args
                .value_of("key")
                .expect("Missing value for required arg: key");
            let address = args.value_of("address").unwrap_or(DEFAULT_ADDRESS);

            let mut stream = TcpStream::connect(address)?;
            let request = Request::Remove { key: key.to_owned() };
            write_mp(&mut stream, &request)?;
            let response = read_mp(&stream)?;
            drop(stream);

            match response {
                Response::Ok => {},
                Response::NotFound => {
                    eprintln!("Key not found");
                    process::exit(1);
                },
                _ => {
                    eprintln!("Protocol error: server replied {:?} to {:?}", response, request);
                    process::exit(2);
                }
            }
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
