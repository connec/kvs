#[macro_use]
extern crate clap;

use clap::{Arg, SubCommand};

fn main() {
    app_from_crate!()
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

    panic!("unimplemented")
}
