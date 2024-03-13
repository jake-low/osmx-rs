use std::error::Error;

use clap::{Parser, Subcommand};

mod builders;
mod expand;
mod sorter;
mod stat;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct CliArgs {
    #[command(subcommand)]
    subcommand: Command,
}

#[derive(Subcommand)]
enum Command {
    Expand(expand::CliArgs),
    Stat(stat::CliArgs),
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = CliArgs::parse();
    match args.subcommand {
        Command::Stat(args) => stat::run(&args)?,
        Command::Expand(args) => expand::run(&args)?,
    };

    Ok(())
}
