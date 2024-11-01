use std::path::Path;

use clap::{builder::TypedValueParser, Parser, Subcommand};
use yamabiko::{serialization::DataFormat, Collection, OperationTarget};

/// Command-line program to manage yamabiko collections
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to the repository to operate on
    #[arg(index(1))]
    repo: String,

    #[arg(short, long, default_value = "json")]
    format: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Get data under selected key
    Get { key: String },
    /// Operations on indexes
    Indexes {
        #[command(subcommand)]
        command: IndexCommand,
    },
    Revert {
        number: usize,
        target: String
    }
}

#[derive(Subcommand, Debug)]
enum IndexCommand {
    List,
    Add {
        #[arg(long)]
        field: String,
        #[arg(
        long, 
        value_parser = clap::builder::PossibleValuesParser::new(["numeric", "sequential", "collection"])
            .map(|s| s.parse::<yamabiko::index::IndexType>().unwrap()),
    )]
        kind: yamabiko::index::IndexType,
    }, 
}

fn main() {
    let args = Args::parse();
    let repo_path = Path::new(&args.repo);
    let collection =
        Collection::initialize(repo_path, DataFormat::Json).expect("Failed to load collection");
    match args.command {
        Command::Get { key } => {
            match collection
                .get_raw(&key, OperationTarget::Main)
                .expect("Failed to get data")
            {
                Some(data) => println!("{}", data),
                None => eprintln!("Not found"),
            }
        }
        Command::Indexes { command } => match command {
            IndexCommand::List => {
                for index in collection.index_list() {
                    println!("{:?}", index);
                }
            }
            IndexCommand::Add { field, kind } => {
                println!("{:?}", collection.add_index(&field, kind));
            },
        },
        Command::Revert { number , target} => {
            collection.revert_n_commits(number, OperationTarget::Transaction(&target)).unwrap();
            println!("Successfully reverted {} commits on {}", number, target);
        },
    }
}
