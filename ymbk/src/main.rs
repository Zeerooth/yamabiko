use std::path::Path;

use clap::{builder::TypedValueParser, Parser, Subcommand};
use git2::Oid;
use yamabiko::{serialization::DataFormat, Collection, OperationTarget};

static ADDITIONAL_HELP_TEXT: &str = color_print::cstr!(
r#"<bold><underline>Examples:</underline></bold>
  [Output the value stored under the key in the specified collection]
  <bold>ymbk ./collection get key1</bold> 

  [Add a numeric index on the field 'number' in the specified collection]
  <bold>ymbk ./collection indexes add --field addr --kind numeric</bold>"#);

/// Command-line program to manage yamabiko collections
#[derive(Parser, Debug)]
#[command(version, about, long_about = None, after_help = ADDITIONAL_HELP_TEXT)]
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
    RevertNCommits {
        number: usize,
        #[clap(long, short, default_value = "main")]
        target: String,
        #[clap(long, action)]
        keep_history: bool
    },
    RevertToCommit {
        commit: String, 
        #[clap(long, action)]
        keep_history: bool
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
        Command::RevertNCommits { number , target, keep_history} => {
            collection.revert_n_commits(number, OperationTarget::Transaction(&target), keep_history).unwrap();
            println!("Successfully reverted {} commits on {}", number, target);
        },
        Command::RevertToCommit { commit , keep_history} => {
            let oid = Oid::from_str(&commit);
            match oid {
                Ok(oid) => {
                    collection.revert_main_to_commit(oid,  keep_history).unwrap();
                    println!("Successfully reverted to commit {} on main", commit);
                }
                Err(_err) => eprintln!("Invalid commit Oid format")
            }
        },
    }
}
