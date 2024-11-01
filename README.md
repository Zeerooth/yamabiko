# Yamabiko

Embedded database based on git

## Desclaimer

*Do not use it for storing important data! yamabiko works for my use case, but it's not thoroughly tested yet.*

## Features

- [x] Get, set and commit serializable data to a local git repo using Key-Value
- [x] Optional long-living transactions (under separate branches)
- [x] Manage indexes for faster queries
- [x] Replicate data to remote repositories (backup)
- [x] Keep the entire history of changes and easily revert back

## Quick showcase

```rust

// serde is used for de/serialization
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct LogStruct {
    pub addr: String,
    pub timestamp: u64,
    pub message: String,
}

// async is optional, but we'll need it if we want to await for replicas to finish syncing
#[tokio::main]
async fn main() {
    // Load or create a repository for storing our data
    // You have to stick to one data format for a single repo,
    // but the exact contents of each record are yours to determine and handle
    // You can mix different structs for de/serialization,
    // but it's a good idea to have a separate collection for each type
    let repo_path = Path::new("/tmp/repo");
    let mut db = Collection::load_or_create(repo_path, DataFormat::Json).unwrap();

    // Setting credentials is only necessary if you plan to replicate data to a remote repo as a backup
    let credentials = RemoteCredentials {
        username: None,
        publickey: None,
        privatekey: std::path::Path::new(&format!("{}/.ssh/id_rsa", env::var("HOME").unwrap())).to_path_buf(),
        passphrase: None,
    };

    // In this example we spawn the replicator in the same thread, so it's going to be blocking.
    // The proper way to replicate data to remotes is to have a replicator per remote per collection
    // running inside a thread or a tokio task. However, this is out of scope for this simple demo. 
    let repl = Replicator::initialize(
        repo_path,
        "gh_backup",
        "git@github.com:torvalds/myepicrepo.git",
        // If you plan to have frequent data updates then ReplicationMethod::All is probably a bad idea
        // Syncing with remote repos is slow
        // And frequent requests are going to get you rate limited if you use an external service
        // Consider using ReplicationMethod::Random(0.05) - only ~5% of commits are going to result in a sync
        // Or ReplicationMethod::Periodic(300) - it'll sync at most every 5 minutes
        ReplicationMethod::All,
        Some(credentials),
    ).unwrap(); 

    println!("We have {} replicas loaded!", db.replicas().len());
 
    let to_save = LogStruct {
        addr: String::from("8.8.8.8"),
        timestamp: 9999999,
        message: String::from("GET /index.html")
    };

    // Choosing a good key is important. If you have the possibility to do so
    // Then it's much better to separate the key into subtrees in a logical way
    // For example: "AS/JP/Kyoto" is good key for storing cities or "2024/10/10/65823" for time based data
    // This makes queries, lookups and sets much faster and the collection becomes more organized
    // Don't worry though! If you need to store flat keys, like with usernames for example,
    // yamabiko will generate artificial subtrees based on hash and handle it internally
    let key = format!("{}/{}", addr, timestamp).as_str();

    // "set" will save the data as a blob and make a new commit
    // You can also use "set_batch" for updating many records at once
    // And long-living transactions to prevent the data from being commited to the main branch automatically
    db.set(key, to_save, yamabiko::OperationTarget::Main).unwrap();
    
    // Only necessary if you make use of replication
    // It's recommended to spawn replication tasks asynchronously to avoid blocking
    let sync_task = tokio::spawn(async move {
        repl.replicate().unwrap();
    }); 

    // QueryBuilder is not very powerful yet,
    // but it allows for making simple queries on data saved in the collection
    let query = QueryBuilder::new()
        .query(q("message", Equal, "GET /index.html") & q("timestamp", Less, 100000))
        .execute(&db);
    
    for res in query.results {
        // This is how you can deserialize the results found in the query
        let obj = db.get_by_oid::<LogStruct>(res).unwrap();
        println!("{:?}", obj);
    }

    // Lastly, by default queries are going to scan the entire repository,
    // deserialize the data and compare the fields to find the results
    // For larger collections and queries this is going to be !extremely! slow.
    // Make sure to create relevant indexes to make queries faster
    db.add_index("timestamp", IndexType::Numeric, OperationTarget::Main);

    // Let's join the replication task and see if it succeded.
    sync_task.await.expect("Failed replication");
}
```

## Examples & Tests

[benches directory contains many examples on how to use this library](./benches/)

