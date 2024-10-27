use std::path::PathBuf;

#[derive(Clone)]
pub enum ReplicationMethod {
    All,
    Partial(usize),
    Random(f64),
}

#[derive(Clone)]
pub struct Replica {
    pub remote: String,
    pub replication_method: ReplicationMethod,
    pub credentials: Option<RemoteCredentials>,
}

#[derive(Clone)]
pub struct RemoteCredentials {
    pub username: Option<String>,
    pub publickey: Option<PathBuf>,
    pub privatekey: PathBuf,
    pub passphrase: Option<String>,
}
