pub enum ReplicationMethod {
    All,
    Partial(usize),
    Random(f64),
}

pub struct Replica {
    pub remote: String,
    pub replication_method: ReplicationMethod,
}
