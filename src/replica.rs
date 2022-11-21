use git2::PushOptions;

pub enum ReplicationMethod {
    All,
    Partial(usize),
    Random(f64),
}

pub struct Replica<'a> {
    pub remote: String,
    pub replication_method: ReplicationMethod,
    pub push_options: Option<PushOptions<'a>>,
}
