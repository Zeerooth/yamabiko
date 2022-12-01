#[derive(PartialEq)]
pub enum IndexType {
    Single,
}

pub struct Index {
    name: String,
    kind: IndexType,
}

impl Index {
    pub fn new(name: &str, kind: IndexType) -> Self {
        Self {
            name: name.to_string(),
            kind,
        }
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }
}
