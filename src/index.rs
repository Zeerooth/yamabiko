use std::{fmt::Display, path::Path};

use git2::{Index as GitIndex, IndexEntry, IndexTime, Oid, Repository};
use parking_lot::MutexGuard;

#[derive(PartialEq, Eq, Hash, Debug)]
pub enum IndexType {
    Single,
}

impl IndexType {
    pub fn from_name(name: &str) -> Result<Self, ()> {
        match name {
            "single" => Ok(Self::Single),
            _ => Err(()),
        }
    }
}

impl Display for IndexType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                IndexType::Single => "single",
            }
        )
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Index {
    name: String,
    indexed_field: String,
    kind: IndexType,
}

impl Index {
    pub fn new(name: &str, indexed_field: &str, kind: IndexType) -> Self {
        Self {
            name: name.to_string(),
            indexed_field: indexed_field.to_string(),
            kind,
        }
    }

    pub fn from_name(name: &str) -> Result<Self, ()> {
        let token_list = name.rsplit_once(".").unwrap().0.rsplit_once("#");
        if let Some(tokens) = token_list {
            return Ok(Self::new(name, tokens.0, IndexType::from_name(tokens.1)?));
        }
        Err(())
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn indexed_field(&self) -> &str {
        self.indexed_field.as_str()
    }

    pub fn create_entry<'a>(&self, repo: &'a MutexGuard<Repository>, oid: Oid, value: &str) {
        let mut git_index = self.git_index(repo);
        let last_entry = git_index.find_prefix(&value).unwrap();
        let next_value = match last_entry {
            Some(v) => {
                let path = git_index.get(v).unwrap().path;
                let num = u64::from_str_radix(
                    &String::from_utf8(path.split_at(path.len() - 16).1.to_vec()).unwrap(),
                    16,
                )
                .unwrap();
                num - 1
            }
            None => u64::MAX,
        };
        let path = format!("{}/{:16x}", value, next_value);
        let entry = IndexEntry {
            ctime: IndexTime::new(0, 0),
            mtime: IndexTime::new(0, 0),
            dev: 0,
            ino: 0,
            mode: 0o100644,
            uid: 0,
            gid: 0,
            file_size: 0,
            id: oid,
            flags: 0,
            flags_extended: 0,
            path: path.as_bytes().to_vec(),
        };
        git_index.add(&entry).unwrap();
        git_index.write().unwrap();
    }

    pub fn git_index<'a>(&self, repo: &'a MutexGuard<Repository>) -> GitIndex {
        GitIndex::open(
            Path::new(repo.path())
                .join(".index")
                .join(self.name())
                .as_path(),
        )
        .unwrap()
    }
}
