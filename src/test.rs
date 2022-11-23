use crate::serialization::DataFormat;

use super::*;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct SampleDbStruct {
    pub str_val: String,
}

impl SampleDbStruct {
    pub fn new(str_val: String) -> Self {
        Self { str_val }
    }
}

pub fn create_db<'a>() -> (Collection<'a>, TempDir) {
    let tmpdir = tempfile::tempdir().unwrap();
    (
        Collection::create(tmpdir.path(), DataFormat::Json).unwrap(),
        tmpdir,
    )
}
