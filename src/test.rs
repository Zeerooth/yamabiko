use crate::serialization::DataFormat;

use super::*;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct SampleDbStruct {
    pub str_val: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct InterigentDbStruct {
    pub num_val: i64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct FloatyDbStruct {
    pub num_val: f64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct ComplexDbStruct {
    pub str_val: String,
    pub usize_val: usize,
    pub float_val: f64,
}

impl SampleDbStruct {
    pub fn new(str_val: String) -> Self {
        Self { str_val }
    }
}

impl ComplexDbStruct {
    pub fn new(str_val: String, usize_val: usize, float_val: f64) -> Self {
        Self {
            str_val,
            usize_val,
            float_val,
        }
    }
}

pub fn create_db<'a>() -> (Collection<'a>, TempDir) {
    let tmpdir = tempfile::tempdir().unwrap();
    (
        Collection::create(tmpdir.path(), DataFormat::Json).unwrap(),
        tmpdir,
    )
}
