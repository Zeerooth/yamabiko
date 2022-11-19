use super::*;
use tempfile::TempDir;

pub fn create_db<'a>() -> (Collection, TempDir) {
    let tmpdir = tempfile::tempdir().unwrap();
    (Collection::create(tmpdir.path()).unwrap(), tmpdir)
}
