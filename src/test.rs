use super::*;
use tempfile::TempDir;

pub fn create_db() -> (Database, TempDir) {
    let tmpdir = TempDir::new().unwrap();
    (Database::create(tmpdir.path()), tmpdir)
}
