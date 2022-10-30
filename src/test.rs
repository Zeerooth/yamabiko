use super::*;
use tempfile::TempDir;

pub fn create_db() -> (Database, TempDir) {
    let tmpdir = tempfile::tempdir_in("/tmp").unwrap();
    (Database::create(tmpdir.path()), tmpdir)
}
