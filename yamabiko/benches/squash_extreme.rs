use std::path::Path;

use criterion::{criterion_group, criterion_main, Criterion};
use git2::Oid;
use yamabiko::{serialization::DataFormat, squash::Squasher, test::create_db, OperationTarget};

fn bench_squash_extreme(bench: &mut Criterion) {
    bench.bench_function("squash 10k commits in repo with 100k", |b| {
        b.iter_with_setup(
            || {
                let (db, td) = create_db(DataFormat::Json);
                let squasher = Squasher::initialize(Path::new(td.path())).unwrap();
                let mut commit_n1k = Oid::zero();
                for i in 0..100_000 {
                    db.set(
                        format!("key-{}", i).as_str(),
                        yamabiko::test::SampleDbStruct::new(String::from("test value")),
                        OperationTarget::Main,
                    )
                    .unwrap();
                    if i == 10000 {
                        commit_n1k = db
                            .repository()
                            .head()
                            .unwrap()
                            .peel_to_commit()
                            .unwrap()
                            .id();
                    }
                }
                (db, squasher, commit_n1k, td)
            },
            |(_db, squasher, commit, _td)| {
                squasher.squash_before_commit(commit).unwrap();
            },
        )
    });
}

criterion_group! {
name = benches;
config = Criterion::default().sample_size(10);
targets = bench_squash_extreme}
criterion_main!(benches);
