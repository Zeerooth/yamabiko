use std::path::Path;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use git2::Oid;
use yamabiko::{serialization::DataFormat, squash::Squasher, test::create_db, OperationTarget};

fn bench_squash(bench: &mut Criterion) {
    bench.bench_function("squash with 2 commits", |b| {
        b.iter_batched(
            || {
                let (db, td) = create_db(DataFormat::Json);
                db.set(
                    "key-1",
                    yamabiko::test::SampleDbStruct::new(String::from("test value")),
                    OperationTarget::Main,
                )
                .unwrap();
                db.set(
                    "key-2",
                    yamabiko::test::SampleDbStruct::new(String::from("test value")),
                    OperationTarget::Main,
                )
                .unwrap();
                let squasher = Squasher::initialize(Path::new(td.path())).unwrap();
                let head = db
                    .repository()
                    .head()
                    .unwrap()
                    .peel_to_commit()
                    .unwrap()
                    .id();
                (db, squasher, head, td)
            },
            |(_db, squasher, commit, _td)| {
                squasher.squash_before_commit(commit).unwrap();
            },
            BatchSize::SmallInput,
        )
    });
    bench.bench_function("squash with 1k commits", |b| {
        b.iter_with_setup(
            || {
                let (db, td) = create_db(DataFormat::Json);
                let squasher = Squasher::initialize(Path::new(td.path())).unwrap();
                for i in 0..1000 {
                    db.set(
                        format!("key-{}", i).as_str(),
                        yamabiko::test::SampleDbStruct::new(String::from("test value")),
                        OperationTarget::Main,
                    )
                    .unwrap();
                }
                let head = db
                    .repository()
                    .head()
                    .unwrap()
                    .peel_to_commit()
                    .unwrap()
                    .id();
                (db, squasher, head, td)
            },
            |(_db, squasher, commit, _td)| {
                squasher.squash_before_commit(commit).unwrap();
            },
        )
    });
    bench.bench_function("squash 5 commits in repo with 10k", |b| {
        b.iter_with_setup(
            || {
                let (db, td) = create_db(DataFormat::Json);
                let squasher = Squasher::initialize(Path::new(td.path())).unwrap();
                let mut commit_n1k = Oid::zero();
                for i in 0..10000 {
                    db.set(
                        format!("key-{}", i).as_str(),
                        yamabiko::test::SampleDbStruct::new(String::from("test value")),
                        OperationTarget::Main,
                    )
                    .unwrap();
                    if i == 5 {
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
    bench.bench_function("squash 1k commits in repo with 10k", |b| {
        b.iter_with_setup(
            || {
                let (db, td) = create_db(DataFormat::Json);
                let squasher = Squasher::initialize(Path::new(td.path())).unwrap();
                let mut commit_n1k = Oid::zero();
                for i in 0..10000 {
                    db.set(
                        format!("key-{}", i).as_str(),
                        yamabiko::test::SampleDbStruct::new(String::from("test value")),
                        OperationTarget::Main,
                    )
                    .unwrap();
                    if i == 1000 {
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
    bench.bench_function("squash 9k commits in repo with 10k", |b| {
        b.iter_with_setup(
            || {
                let (db, td) = create_db(DataFormat::Json);
                let squasher = Squasher::initialize(Path::new(td.path())).unwrap();
                let mut commit_n1k = Oid::zero();
                for i in 0..10000 {
                    db.set(
                        format!("key-{}", i).as_str(),
                        yamabiko::test::SampleDbStruct::new(String::from("test value")),
                        OperationTarget::Main,
                    )
                    .unwrap();
                    if i == 9000 {
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
config = Criterion::default().sample_size(20);
targets = bench_squash}
criterion_main!(benches);
