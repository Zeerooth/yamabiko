use std::collections::HashMap;

use criterion::{criterion_group, criterion_main, Criterion};
use yamabiko::{test::create_db, OperationTarget};

fn bench_sets(bench: &mut Criterion) {
    bench.bench_function("sets on empty db", |b| {
        let (db, _td) = create_db();
        let mut i = 0;
        b.iter(|| {
            db.set(
                format!("key-{}", i).as_str(),
                yamabiko::test::SampleDbStruct::new(String::from("test value")),
                OperationTarget::Main,
            );
            i += 1;
        })
    });
    bench.bench_function("sets on empty db with an index", |b| {
        let (db, _td) = create_db();
        db.add_index(
            "str_val",
            yamabiko::index::IndexType::Sequential,
            OperationTarget::Main,
        );
        let mut i = 0;
        b.iter(|| {
            db.set(
                format!("key-{}", i).as_str(),
                yamabiko::test::SampleDbStruct::new(String::from("test value")),
                OperationTarget::Main,
            );
            i += 1;
        })
    });
    bench.bench_function("sets on larger database", |b| {
        let (db, _td) = create_db();
        const INIT_DB_SIZE: usize = 5_000;
        let hm: [usize; INIT_DB_SIZE] = core::array::from_fn(|i| i + 1);
        let hm2 = hm
            .iter()
            .map(|x| (format!("key-{}", x), "some value".as_bytes()));
        db.set_batch(hm2, OperationTarget::Main);
        let mut i = INIT_DB_SIZE;
        b.iter(|| {
            db.set(
                format!("key-{}", i).as_str(),
                yamabiko::test::SampleDbStruct::new(String::from("test value")),
                OperationTarget::Main,
            );
            i += 1;
        })
    });
    bench.bench_function("sets on larger database with an index", |b| {
        let (db, _td) = create_db();
        const INIT_DB_SIZE: usize = 5_000;
        let hm: [usize; INIT_DB_SIZE] = core::array::from_fn(|i| i + 1);
        db.add_index(
            "str_val",
            yamabiko::index::IndexType::Sequential,
            OperationTarget::Main,
        );
        let hm2 = hm
            .iter()
            .map(|x| (format!("key-{}", x), "some value".as_bytes()));
        db.set_batch(hm2, OperationTarget::Main);
        let mut i = INIT_DB_SIZE;
        b.iter(|| {
            db.set(
                format!("key-{}", i).as_str(),
                yamabiko::test::SampleDbStruct::new(String::from("test value")),
                OperationTarget::Main,
            );
            i += 1;
        })
    });
    bench.bench_function("batch set", |b| {
        let (db, _td) = create_db();
        let mut i = 0;
        b.iter(|| {
            let mut hm = HashMap::with_capacity(100);
            for x in 0..100 {
                hm.insert(
                    format!("key-{}", x + i),
                    yamabiko::test::SampleDbStruct::new(String::from("test value")),
                );
            }
            db.set_batch(hm, OperationTarget::Main);
            i += 100;
        });
    });
}

fn bench_sets_and_gets(bench: &mut Criterion) {
    bench.bench_function("gets on empty db", |b| {
        let (db, _td) = create_db();
        let mut i = 0;
        b.iter(|| {
            db.set(
                format!("key-{}", i).as_str(),
                yamabiko::test::SampleDbStruct::new(String::from("test value")),
                OperationTarget::Main,
            );
            db.get::<yamabiko::test::SampleDbStruct>(
                format!("key-{}", i).as_str(),
                OperationTarget::Main,
            )
            .unwrap();
            i += 1;
        })
    });
    bench.bench_function("gets on larger database", |b| {
        let (db, _td) = create_db();
        const INIT_DB_SIZE: usize = 5_000;
        let hm: [usize; INIT_DB_SIZE] = core::array::from_fn(|i| i + 1);
        let hm2 = hm
            .iter()
            .map(|x| (format!("key-{}", x), "some value".as_bytes()));
        db.set_batch(hm2, OperationTarget::Main);
        let mut i = INIT_DB_SIZE;
        b.iter(|| {
            db.set(
                format!("key-{}", i).as_str(),
                yamabiko::test::SampleDbStruct::new(String::from("test value")),
                OperationTarget::Main,
            );
            db.get::<yamabiko::test::SampleDbStruct>(
                format!("key-{}", i).as_str(),
                OperationTarget::Main,
            )
            .unwrap();
            i += 1;
        })
    });
}

criterion_group! {
name = benches;
config = Criterion::default().sample_size(20);
targets = bench_sets, bench_sets_and_gets}
criterion_main!(benches);
