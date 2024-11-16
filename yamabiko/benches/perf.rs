use std::collections::HashMap;

use criterion::{criterion_group, criterion_main, Criterion};
use yamabiko::{serialization::DataFormat, test::create_db, OperationTarget};

fn bench_sets(bench: &mut Criterion) {
    for data_format in [DataFormat::Json, DataFormat::Yaml, DataFormat::Pot] {
        bench.bench_function(
            format!("sets on empty db ({})", data_format).as_str(),
            |b| {
                let (db, _td) = create_db(data_format);
                let mut i = 0;
                b.iter(|| {
                    db.set(
                        format!("key-{}", i).as_str(),
                        yamabiko::test::SampleDbStruct::new(String::from("test value")),
                        OperationTarget::Main,
                    )
                    .unwrap();
                    i += 1;
                })
            },
        );
        bench.bench_function(
            format!("sets on empty db with an index ({})", data_format).as_str(),
            |b| {
                let (db, _td) = create_db(data_format);
                db.add_index("str_val", yamabiko::index::IndexType::Sequential);
                let mut i = 0;
                b.iter(|| {
                    db.set(
                        format!("key-{}", i).as_str(),
                        yamabiko::test::SampleDbStruct::new(String::from("test value")),
                        OperationTarget::Main,
                    )
                    .unwrap();
                    i += 1;
                })
            },
        );
        bench.bench_function(
            format!("sets on larger database ({})", data_format).as_str(),
            |b| {
                let (db, _td) = create_db(data_format);
                const INIT_DB_SIZE: usize = 5_000;
                let hm: [usize; INIT_DB_SIZE] = core::array::from_fn(|i| i + 1);
                let hm2 = hm
                    .iter()
                    .map(|x| (format!("key-{}", x), "some value".as_bytes()));
                db.set_batch(hm2, OperationTarget::Main).unwrap();
                let mut i = INIT_DB_SIZE;
                b.iter(|| {
                    db.set(
                        format!("key-{}", i).as_str(),
                        yamabiko::test::SampleDbStruct::new(String::from("test value")),
                        OperationTarget::Main,
                    )
                    .unwrap();
                    i += 1;
                })
            },
        );
        bench.bench_function(
            format!("sets on larger database with an index ({})", data_format).as_str(),
            |b| {
                let (db, _td) = create_db(data_format);
                const INIT_DB_SIZE: usize = 5_000;
                let hm: [usize; INIT_DB_SIZE] = core::array::from_fn(|i| i + 1);
                db.add_index("str_val", yamabiko::index::IndexType::Sequential);
                let hm2 = hm
                    .iter()
                    .map(|x| (format!("key-{}", x), "some value".as_bytes()));
                db.set_batch(hm2, OperationTarget::Main).unwrap();
                let mut i = INIT_DB_SIZE;
                b.iter(|| {
                    db.set(
                        format!("key-{}", i).as_str(),
                        yamabiko::test::SampleDbStruct::new(String::from("test value")),
                        OperationTarget::Main,
                    )
                    .unwrap();
                    i += 1;
                })
            },
        );
        bench.bench_function(format!("batch set ({})", data_format).as_str(), |b| {
            let (db, _td) = create_db(data_format);
            let mut i = 0;
            b.iter(|| {
                let mut hm = HashMap::with_capacity(100);
                for x in 0..100 {
                    hm.insert(
                        format!("key-{}", x + i),
                        yamabiko::test::SampleDbStruct::new(String::from("test value")),
                    );
                }
                db.set_batch(hm, OperationTarget::Main).unwrap();
                i += 100;
            });
        });
    }
}

fn bench_sets_and_gets(bench: &mut Criterion) {
    for data_format in [DataFormat::Json, DataFormat::Yaml, DataFormat::Pot] {
        bench.bench_function(
            format!("gets on empty db ({})", data_format).as_str(),
            |b| {
                let (db, _td) = create_db(data_format);
                let mut i = 0;
                b.iter(|| {
                    db.set(
                        format!("key-{}", i).as_str(),
                        yamabiko::test::SampleDbStruct::new(String::from("test value")),
                        OperationTarget::Main,
                    )
                    .unwrap();
                    db.get::<yamabiko::test::SampleDbStruct>(
                        format!("key-{}", i).as_str(),
                        OperationTarget::Main,
                    )
                    .unwrap();
                    i += 1;
                })
            },
        );
        bench.bench_function(
            format!("gets on a larger database ({})", data_format).as_str(),
            |b| {
                let (db, _td) = create_db(data_format);
                const INIT_DB_SIZE: usize = 5_000;
                let hm: [usize; INIT_DB_SIZE] = core::array::from_fn(|i| i + 1);
                let hm2 = hm
                    .iter()
                    .map(|x| (format!("key-{}", x), "some value".as_bytes()));
                db.set_batch(hm2, OperationTarget::Main).unwrap();
                let mut i = INIT_DB_SIZE;
                b.iter(|| {
                    db.set(
                        format!("key-{}", i).as_str(),
                        yamabiko::test::SampleDbStruct::new(String::from("test value")),
                        OperationTarget::Main,
                    )
                    .unwrap();
                    db.get::<yamabiko::test::SampleDbStruct>(
                        format!("key-{}", i).as_str(),
                        OperationTarget::Main,
                    )
                    .unwrap();
                    i += 1;
                })
            },
        );
    }
}

criterion_group! {
name = benches;
config = Criterion::default().sample_size(20);
targets = bench_sets, bench_sets_and_gets}
criterion_main!(benches);
