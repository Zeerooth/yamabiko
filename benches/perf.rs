use std::collections::HashMap;

use criterion::{criterion_group, criterion_main, Criterion};
use yamabiko::test::create_db;

fn fifty_sets(bench: &mut Criterion) {
    let (db, _td) = create_db();
    bench.bench_function("set 50", |b| {
        b.iter(|| {
            (0..50).fold(0, |_a, x| {
                db.set(format!("key-{}", x).as_str(), b"some value");
                x
            })
        })
    });
}

fn fifty_sets_on_large_database(bench: &mut Criterion) {
    let (db, _td) = create_db();
    let hm = (0..10_000)
        .map(|x| (x.to_string(), "some value".as_bytes()))
        .collect::<Vec<(String, &[u8])>>();
    db.set_batch(hm);
    bench.bench_function("set 50; large database", |b| {
        b.iter(|| {
            (0..50).fold(0, |_a, x| {
                db.set(format!("key-{}", x).as_str(), b"some value");
                x
            })
        })
    });
}

fn two_hundred_sets(bench: &mut Criterion) {
    let (db, _td) = create_db();
    bench.bench_function("set 200", |b| {
        b.iter(|| {
            (0..200).fold(0, |_a, x| {
                db.set(format!("key-{}", x).as_str(), b"some value");
                x
            })
        });
    });
}

fn thousand_batch_sets(bench: &mut Criterion) {
    bench.bench_function("batch set 100", |b| {
        b.iter(|| {
            (0..5).fold(0, |_a, w| {
                let (db, _td) = create_db();
                let mut hm = HashMap::with_capacity(1000);
                for x in 0..1000 {
                    hm.insert(format!("key-{}", x), "some value".as_bytes());
                }
                db.set_batch(hm);
                w
            })
        });
    });
}

criterion_group!(
    benches,
    fifty_sets,
    fifty_sets_on_large_database,
    two_hundred_sets,
    thousand_batch_sets
);
criterion_main!(benches);
