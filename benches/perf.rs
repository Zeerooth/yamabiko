use std::collections::HashMap;

use criterion::{criterion_group, criterion_main, Criterion};
use yamabiko::test::create_db;

fn bench_sets(bench: &mut Criterion) {
    bench.bench_function("sets on empty db", |b| {
        let (db, _td) = create_db();
        let mut i = 0;
        b.iter(|| {
            db.set(format!("key-{}", i).as_str(), b"some value");
            i += 1;
        })
    });
    bench.bench_function("sets on larger database", |b| {
        let (db, _td) = create_db();
        const INIT_DB_SIZE: usize = 10_000;
        let hm: [usize; INIT_DB_SIZE] = core::array::from_fn(|i| i + 1);
        let hm2 = hm
            .iter()
            .map(|x| (format!("key-{}", x), "some value".as_bytes()));
        db.set_batch(hm2);
        let mut i = INIT_DB_SIZE;
        b.iter(|| {
            db.set(format!("key-{}", i).as_str(), b"some value");
            i += 1;
        })
    });
    bench.bench_function("batch set", |b| {
        let (db, _td) = create_db();
        let mut i = 0;
        b.iter(|| {
            let mut hm = HashMap::with_capacity(100);
            for x in 0..100 {
                hm.insert(format!("key-{}", x + i), "some value".as_bytes());
            }
            db.set_batch(hm);
            i += 100;
        });
    });
}

criterion_group!(benches, bench_sets);
criterion_main!(benches);
