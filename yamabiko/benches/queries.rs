use std::{cmp::Ordering::*, time::Duration};

use criterion::{criterion_group, criterion_main, Criterion};
use yamabiko::{
    index::{Index, IndexType},
    query::{q, QueryBuilder, ResolutionStrategy},
    serialization::DataFormat,
    test::create_db,
    OperationTarget,
};

fn bench_queries(bench: &mut Criterion) {
    for data_format in [DataFormat::Json, DataFormat::Yaml, DataFormat::Pot] {
        let (db, _td) = create_db(data_format);
        const INIT_DB_SIZE: usize = 10_000;
        let hm: [usize; INIT_DB_SIZE] = core::array::from_fn(|i| i + 1);
        let hm2 = hm.iter().map(|x| {
            (
                format!("key-{}", x),
                yamabiko::test::ComplexDbStruct::new(String::from("test value"), *x, *x as f64),
            )
        });
        db.set_batch(hm2, OperationTarget::Main).unwrap();
        bench.bench_function(
            format!("query large database without indexes ({})", data_format).as_str(),
            |b| {
                b.iter(|| {
                    assert_eq!(
                        QueryBuilder::query(
                            q("usize_val", Less, 100)
                                | q("usize_val", Equal, 1000)
                                | q("usize_val", Greater, 9900),
                        )
                        .execute(&db)
                        .unwrap()
                        .count,
                        200
                    )
                })
            },
        );

        let (db, _td) = create_db(data_format);
        db.add_index("usize_val", IndexType::Numeric);
        let hm: [usize; INIT_DB_SIZE] = core::array::from_fn(|i| i + 1);
        let hm2 = hm.iter().map(|x| {
            (
                format!("key-{}", x),
                yamabiko::test::ComplexDbStruct::new(String::from("test value"), *x, *x as f64),
            )
        });
        db.set_batch(hm2, OperationTarget::Main).unwrap();
        bench.bench_function(
            format!("query large database with an index ({})", data_format).as_str(),
            |b| {
                b.iter(|| {
                    let query_result = QueryBuilder::query(
                        q("usize_val", Less, 100)
                            | q("usize_val", Equal, 1000)
                            | q("usize_val", Greater, 9900),
                    )
                    .execute(&db)
                    .unwrap();
                    assert_eq!(query_result.count, 200);
                    let index =
                        Index::new("usize_val#numeric.index", "usize_val", IndexType::Numeric);
                    assert_eq!(
                        query_result.resolution_strategy,
                        ResolutionStrategy::UseIndexes(vec![index.clone(), index.clone(), index])
                    )
                })
            },
        );
    }
}

criterion_group! {
name = benches;
config = Criterion::default().sample_size(50).nresamples(5000).warm_up_time(Duration::new(2, 0)).measurement_time(Duration::new(5, 0));
targets = bench_queries}
criterion_main!(benches);
