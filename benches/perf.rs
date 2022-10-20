#[macro_use]
extern crate bencher;

use bencher::Bencher;
use yamabiko::test::create_db;

fn a(bench: &mut Bencher) {
    bench.iter(|| (0..100).fold(0, |x, y| x + y))
}

fn b(bench: &mut Bencher) {
    const N: usize = 128;
    bench.iter(|| vec![0u8; N]);

    bench.bytes = N as u64;
}

fn fifty_sets(bench: &mut Bencher) {
    let (db, _td) = create_db();
    bench.iter(|| {
        (0..50).fold(0, |_a, x| {
            db.set(format!("key-{}", x).as_str(), b"some value");
            x
        })
    });
}

fn two_hundred_sets(bench: &mut Bencher) {
    let (db, _td) = create_db();
    bench.iter(|| {
        (0..200).fold(0, |_a, x| {
            db.set(format!("key-{}", x).as_str(), b"some value");
            x
        })
    });
}

benchmark_group!(benches, a, b, fifty_sets, two_hundred_sets);
benchmark_main!(benches);
