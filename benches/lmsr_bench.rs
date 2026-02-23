use criterion::{black_box, criterion_group, criterion_main, Criterion};
use polymarket_btc_bot::lmsr::math;

fn bench_softmax_binary(c: &mut Criterion) {
    c.bench_function("softmax_binary", |b| {
        b.iter(|| math::softmax_binary(black_box(10.0), black_box(5.0), black_box(3.0)))
    });
}

fn bench_cost_function(c: &mut Criterion) {
    c.bench_function("cost_function", |b| {
        b.iter(|| math::cost_function(black_box(100.0), black_box(50.0), black_box(30.0)))
    });
}

fn bench_cost_to_buy_yes(c: &mut Criterion) {
    c.bench_function("cost_to_buy_yes", |b| {
        b.iter(|| {
            math::cost_to_buy_yes(
                black_box(100.0),
                black_box(50.0),
                black_box(30.0),
                black_box(5.0),
            )
        })
    });
}

fn bench_log_sum_exp(c: &mut Criterion) {
    c.bench_function("log_sum_exp", |b| {
        b.iter(|| math::log_sum_exp(black_box(1000.0), black_box(1001.0)))
    });
}

criterion_group!(
    benches,
    bench_softmax_binary,
    bench_cost_function,
    bench_cost_to_buy_yes,
    bench_log_sum_exp,
);
criterion_main!(benches);
