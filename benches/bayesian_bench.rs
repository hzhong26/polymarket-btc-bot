use criterion::{black_box, criterion_group, criterion_main, Criterion};
use polymarket_btc_bot::bayesian::models;

fn bench_normal_cdf(c: &mut Criterion) {
    c.bench_function("normal_cdf", |b| {
        b.iter(|| models::normal_cdf(black_box(1.5)))
    });
}

fn bench_binary_call_probability(c: &mut Criterion) {
    c.bench_function("binary_call_probability", |b| {
        b.iter(|| {
            models::binary_call_probability(
                black_box(68000.0),
                black_box(67500.0),
                black_box(300.0),
                black_box(0.80),
            )
        })
    });
}

fn bench_black_scholes_d2(c: &mut Criterion) {
    c.bench_function("black_scholes_d2", |b| {
        b.iter(|| {
            models::black_scholes_d2(
                black_box(68000.0),
                black_box(67500.0),
                black_box(300.0),
                black_box(0.80),
                black_box(0.0),
            )
        })
    });
}

criterion_group!(
    benches,
    bench_normal_cdf,
    bench_binary_call_probability,
    bench_black_scholes_d2,
);
criterion_main!(benches);
