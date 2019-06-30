use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use rand::{Rng, SeedableRng};
use rand::distributions::Standard;
use rand::rngs::{StdRng};
use rand::seq::IteratorRandom;
use std::collections::HashMap;
use tempfile::TempDir;

use kvs::{KvsEngine, KvStore, SledKvStore};

fn bench_kvs(c: &mut Criterion) {
    c.bench_function("kvs_write", |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = KvStore::open(temp_dir.path()).unwrap();
        let mut rng = StdRng::seed_from_u64(0);

        b.iter_batched(
            || gen_kv(&mut rng),
            |(key, val)| engine.set(key, val).unwrap(),
            BatchSize::SmallInput,
        )
    });

    c.bench_function("kvs_read", |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = KvStore::open(temp_dir.path()).unwrap();
        let mut rng = StdRng::seed_from_u64(0);
        let data = gen_data(&mut rng, &mut engine);

        b.iter_batched(
            || {
                let key = data.keys().choose(&mut rng).unwrap();
                let value = data.get(key).unwrap();
                (key.to_owned(), value.to_owned())
            },
            |(key, value)| assert_eq!(engine.get(key).unwrap().unwrap(), value),
            BatchSize::SmallInput,
        )
    });
}

fn bench_sled(c: &mut Criterion) {
    c.bench_function("sled_write", |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = SledKvStore::start_default(temp_dir.path()).unwrap();
        let mut rng = StdRng::seed_from_u64(0);

        b.iter_batched(
            || gen_kv(&mut rng),
            |(key, val)| engine.set(key, val).unwrap(),
            BatchSize::SmallInput,
        )
    });

    c.bench_function("sled_read", |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = SledKvStore::start_default(temp_dir.path()).unwrap();
        let mut rng = StdRng::seed_from_u64(0);
        let data = gen_data(&mut rng, &mut engine);

        b.iter_batched(
            || {
                let key = data.keys().choose(&mut rng).unwrap();
                let value = data.get(key).unwrap();
                (key.to_owned(), value.to_owned())
            },
            |(key, value)| assert_eq!(engine.get(key).unwrap().unwrap(), value),
            BatchSize::SmallInput,
        )
    });
}

fn gen_data(mut rng: impl Rng, engine: &mut impl KvsEngine) -> HashMap<String, String> {
    let mut data = HashMap::with_capacity(1000);
    for _ in 0..1000 {
        let (key, value) = gen_kv(&mut rng);
        data.insert(key.clone(), value.clone());
        engine.set(key, value).unwrap();
    }
    data
}

fn gen_kv(mut rng: impl Rng) -> (String, String) {
    let key_len = rng.gen_range(1, 100001);
    let key = rng.sample_iter::<char, _>(&Standard).take(key_len).collect();

    let val_len = rng.gen_range(1, 100001);
    let val = rng.sample_iter::<char, _>(&Standard).take(val_len).collect();

    (key, val)
}

criterion_group!(benches, bench_kvs, bench_sled);
criterion_main!(benches);
