use criterion::{criterion_group, criterion_main, Criterion};
use tempfile::TempDir;

use kvs::KvStore;

fn bm_set(c: &mut Criterion) {
  let temp_dir = TempDir::new().expect("unable to create temporary working directory");
  let mut store = KvStore::open(temp_dir.path()).expect("unable open kv store");

  c.bench_function("set", move |b| b.iter(|| store.set("hello".to_owned(), "world".to_owned())));
}

fn bm_get(c: &mut Criterion) {
  let temp_dir = TempDir::new().expect("unable to create temporary working directory");
  let mut store = KvStore::open(temp_dir.path()).expect("unable open kv store");
  store.set("hello".to_owned(), "world".to_owned()).expect("unable to set test value");

  c.bench_function("get", move |b| b.iter(|| store.get("hello".to_owned())));
}

criterion_group!(benches, bm_set, bm_get);
criterion_main!(benches);
