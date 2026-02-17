use graphrecords_utils::aliases::GrHashMap;
use parking_lot::Mutex;
use rustc_hash::FxBuildHasher;
use std::hash::Hash;

type FxGrHashMap<K, V> = GrHashMap<K, V, FxBuildHasher>;

pub(crate) struct ConversionLut<K, V>(Mutex<FxGrHashMap<K, V>>);

impl<K, V> ConversionLut<K, V> {
    pub const fn new() -> Self {
        Self(Mutex::new(FxGrHashMap::with_hasher(FxBuildHasher)))
    }

    pub fn get_or_insert<F>(&self, key: K, insert_fn: F) -> V
    where
        K: Eq + Hash,
        V: Copy,
        F: FnOnce() -> V,
    {
        let mut inner = self.0.lock();
        *inner.entry(key).or_insert_with(insert_fn)
    }
}
