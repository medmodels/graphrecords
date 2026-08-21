use graphrecords_utils::aliases::GrHashMap;
use parking_lot::Mutex;
use pyo3::{Py, types::PyType};
use rustc_hash::FxBuildHasher;
use std::hash::{Hash, Hasher};

type FxGrHashMap<K, V> = GrHashMap<K, V, FxBuildHasher>;

pub(crate) struct TypeObjectKey(Py<PyType>);

impl TypeObjectKey {
    fn address(&self) -> usize {
        self.0.as_ptr() as usize
    }
}

impl From<Py<PyType>> for TypeObjectKey {
    fn from(value: Py<PyType>) -> Self {
        Self(value)
    }
}

impl PartialEq for TypeObjectKey {
    fn eq(&self, other: &Self) -> bool {
        self.address() == other.address()
    }
}

impl Eq for TypeObjectKey {}

impl Hash for TypeObjectKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.address().hash(state);
    }
}

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
