use graphrecords_utils::aliases::GrHashMap;
use parking_lot::Mutex;
use rustc_hash::FxBuildHasher;

type FxGrHashMap<K, V> = GrHashMap<K, V, FxBuildHasher>;

pub(crate) struct ConversionLut<K, V>(Mutex<FxGrHashMap<K, V>>);

impl<K, V> ConversionLut<K, V> {
    pub const fn new() -> Self {
        Self(Mutex::new(FxGrHashMap::with_hasher(FxBuildHasher)))
    }

    pub fn map<F, O>(&self, operation: F) -> O
    where
        F: FnOnce(&mut FxGrHashMap<K, V>) -> O,
    {
        let mut inner = self.0.lock();
        operation(&mut inner)
    }
}
