use hashbrown::{DefaultHashBuilder, HashMap, HashSet, hash_map::Entry};

pub type GrHashMap<K, V, S = DefaultHashBuilder> = HashMap<K, V, S>;
pub type GrHashMapEntry<'a, K, V, S> = Entry<'a, K, V, S>;
pub type GrHashSet<T> = HashSet<T>;
