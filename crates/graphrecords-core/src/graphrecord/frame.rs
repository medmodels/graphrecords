use super::datatypes::GroupIndex;
use std::collections::HashMap;

pub struct Tables<T> {
    pub nodes: T,
    pub edges: T,
}

pub struct Export<T> {
    pub ungrouped: Tables<T>,
    pub groups: HashMap<GroupIndex, Tables<T>>,
}
