use super::GraphRecord;
use super::GraphRecordAttribute;
#[cfg(feature = "plugins")]
use super::graph::Attributes;
use super::graph::NodeIndex;
use super::group_mapping::Group;
use super::intern_table::{AttributeNameKind, GroupKind, Handle, HandleKind, NodeIndexKind};
use crate::errors::{GraphRecordError, GraphRecordResult};

pub trait HandleLookup<K: HandleKind> {
    fn resolve_handle(&self, handle: Handle<K>) -> GraphRecordResult<&K::Value>;
    fn handle_of(&self, value: &K::Value) -> Option<Handle<K>>;
}

pub trait AsLookup<K: HandleKind> {
    fn resolve(&self, graph: &GraphRecord) -> GraphRecordResult<Handle<K>>;
}

pub trait AsAttributeName {
    fn as_attribute_name<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<&'a GraphRecordAttribute>;
}

impl<T: AsAttributeName + ?Sized> AsAttributeName for &T {
    fn as_attribute_name<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<&'a GraphRecordAttribute> {
        <T as AsAttributeName>::as_attribute_name(*self, graphrecord)
    }
}

impl AsAttributeName for GraphRecordAttribute {
    fn as_attribute_name<'a>(
        &'a self,
        _: &'a GraphRecord,
    ) -> GraphRecordResult<&'a GraphRecordAttribute> {
        Ok(self)
    }
}

impl AsAttributeName for Handle<AttributeNameKind> {
    fn as_attribute_name<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<&'a GraphRecordAttribute> {
        <GraphRecord as HandleLookup<AttributeNameKind>>::resolve_handle(graphrecord, *self)
    }
}

pub fn resolve_all<K, I, L>(
    graph: &GraphRecord,
    items: I,
) -> GraphRecordResult<Vec<Handle<K>>>
where
    K: HandleKind,
    I: IntoIterator<Item = L>,
    L: AsLookup<K>,
{
    items.into_iter().map(|item| item.resolve(graph)).collect()
}

#[cfg(feature = "plugins")]
pub type EdgeHandleTuples = Vec<(Handle<NodeIndexKind>, Handle<NodeIndexKind>, Attributes)>;

#[cfg(feature = "plugins")]
pub fn resolve_edge_handles<S, T>(
    graph: &GraphRecord,
    edges: Vec<(S, T, Attributes)>,
) -> GraphRecordResult<EdgeHandleTuples>
where
    S: AsLookup<NodeIndexKind>,
    T: AsLookup<NodeIndexKind>,
{
    edges
        .into_iter()
        .map(|(source, target, attributes)| {
            let source_handle = source.resolve(graph)?;
            let target_handle = target.resolve(graph)?;

            Ok((source_handle, target_handle, attributes))
        })
        .collect()
}

impl<T, K> AsLookup<K> for &T
where
    T: AsLookup<K> + ?Sized,
    K: HandleKind,
{
    fn resolve(&self, graph: &GraphRecord) -> GraphRecordResult<Handle<K>> {
        <T as AsLookup<K>>::resolve(*self, graph)
    }
}

impl AsLookup<NodeIndexKind> for NodeIndex {
    fn resolve(&self, graph: &GraphRecord) -> GraphRecordResult<Handle<NodeIndexKind>> {
        <GraphRecord as HandleLookup<NodeIndexKind>>::handle_of(graph, self).ok_or_else(|| {
            GraphRecordError::IndexError(format!("Cannot find node with index {self}"))
        })
    }
}

#[cfg(feature = "safe-handles")]
impl AsLookup<NodeIndexKind> for Handle<NodeIndexKind> {
    fn resolve(&self, graph: &GraphRecord) -> GraphRecordResult<Self> {
        graph.graph.validate_handle(*self)?;

        Ok(*self)
    }
}

#[cfg(not(feature = "safe-handles"))]
impl AsLookup<NodeIndexKind> for Handle<NodeIndexKind> {
    fn resolve(&self, _: &GraphRecord) -> GraphRecordResult<Self> {
        Ok(*self)
    }
}

impl AsLookup<GroupKind> for Group {
    fn resolve(&self, graph: &GraphRecord) -> GraphRecordResult<Handle<GroupKind>> {
        <GraphRecord as HandleLookup<GroupKind>>::handle_of(graph, self)
            .ok_or_else(|| GraphRecordError::IndexError(format!("Cannot find group {self}")))
    }
}

#[cfg(feature = "safe-handles")]
impl AsLookup<GroupKind> for Handle<GroupKind> {
    fn resolve(&self, graph: &GraphRecord) -> GraphRecordResult<Self> {
        graph.graph.validate_handle(*self)?;

        Ok(*self)
    }
}

#[cfg(not(feature = "safe-handles"))]
impl AsLookup<GroupKind> for Handle<GroupKind> {
    fn resolve(&self, _: &GraphRecord) -> GraphRecordResult<Self> {
        Ok(*self)
    }
}

impl AsLookup<AttributeNameKind> for GraphRecordAttribute {
    fn resolve(&self, graph: &GraphRecord) -> GraphRecordResult<Handle<AttributeNameKind>> {
        <GraphRecord as HandleLookup<AttributeNameKind>>::handle_of(graph, self)
            .ok_or_else(|| GraphRecordError::IndexError(format!("Cannot find attribute {self}")))
    }
}

#[cfg(feature = "safe-handles")]
impl AsLookup<AttributeNameKind> for Handle<AttributeNameKind> {
    fn resolve(&self, graph: &GraphRecord) -> GraphRecordResult<Self> {
        graph.graph.validate_handle(*self)?;

        Ok(*self)
    }
}

#[cfg(not(feature = "safe-handles"))]
impl AsLookup<AttributeNameKind> for Handle<AttributeNameKind> {
    fn resolve(&self, _: &GraphRecord) -> GraphRecordResult<Self> {
        Ok(*self)
    }
}

impl HandleLookup<NodeIndexKind> for GraphRecord {
    fn resolve_handle(
        &self,
        handle: Handle<NodeIndexKind>,
    ) -> GraphRecordResult<&<NodeIndexKind as HandleKind>::Value> {
        #[cfg(feature = "safe-handles")]
        self.graph.validate_handle(handle)?;

        if !self.graph.nodes.contains_key(&handle) {
            return Err(GraphRecordError::IndexError(format!(
                "Cannot find node for handle {handle:?}"
            )));
        }

        Ok(self.graph.node_index_table.resolve(handle))
    }

    fn handle_of(
        &self,
        value: &<NodeIndexKind as HandleKind>::Value,
    ) -> Option<Handle<NodeIndexKind>> {
        self.graph.node_index_table.get(value)
    }
}

impl HandleLookup<GroupKind> for GraphRecord {
    fn resolve_handle(
        &self,
        handle: Handle<GroupKind>,
    ) -> GraphRecordResult<&<GroupKind as HandleKind>::Value> {
        #[cfg(feature = "safe-handles")]
        self.graph.validate_handle(handle)?;

        if !self.group_mapping.contains_group(handle) {
            return Err(GraphRecordError::IndexError(format!(
                "Cannot find group for handle {handle:?}"
            )));
        }

        Ok(self.graph.group_name_table.resolve(handle))
    }

    fn handle_of(&self, value: &<GroupKind as HandleKind>::Value) -> Option<Handle<GroupKind>> {
        self.graph.group_name_table.get(value)
    }
}

impl HandleLookup<AttributeNameKind> for GraphRecord {
    fn resolve_handle(
        &self,
        handle: Handle<AttributeNameKind>,
    ) -> GraphRecordResult<&<AttributeNameKind as HandleKind>::Value> {
        #[cfg(feature = "safe-handles")]
        self.graph.validate_handle(handle)?;

        if handle.index() as usize >= self.graph.attribute_name_table.len() {
            return Err(GraphRecordError::IndexError(format!(
                "Cannot find attribute for handle {handle:?}"
            )));
        }

        Ok(self.graph.attribute_name_table.resolve(handle))
    }

    fn handle_of(
        &self,
        value: &<AttributeNameKind as HandleKind>::Value,
    ) -> Option<Handle<AttributeNameKind>> {
        self.graph.attribute_name_table.get(value)
    }
}
