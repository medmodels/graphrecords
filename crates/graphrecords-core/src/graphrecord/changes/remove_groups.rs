#[cfg(feature = "plugins")]
use super::Changes;
use super::{Change, sealed::Sealed};
#[cfg(feature = "plugins")]
use crate::graphrecord::{GraphRecord, plugins::Plugin};
use crate::{
    errors::{GraphRecordError, GraphRecordResult},
    graphrecord::{datatypes::GroupIndex, state::GraphState},
};
use graphrecords_utils::distinct::Distinct;

pub struct RemoveGroups {
    group_indices: Vec<GroupIndex>,
}

impl RemoveGroups {
    #[must_use]
    pub fn new(group_indices: Vec<GroupIndex>) -> Self {
        let group_indices: Vec<_> = group_indices.into_iter().collect::<Distinct<_>>().into();

        Self { group_indices }
    }

    #[must_use]
    pub fn group_indices(&self) -> &[GroupIndex] {
        &self.group_indices
    }
}

impl Sealed for RemoveGroups {}

impl Change for RemoveGroups {
    fn apply(self: Box<Self>, mut state: GraphState) -> GraphRecordResult<GraphState> {
        for group_index in self.group_indices {
            let address = state
                .resolve_group_address(&group_index)
                .ok_or(GraphRecordError::GroupNotFound { group_index })?;

            state.remove_group(address)?;
        }

        Ok(state)
    }

    #[cfg(feature = "plugins")]
    fn dispatch(
        self: Box<Self>,
        plugin: &dyn Plugin,
        record: &GraphRecord,
    ) -> GraphRecordResult<Changes> {
        plugin.on_remove_groups(record, *self)
    }

    #[cfg(feature = "plugins")]
    fn post_dispatch_hook(
        &self,
    ) -> fn(
        plugin: &dyn Plugin,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        |plugin, previous, candidate| plugin.post_remove_groups(previous, candidate)
    }
}

#[cfg(test)]
mod test {
    use super::RemoveGroups;
    use crate::graphrecord::datatypes::GroupIndex;

    #[test]
    fn test_new() {
        let removal = RemoveGroups::new(vec!["lorem".into(), "ipsum".into(), "lorem".into()]);

        assert_eq!(
            vec![GroupIndex::from("lorem"), GroupIndex::from("ipsum")],
            removal.group_indices()
        );
    }
}
