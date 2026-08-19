#[cfg(feature = "plugins")]
use super::Changes;
use super::{Change, sealed::Sealed};
#[cfg(feature = "plugins")]
use crate::graphrecord::{GraphRecord, plugins::Plugin};
use crate::{
    errors::{GraphRecordError, GraphRecordResult},
    graphrecord::{datatypes::Group, state::GraphState},
};
use graphrecords_utils::distinct::Distinct;

pub struct RemoveGroups {
    groups: Vec<Group>,
}

impl RemoveGroups {
    #[must_use]
    pub fn new(groups: Vec<Group>) -> Self {
        let groups: Vec<_> = groups.into_iter().collect::<Distinct<_>>().into();

        Self { groups }
    }

    #[must_use]
    pub fn groups(&self) -> &[Group] {
        &self.groups
    }
}

impl Sealed for RemoveGroups {}

impl Change for RemoveGroups {
    fn apply(self: Box<Self>, mut state: GraphState) -> GraphRecordResult<GraphState> {
        for group in self.groups {
            let address = state
                .resolve_group_address(&group)
                .ok_or(GraphRecordError::GroupNotFound { group })?;

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
    use crate::graphrecord::datatypes::Group;

    #[test]
    fn test_new() {
        let removal = RemoveGroups::new(vec!["lorem".into(), "ipsum".into(), "lorem".into()]);

        assert_eq!(
            vec![Group::from("lorem"), Group::from("ipsum")],
            removal.groups()
        );
    }
}
