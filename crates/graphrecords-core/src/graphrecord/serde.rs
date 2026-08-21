use crate::graphrecord::{GraphRecord, state::GraphState};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error};
use std::sync::Arc;

impl Serialize for GraphRecord {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.state.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for GraphRecord {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let mut state = GraphState::deserialize(deserializer)?;

        if !state.is_referentially_consistent() {
            return Err(D::Error::custom(
                "graph state must be referentially consistent",
            ));
        }

        state.rebuild_dictionaries();

        Ok(Self {
            state: Arc::new(state),
            #[cfg(feature = "plugins")]
            plugins: Arc::new(Vec::new()),
        })
    }
}

#[cfg(all(test, feature = "io"))]
mod test {
    use crate::graphrecord::{
        AttributeMap, GraphRecord, Value,
        state::{EdgeEndpoints, NodeAddress},
    };

    fn create_graphrecord() -> GraphRecord {
        let graphrecord = GraphRecord::new()
            .add_node(
                "lorem",
                AttributeMap::from([("ipsum".into(), "dolor".into())]),
            )
            .unwrap()
            .add_node("sit", AttributeMap::new())
            .unwrap();
        let graphrecord = graphrecord
            .add_edge(
                "lorem",
                "sit",
                AttributeMap::from([("amet".into(), 42.into())]),
            )
            .unwrap();

        graphrecord.add_group("consectetur").unwrap()
    }

    #[test]
    fn test_serialize() {
        let graphrecord = create_graphrecord();

        let serialized = ron::to_string(&graphrecord).unwrap();

        assert_eq!(ron::to_string(&graphrecord).unwrap(), serialized);
    }

    #[test]
    #[cfg(feature = "plugins")]
    fn test_serialize_plugins() {
        use crate::graphrecord::Plugin;

        struct Quiet;

        impl Plugin for Quiet {}

        let graphrecord = create_graphrecord();
        let with_plugin = graphrecord.add_plugin("sed", Quiet).unwrap();

        assert_eq!(
            ron::to_string(&graphrecord).unwrap(),
            ron::to_string(&with_plugin).unwrap()
        );
    }

    #[test]
    fn test_deserialize() {
        let graphrecord = create_graphrecord();
        let serialized = ron::to_string(&graphrecord).unwrap();

        let deserialized: GraphRecord = ron::from_str(&serialized).unwrap();

        assert_eq!(2, deserialized.node_count());
        assert_eq!(1, deserialized.edge_count());
        assert_eq!(1, deserialized.group_count());
        assert!(deserialized.contains_node("lorem"));
        assert!(deserialized.contains_group("consectetur"));
        assert_eq!(
            Some(Value::from("dolor")),
            deserialized
                .node("lorem")
                .unwrap()
                .attribute("ipsum")
                .map(Value::from)
        );
    }

    #[test]
    fn test_invalid_deserialize() {
        let graphrecord = create_graphrecord();
        let mut state = (**graphrecord.state()).clone();
        let edge_address = state.edge_addresses().next().unwrap();

        state
            .edge_endpoints
            .get_mut_or_default(edge_address.chunk_index())
            .set(
                edge_address.chunk_local_address(),
                EdgeEndpoints {
                    source_address: NodeAddress::new(999),
                    target_address: NodeAddress::new(999),
                },
            );

        let serialized = ron::to_string(&state).unwrap();

        assert!(ron::from_str::<GraphRecord>(&serialized).is_err());
    }
}
