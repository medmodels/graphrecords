import pickle
import re
import unittest

import pytest

from graphrecords import GraphRecord
from graphrecords.datatype import (
    Any,
    Bool,
    DateTime,
    Duration,
    Float,
    Int,
    Null,
    Option,
    String,
    Union,
)
from graphrecords.schema import (
    AttributeDataType,
    AttributeType,
    GroupSchema,
    Schema,
    SchemaType,
)


class TestAttributeType(unittest.TestCase):
    def test_infer(self) -> None:
        assert AttributeType.infer(String()) == AttributeType.Unstructured
        assert AttributeType.infer(Int()) == AttributeType.Continuous
        assert AttributeType.infer(Float()) == AttributeType.Continuous
        assert AttributeType.infer(Bool()) == AttributeType.Categorical
        assert AttributeType.infer(DateTime()) == AttributeType.Temporal
        assert AttributeType.infer(Duration()) == AttributeType.Temporal
        assert AttributeType.infer(Null()) == AttributeType.Unstructured
        assert AttributeType.infer(Any()) == AttributeType.Unstructured
        assert AttributeType.infer(Union(Int(), Float())) == AttributeType.Continuous
        assert AttributeType.infer(Option(Int())) == AttributeType.Continuous

    def test_repr(self) -> None:
        assert repr(AttributeType.Categorical) == "AttributeType.Categorical"
        assert repr(AttributeType.Continuous) == "AttributeType.Continuous"
        assert repr(AttributeType.Temporal) == "AttributeType.Temporal"
        assert repr(AttributeType.Unstructured) == "AttributeType.Unstructured"

    def test_str(self) -> None:
        assert str(AttributeType.Categorical) == "Categorical"
        assert str(AttributeType.Continuous) == "Continuous"
        assert str(AttributeType.Temporal) == "Temporal"
        assert str(AttributeType.Unstructured) == "Unstructured"


class TestSchemaType(unittest.TestCase):
    def test_repr(self) -> None:
        assert repr(SchemaType.Provided) == "SchemaType.Provided"
        assert repr(SchemaType.Inferred) == "SchemaType.Inferred"

    def test_str(self) -> None:
        assert str(SchemaType.Provided) == "Provided"
        assert str(SchemaType.Inferred) == "Inferred"


class TestAttributeDataType(unittest.TestCase):
    def test_construction_and_getters(self) -> None:
        categorical = AttributeDataType(Bool(), AttributeType.Categorical)
        continuous = AttributeDataType(Int(), AttributeType.Continuous)
        temporal = AttributeDataType(DateTime(), AttributeType.Temporal)
        unstructured = AttributeDataType(String(), AttributeType.Unstructured)
        inferred = AttributeDataType(Int())

        assert categorical.data_type == Bool()
        assert categorical.attribute_type == AttributeType.Categorical
        assert continuous.data_type == Int()
        assert continuous.attribute_type == AttributeType.Continuous
        assert temporal.data_type == DateTime()
        assert temporal.attribute_type == AttributeType.Temporal
        assert unstructured.data_type == String()
        assert unstructured.attribute_type == AttributeType.Unstructured
        assert inferred.data_type == Int()
        assert inferred.attribute_type == AttributeType.Continuous

    def test_eq(self) -> None:
        attribute_data_type = AttributeDataType(Int(), AttributeType.Continuous)

        assert attribute_data_type == AttributeDataType(Int(), AttributeType.Continuous)
        assert attribute_data_type != AttributeDataType(
            Float(), AttributeType.Continuous
        )
        assert attribute_data_type != AttributeDataType(
            Int(), AttributeType.Categorical
        )
        assert attribute_data_type != "not an attribute data type"

    def test_repr(self) -> None:
        attribute_data_type = AttributeDataType(Int(), AttributeType.Continuous)

        assert (
            repr(attribute_data_type)
            == "AttributeDataType(DataType.Int, AttributeType.Continuous)"
        )

    def test_invalid_construction(self) -> None:
        with pytest.raises(
            ValueError,
            match=r"Continuous attribute must be of \(sub-\)type `Int` or `Float`\.",
        ):
            AttributeDataType(String(), AttributeType.Continuous)

        with pytest.raises(
            ValueError,
            match=r"Temporal attribute must be of \(sub-\)type `DateTime` or `Duration`\.",
        ):
            AttributeDataType(Bool(), AttributeType.Temporal)

    def test_reduce(self) -> None:
        attribute_data_type = AttributeDataType(Int(), AttributeType.Continuous)

        restored = pickle.loads(pickle.dumps(attribute_data_type))

        assert restored == attribute_data_type


class TestGroupSchema(unittest.TestCase):
    def test_init_defaults(self) -> None:
        group_schema = GroupSchema()

        assert group_schema.nodes == {}
        assert group_schema.edges == {}

    def test_nodes(self) -> None:
        group_schema = GroupSchema(
            nodes={"key1": AttributeDataType(Int(), AttributeType.Continuous)}
        )

        assert group_schema.nodes == {
            "key1": AttributeDataType(Int(), AttributeType.Continuous)
        }
        assert group_schema.edges == {}

    def test_edges(self) -> None:
        group_schema = GroupSchema(
            edges={"key1": AttributeDataType(Bool(), AttributeType.Categorical)}
        )

        assert group_schema.edges == {
            "key1": AttributeDataType(Bool(), AttributeType.Categorical)
        }
        assert group_schema.nodes == {}

    def test_validate_node(self) -> None:
        group_schema = GroupSchema(
            nodes={
                "key1": AttributeDataType(Int(), AttributeType.Categorical),
                "key2": AttributeDataType(Float(), AttributeType.Continuous),
            }
        )

        group_schema.validate_node("0", {"key1": 0, "key2": 0.0})

        with pytest.raises(
            ValueError,
            match=(
                r'Attribute `"key1"` of node with index `"0"` is of type '
                r"`Float`\. Expected `Int`\."
            ),
        ):
            group_schema.validate_node("0", {"key1": 0.0, "key2": 0.0})

    def test_validate_edge(self) -> None:
        graphrecord = GraphRecord().add_node(0, {}).add_node(1, {})
        graphrecord = graphrecord.add_edge(0, 1, {})
        edge_index = graphrecord.edge_indices()[0]

        group_schema = GroupSchema(
            edges={
                "key1": AttributeDataType(Int(), AttributeType.Categorical),
                "key2": AttributeDataType(Float(), AttributeType.Continuous),
            }
        )

        group_schema.validate_edge(edge_index, {"key1": 0, "key2": 0.0})

        with pytest.raises(
            ValueError,
            match=(
                rf'Attribute `"key1"` of edge with index `{re.escape(str(edge_index))}` '
                r"is of type `Float`\. Expected `Int`\."
            ),
        ):
            group_schema.validate_edge(edge_index, {"key1": 0.0, "key2": 0.0})

    def test_eq(self) -> None:
        group_schema = GroupSchema(
            nodes={"key1": AttributeDataType(Int(), AttributeType.Continuous)}
        )

        assert group_schema == GroupSchema(
            nodes={"key1": AttributeDataType(Int(), AttributeType.Continuous)}
        )
        assert group_schema != GroupSchema(
            nodes={"key1": AttributeDataType(Float(), AttributeType.Continuous)}
        )
        assert group_schema != GroupSchema()
        assert group_schema != "not a group schema"

    def test_repr(self) -> None:
        group_schema = GroupSchema(
            nodes={"key1": AttributeDataType(Int(), AttributeType.Continuous)}
        )

        assert "key1" in repr(group_schema)
        assert repr(group_schema) != repr(GroupSchema())

    def test_reduce(self) -> None:
        group_schema = GroupSchema(
            nodes={"key1": AttributeDataType(Int(), AttributeType.Continuous)}
        )

        restored = pickle.loads(pickle.dumps(group_schema))

        assert restored == group_schema


class TestSchema(unittest.TestCase):
    def test_init_defaults(self) -> None:
        schema = Schema()

        assert schema.groups == {}
        assert schema.ungrouped.nodes == {}
        assert schema.ungrouped.edges == {}
        assert schema.schema_type == SchemaType.Provided

    def test_init_provided(self) -> None:
        group_schema = GroupSchema(
            nodes={"key1": AttributeDataType(Int(), AttributeType.Continuous)}
        )

        schema = Schema(groups={"group1": group_schema}, ungrouped=group_schema)

        assert schema.groups == {"group1": group_schema}
        assert schema.group("group1").nodes == group_schema.nodes
        assert schema.ungrouped.nodes == group_schema.nodes
        assert schema.schema_type == SchemaType.Provided

    def test_init_inferred(self) -> None:
        schema = Schema(schema_type=SchemaType.Inferred)

        assert schema.schema_type == SchemaType.Inferred

    def test_infer(self) -> None:
        graphrecord = GraphRecord()
        graphrecord = graphrecord.add_nodes([(0, {"key1": 0}), (1, {"key2": 0.0})])
        graphrecord = graphrecord.add_edges([(0, 1, {"key3": True})])

        schema = Schema.infer(graphrecord)

        assert schema.schema_type == SchemaType.Inferred
        assert schema.groups == {}
        assert set(schema.ungrouped.nodes) == {"key1", "key2"}
        assert set(schema.ungrouped.edges) == {"key3"}

        graphrecord = graphrecord.add_group("group1")
        graphrecord = graphrecord.add_nodes_to_group([0, 1], "group1")
        graphrecord = graphrecord.add_edges_to_group(
            graphrecord.edge_indices(), "group1"
        )

        schema = Schema.infer(graphrecord)

        assert set(schema.groups) == {"group1"}
        assert set(schema.group("group1").nodes) == {"key1", "key2"}
        assert set(schema.group("group1").edges) == {"key3"}
        assert schema.ungrouped.nodes == {}
        assert schema.ungrouped.edges == {}

    def test_groups(self) -> None:
        schema = Schema(groups={"group1": GroupSchema(), "group2": GroupSchema()})

        assert schema.groups == {"group1": GroupSchema(), "group2": GroupSchema()}

    def test_group(self) -> None:
        group_schema = GroupSchema(
            nodes={"key1": AttributeDataType(Int(), AttributeType.Continuous)}
        )
        schema = Schema(groups={"group1": group_schema})

        assert schema.group("group1").nodes == group_schema.nodes

        with pytest.raises(
            ValueError,
            match=r'Group with index `"missing"` is not defined in the schema',
        ):
            schema.group("missing")

    def test_ungrouped(self) -> None:
        group_schema = GroupSchema(
            edges={"key1": AttributeDataType(Bool(), AttributeType.Categorical)}
        )
        schema = Schema(ungrouped=group_schema)

        assert schema.ungrouped.edges == group_schema.edges

    def test_schema_type(self) -> None:
        assert Schema().schema_type == SchemaType.Provided
        assert (
            Schema(schema_type=SchemaType.Provided).schema_type == SchemaType.Provided
        )
        assert (
            Schema(schema_type=SchemaType.Inferred).schema_type == SchemaType.Inferred
        )

    def test_validate_node(self) -> None:
        schema = Schema(
            groups={
                "group1": GroupSchema(
                    nodes={"key1": AttributeDataType(Int(), AttributeType.Continuous)}
                )
            }
        )
        schema = schema.set_node_attribute("key1", Bool(), AttributeType.Categorical)

        schema.validate_node("0", {"key1": True})
        schema.validate_node("0", {"key1": 0}, "group1")

        with pytest.raises(
            ValueError,
            match=(
                r'Attribute `"key1"` of node with index `"0"` is of type '
                r"`Int`\. Expected `Bool`\."
            ),
        ):
            schema.validate_node("0", {"key1": 0})

        with pytest.raises(
            ValueError,
            match=r'Group with index `"missing"` is not defined in the schema',
        ):
            schema.validate_node("0", {"key1": 0}, "missing")

    def test_validate_edge(self) -> None:
        graphrecord = GraphRecord().add_node(0, {}).add_node(1, {})
        graphrecord = graphrecord.add_edge(0, 1, {})
        edge_index = graphrecord.edge_indices()[0]

        schema = Schema(
            groups={
                "group1": GroupSchema(
                    edges={"key1": AttributeDataType(Int(), AttributeType.Continuous)}
                )
            }
        )
        schema = schema.set_edge_attribute("key1", Bool(), AttributeType.Categorical)

        schema.validate_edge(edge_index, {"key1": True})
        schema.validate_edge(edge_index, {"key1": 0}, "group1")

        with pytest.raises(
            ValueError,
            match=(
                rf'Attribute `"key1"` of edge with index `{re.escape(str(edge_index))}` '
                r"is of type `Int`\. Expected `Bool`\."
            ),
        ):
            schema.validate_edge(edge_index, {"key1": 0})

        with pytest.raises(
            ValueError,
            match=r'Group with index `"missing"` is not defined in the schema',
        ):
            schema.validate_edge(edge_index, {"key1": 0}, "missing")

    def test_set_node_attribute(self) -> None:
        schema = Schema()

        updated = schema.set_node_attribute("key1", Int(), AttributeType.Continuous)

        assert updated is not schema
        assert updated.ungrouped.nodes == {
            "key1": AttributeDataType(Int(), AttributeType.Continuous)
        }
        assert schema.ungrouped.nodes == {}

        grouped = schema.set_node_attribute(
            "key1", Float(), AttributeType.Continuous, "group1"
        )

        assert grouped.group("group1").nodes == {
            "key1": AttributeDataType(Float(), AttributeType.Continuous)
        }
        assert set(grouped.groups) == {"group1"}
        assert schema.groups == {}

        overwritten = updated.set_node_attribute(
            "key1", Bool(), AttributeType.Categorical
        )

        assert overwritten.ungrouped.nodes == {
            "key1": AttributeDataType(Bool(), AttributeType.Categorical)
        }
        assert updated.ungrouped.nodes == {
            "key1": AttributeDataType(Int(), AttributeType.Continuous)
        }

    def test_set_edge_attribute(self) -> None:
        schema = Schema()

        updated = schema.set_edge_attribute("key1", Int(), AttributeType.Continuous)

        assert updated is not schema
        assert updated.ungrouped.edges == {
            "key1": AttributeDataType(Int(), AttributeType.Continuous)
        }
        assert schema.ungrouped.edges == {}

        grouped = schema.set_edge_attribute(
            "key1", Bool(), AttributeType.Categorical, "group1"
        )

        assert grouped.group("group1").edges == {
            "key1": AttributeDataType(Bool(), AttributeType.Categorical)
        }
        assert schema.groups == {}

    def test_update_node_attribute(self) -> None:
        schema = Schema()

        inserted = schema.update_node_attribute("key1", Int(), AttributeType.Continuous)

        assert inserted is not schema
        assert inserted.ungrouped.nodes == {
            "key1": AttributeDataType(Int(), AttributeType.Continuous)
        }
        assert schema.ungrouped.nodes == {}

        widened = inserted.update_node_attribute(
            "key1", Float(), AttributeType.Continuous
        )

        assert widened.ungrouped.nodes == {
            "key1": AttributeDataType(Union(Int(), Float()), AttributeType.Continuous)
        }
        assert inserted.ungrouped.nodes == {
            "key1": AttributeDataType(Int(), AttributeType.Continuous)
        }

        grouped = inserted.update_node_attribute(
            "key1", Float(), AttributeType.Continuous, "group1"
        )

        assert grouped.group("group1").nodes == {
            "key1": AttributeDataType(Float(), AttributeType.Continuous)
        }
        assert grouped.ungrouped.nodes == {
            "key1": AttributeDataType(Int(), AttributeType.Continuous)
        }

    def test_update_edge_attribute(self) -> None:
        schema = Schema().set_edge_attribute("key1", Bool(), AttributeType.Categorical)

        widened = schema.update_edge_attribute(
            "key1", String(), AttributeType.Unstructured
        )

        assert widened is not schema
        assert widened.ungrouped.edges == {
            "key1": AttributeDataType(
                Union(Bool(), String()), AttributeType.Unstructured
            )
        }
        assert schema.ungrouped.edges == {
            "key1": AttributeDataType(Bool(), AttributeType.Categorical)
        }

        grouped = schema.set_edge_attribute(
            "key1", Bool(), AttributeType.Categorical, "group1"
        ).update_edge_attribute("key1", String(), AttributeType.Unstructured, "group1")

        assert grouped.group("group1").edges == {
            "key1": AttributeDataType(
                Union(Bool(), String()), AttributeType.Unstructured
            )
        }

    def test_remove_node_attribute(self) -> None:
        schema = Schema().set_node_attribute("key1", Int(), AttributeType.Continuous)

        removed = schema.remove_node_attribute("key1")

        assert removed is not schema
        assert removed.ungrouped.nodes == {}
        assert schema.ungrouped.nodes == {
            "key1": AttributeDataType(Int(), AttributeType.Continuous)
        }

        grouped_schema = schema.set_node_attribute(
            "key1", Int(), AttributeType.Continuous, "group1"
        )
        grouped_removed = grouped_schema.remove_node_attribute("key1", "group1")

        assert grouped_removed.group("group1").nodes == {}
        assert grouped_schema.group("group1").nodes == {
            "key1": AttributeDataType(Int(), AttributeType.Continuous)
        }

        noop = schema.remove_node_attribute("missing")

        assert noop is not schema
        assert noop.ungrouped.nodes == schema.ungrouped.nodes

    def test_remove_edge_attribute(self) -> None:
        schema = Schema().set_edge_attribute("key1", Bool(), AttributeType.Categorical)

        removed = schema.remove_edge_attribute("key1")

        assert removed is not schema
        assert removed.ungrouped.edges == {}
        assert schema.ungrouped.edges == {
            "key1": AttributeDataType(Bool(), AttributeType.Categorical)
        }

        grouped_schema = schema.set_edge_attribute(
            "key1", Bool(), AttributeType.Categorical, "group1"
        )
        grouped_removed = grouped_schema.remove_edge_attribute("key1", "group1")

        assert grouped_removed.group("group1").edges == {}
        assert grouped_schema.group("group1").edges == {
            "key1": AttributeDataType(Bool(), AttributeType.Categorical)
        }

    def test_add_group(self) -> None:
        schema = Schema()
        group_schema = GroupSchema(
            nodes={"key1": AttributeDataType(Int(), AttributeType.Continuous)},
            edges={"key1": AttributeDataType(Float(), AttributeType.Continuous)},
        )

        added = schema.add_group("group1", group_schema)

        assert added is not schema
        assert added.groups == {"group1": group_schema}
        assert added.group("group1").nodes == group_schema.nodes
        assert added.group("group1").edges == group_schema.edges
        assert schema.groups == {}

        with pytest.raises(
            ValueError,
            match=r'Group with index `"group1"` already exists in the schema',
        ):
            added.add_group("group1", GroupSchema())

    def test_remove_group(self) -> None:
        schema = Schema(groups={"group1": GroupSchema()})

        removed = schema.remove_group("group1")

        assert removed is not schema
        assert removed.groups == {}
        assert schema.groups == {"group1": GroupSchema()}

        noop = schema.remove_group("missing")

        assert noop is not schema
        assert noop.groups == {"group1": GroupSchema()}

    def test_freeze(self) -> None:
        schema = Schema(schema_type=SchemaType.Inferred)

        frozen = schema.freeze()

        assert frozen is not schema
        assert frozen.schema_type == SchemaType.Provided
        assert schema.schema_type == SchemaType.Inferred

    def test_unfreeze(self) -> None:
        schema = Schema(schema_type=SchemaType.Provided)

        unfrozen = schema.unfreeze()

        assert unfrozen is not schema
        assert unfrozen.schema_type == SchemaType.Inferred
        assert schema.schema_type == SchemaType.Provided

    def test_eq(self) -> None:
        schema = Schema(groups={"group1": GroupSchema()})

        assert schema == Schema(groups={"group1": GroupSchema()})
        assert schema != Schema(groups={"group2": GroupSchema()})
        assert schema != Schema()
        assert schema != "not a schema"

    def test_repr(self) -> None:
        schema = Schema(groups={"group1": GroupSchema()})

        assert "group1" in repr(schema)
        assert repr(schema) != repr(Schema())

    def test_reduce(self) -> None:
        schema = Schema().set_node_attribute("key1", Int(), AttributeType.Continuous)

        restored = pickle.loads(pickle.dumps(schema))

        assert restored == schema


class TestGraphRecordSchemaWiring(unittest.TestCase):
    def test_with_schema(self) -> None:
        schema = Schema().set_node_attribute("key1", Int(), AttributeType.Continuous)

        graphrecord = GraphRecord.with_schema(schema)

        assert graphrecord.schema.schema_type == SchemaType.Provided
        assert graphrecord.schema.ungrouped.nodes == schema.ungrouped.nodes

        graphrecord = graphrecord.add_node(0, {"key1": 1})

        assert graphrecord.node_indices() == [0]

        with pytest.raises(
            ValueError,
            match=(
                r'Attribute `"key1"` of node with index `1` is of type '
                r"`String`\. Expected `Int`\."
            ),
        ):
            graphrecord.add_node(1, {"key1": "invalid"})

    def test_set_schema(self) -> None:
        graphrecord = GraphRecord().add_node(0, {"key1": 1})
        schema = Schema().set_node_attribute("key1", Int(), AttributeType.Continuous)

        updated = graphrecord.set_schema(schema)

        assert updated is not graphrecord
        assert updated.schema.schema_type == SchemaType.Provided
        assert graphrecord.schema.schema_type == SchemaType.Inferred

        invalid_schema = Schema().set_node_attribute(
            "key1", String(), AttributeType.Unstructured
        )

        with pytest.raises(
            ValueError,
            match=(
                r'Attribute `"key1"` of node with index `0` is of type '
                r"`Int`\. Expected `String`\."
            ),
        ):
            graphrecord.set_schema(invalid_schema)

    def test_freeze_schema(self) -> None:
        graphrecord = GraphRecord()

        assert graphrecord.schema.schema_type == SchemaType.Inferred

        frozen = graphrecord.freeze_schema()

        assert frozen is not graphrecord
        assert frozen.schema.schema_type == SchemaType.Provided
        assert graphrecord.schema.schema_type == SchemaType.Inferred

    def test_unfreeze_schema(self) -> None:
        graphrecord = GraphRecord.with_schema(Schema(schema_type=SchemaType.Provided))

        unfrozen = graphrecord.unfreeze_schema()

        assert unfrozen is not graphrecord
        assert unfrozen.schema.schema_type == SchemaType.Inferred
        assert graphrecord.schema.schema_type == SchemaType.Provided


if __name__ == "__main__":
    suite = unittest.TestSuite()

    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestAttributeType))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestSchemaType))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestAttributeDataType))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestGroupSchema))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestSchema))
    suite.addTests(
        unittest.TestLoader().loadTestsFromTestCase(TestGraphRecordSchemaWiring)
    )

    unittest.TextTestRunner(verbosity=2).run(suite)
