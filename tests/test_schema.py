import unittest

import pytest

import graphrecords as gr
from graphrecords._graphrecords import (
    PyAttributeDataType,
    PyAttributeType,
    PyGroupSchema,
    PySchema,
)


class TestAttributeType(unittest.TestCase):
    def test_from_py_attribute_type(self) -> None:
        assert (
            gr.AttributeType._from_py_attribute_type(PyAttributeType.Categorical)
            == gr.AttributeType.Categorical
        )
        assert (
            gr.AttributeType._from_py_attribute_type(PyAttributeType.Continuous)
            == gr.AttributeType.Continuous
        )
        assert (
            gr.AttributeType._from_py_attribute_type(PyAttributeType.Temporal)
            == gr.AttributeType.Temporal
        )
        assert (
            gr.AttributeType._from_py_attribute_type(PyAttributeType.Unstructured)
            == gr.AttributeType.Unstructured
        )

    def test_infer(self) -> None:
        assert gr.AttributeType.infer(gr.String()) == gr.AttributeType.Unstructured
        assert gr.AttributeType.infer(gr.Int()) == gr.AttributeType.Continuous
        assert gr.AttributeType.infer(gr.Float()) == gr.AttributeType.Continuous
        assert gr.AttributeType.infer(gr.Bool()) == gr.AttributeType.Categorical
        assert gr.AttributeType.infer(gr.DateTime()) == gr.AttributeType.Temporal
        assert gr.AttributeType.infer(gr.Duration()) == gr.AttributeType.Temporal
        assert gr.AttributeType.infer(gr.Null()) == gr.AttributeType.Unstructured
        assert gr.AttributeType.infer(gr.Any()) == gr.AttributeType.Unstructured
        assert (
            gr.AttributeType.infer(gr.Union(gr.Int(), gr.Float()))
            == gr.AttributeType.Continuous
        )
        assert (
            gr.AttributeType.infer(gr.Option(gr.Int())) == gr.AttributeType.Continuous
        )

    def test_into_py_attribute_type(self) -> None:
        assert (
            gr.AttributeType.Categorical._into_py_attribute_type()
            == PyAttributeType.Categorical
        )
        assert (
            gr.AttributeType.Continuous._into_py_attribute_type()
            == PyAttributeType.Continuous
        )
        assert (
            gr.AttributeType.Temporal._into_py_attribute_type()
            == PyAttributeType.Temporal
        )
        assert (
            gr.AttributeType.Unstructured._into_py_attribute_type()
            == PyAttributeType.Unstructured
        )

    def test_repr(self) -> None:
        assert repr(gr.AttributeType.Categorical) == "AttributeType.Categorical"
        assert repr(gr.AttributeType.Continuous) == "AttributeType.Continuous"
        assert repr(gr.AttributeType.Temporal) == "AttributeType.Temporal"
        assert repr(gr.AttributeType.Unstructured) == "AttributeType.Unstructured"

    def test_str(self) -> None:
        assert str(gr.AttributeType.Categorical) == "Categorical"
        assert str(gr.AttributeType.Continuous) == "Continuous"
        assert str(gr.AttributeType.Temporal) == "Temporal"
        assert str(gr.AttributeType.Unstructured) == "Unstructured"

    def test_hash(self) -> None:
        assert hash(gr.AttributeType.Categorical) == hash("Categorical")
        assert hash(gr.AttributeType.Continuous) == hash("Continuous")
        assert hash(gr.AttributeType.Temporal) == hash("Temporal")
        assert hash(gr.AttributeType.Unstructured) == hash("Unstructured")

    def test_eq(self) -> None:
        assert gr.AttributeType.Categorical == gr.AttributeType.Categorical
        assert gr.AttributeType.Categorical == PyAttributeType.Categorical
        assert gr.AttributeType.Categorical != gr.AttributeType.Continuous
        assert gr.AttributeType.Categorical != gr.AttributeType.Temporal
        assert gr.AttributeType.Categorical != gr.AttributeType.Unstructured
        assert gr.AttributeType.Categorical != PyAttributeType.Continuous
        assert gr.AttributeType.Categorical != PyAttributeType.Temporal
        assert gr.AttributeType.Categorical != PyAttributeType.Unstructured

        assert gr.AttributeType.Continuous == gr.AttributeType.Continuous
        assert gr.AttributeType.Continuous == PyAttributeType.Continuous
        assert gr.AttributeType.Continuous != gr.AttributeType.Categorical
        assert gr.AttributeType.Continuous != gr.AttributeType.Temporal
        assert gr.AttributeType.Continuous != gr.AttributeType.Unstructured
        assert gr.AttributeType.Continuous != PyAttributeType.Categorical
        assert gr.AttributeType.Continuous != PyAttributeType.Temporal
        assert gr.AttributeType.Continuous != PyAttributeType.Unstructured
        assert gr.AttributeType.Continuous != "Continuous"

        assert gr.AttributeType.Temporal == gr.AttributeType.Temporal
        assert gr.AttributeType.Temporal == PyAttributeType.Temporal
        assert gr.AttributeType.Temporal != gr.AttributeType.Categorical
        assert gr.AttributeType.Temporal != gr.AttributeType.Continuous
        assert gr.AttributeType.Temporal != gr.AttributeType.Unstructured
        assert gr.AttributeType.Temporal != PyAttributeType.Categorical
        assert gr.AttributeType.Temporal != PyAttributeType.Continuous
        assert gr.AttributeType.Temporal != PyAttributeType.Unstructured
        assert gr.AttributeType.Temporal != "Temporal"

        assert gr.AttributeType.Unstructured == gr.AttributeType.Unstructured
        assert gr.AttributeType.Unstructured == PyAttributeType.Unstructured
        assert gr.AttributeType.Unstructured != gr.AttributeType.Categorical
        assert gr.AttributeType.Unstructured != gr.AttributeType.Continuous
        assert gr.AttributeType.Unstructured != gr.AttributeType.Temporal
        assert gr.AttributeType.Unstructured != PyAttributeType.Categorical
        assert gr.AttributeType.Unstructured != PyAttributeType.Continuous
        assert gr.AttributeType.Unstructured != PyAttributeType.Temporal
        assert gr.AttributeType.Unstructured != "Unstructured"


class TestGroupSchema(unittest.TestCase):
    def test_from_py_group_schema(self) -> None:
        assert gr.GroupSchema._from_py_group_schema(
            PyGroupSchema(
                nodes={
                    "test": PyAttributeDataType(
                        gr.String()._inner(), PyAttributeType.Unstructured
                    )
                },
                edges={},
            )
        ).nodes == {"test": (gr.String(), gr.AttributeType.Unstructured)}

    def test_nodes(self) -> None:
        group_schema = gr.GroupSchema(nodes={"test": gr.String()}, edges={})

        assert group_schema.nodes == {
            "test": (gr.String(), gr.AttributeType.Unstructured)
        }

    def test_edges(self) -> None:
        group_schema = gr.GroupSchema(nodes={}, edges={"test": gr.String()})

        assert group_schema.edges == {
            "test": (gr.String(), gr.AttributeType.Unstructured)
        }

    def test_validate_node(self) -> None:
        group_schema = gr.GroupSchema(
            nodes={
                "key1": (gr.Int(), gr.AttributeType.Categorical),
                "key2": (gr.Float(), gr.AttributeType.Continuous),
            },
            edges={},
        )

        group_schema.validate_node("0", {"key1": 0, "key2": 0.0})

        with pytest.raises(
            ValueError,
            match=r"Attribute key1 of node with index 0 is of type Float. Expected Int.",
        ):
            group_schema.validate_node("0", {"key1": 0.0, "key2": 0.0})

    def test_validate_edge(self) -> None:
        group_schema = gr.GroupSchema(
            nodes={},
            edges={
                "key1": (gr.Int(), gr.AttributeType.Categorical),
                "key2": (gr.Float(), gr.AttributeType.Continuous),
            },
        )

        group_schema.validate_edge(0, {"key1": 0, "key2": 0.0})

        with pytest.raises(
            ValueError,
            match=r"Attribute key1 of edge with index 0 is of type Float. Expected Int.",
        ):
            group_schema.validate_edge(0, {"key1": 0.0, "key2": 0.0})


class TestSchema(unittest.TestCase):
    def test_infer(self) -> None:
        graphrecord = gr.GraphRecord()
        graphrecord.add_nodes([(0, {"key1": 0}), (1, {"key2": 0.0})])
        graphrecord.add_edges((0, 1, {"key3": True}))

        schema = gr.Schema.infer(graphrecord)

        assert len(schema.ungrouped.nodes) == 2
        assert len(schema.ungrouped.edges) == 1

        graphrecord.add_group("test", [0, 1], [0])

        schema = gr.Schema.infer(graphrecord)

        assert len(schema.ungrouped.nodes) == 0
        assert len(schema.ungrouped.edges) == 0
        assert len(schema.groups) == 1
        assert len(schema.group("test").nodes) == 2
        assert len(schema.group("test").edges) == 1

    def test_from_py_schema(self) -> None:
        assert gr.Schema._from_py_schema(
            PySchema(
                groups={},
                ungrouped=PyGroupSchema(
                    nodes={
                        "test": PyAttributeDataType(
                            gr.String()._inner(), PyAttributeType.Unstructured
                        )
                    },
                    edges={},
                ),
            )
        ).ungrouped.nodes == {"test": (gr.String(), gr.AttributeType.Unstructured)}

    def test_groups(self) -> None:
        schema = gr.Schema(
            groups={"test": gr.GroupSchema()}, ungrouped=gr.GroupSchema()
        )

        assert schema.groups == ["test"]

    def test_group(self) -> None:
        schema = gr.Schema(
            groups={"test": gr.GroupSchema(nodes={"test": gr.String()}, edges={})},
            ungrouped=gr.GroupSchema(),
        )

        assert schema.group("test").nodes == {
            "test": (gr.String(), gr.AttributeType.Unstructured)
        }

        with pytest.raises(ValueError, match=r"Group invalid not found in schema."):
            schema.group("invalid")

    def test_default(self) -> None:
        schema = gr.Schema(
            groups={}, ungrouped=gr.GroupSchema(nodes={"test": gr.String()}, edges={})
        )

        assert schema.ungrouped.nodes == {
            "test": (gr.String(), gr.AttributeType.Unstructured)
        }

    def test_schema_type(self) -> None:
        schema = gr.Schema(groups={}, ungrouped=gr.GroupSchema())

        assert schema.schema_type == gr.SchemaType.Provided

        schema = gr.Schema(
            groups={}, ungrouped=gr.GroupSchema(), schema_type=gr.SchemaType.Provided
        )

        assert schema.schema_type == gr.SchemaType.Provided

        schema = gr.Schema(
            groups={"test": gr.GroupSchema()},
            ungrouped=gr.GroupSchema(),
            schema_type=gr.SchemaType.Inferred,
        )

        assert schema.schema_type == gr.SchemaType.Inferred

    def test_validate_node(self) -> None:
        schema = gr.Schema(groups={}, ungrouped=gr.GroupSchema())

        schema.set_node_attribute("key1", gr.Int(), gr.AttributeType.Continuous)

        schema.validate_node("0", {"key1": 0})

        with pytest.raises(
            ValueError,
            match=r"Attribute key1 of node with index 0 is of type String. Expected Int.",
        ):
            schema.validate_node("0", {"key1": "invalid"})

    def test_validate_edge(self) -> None:
        schema = gr.Schema(groups={}, ungrouped=gr.GroupSchema())

        schema.set_edge_attribute("key1", gr.Bool(), gr.AttributeType.Categorical)

        schema.validate_edge(0, {"key1": True})

        with pytest.raises(
            ValueError,
            match=r"Attribute key1 of edge with index 0 is of type Int. Expected Bool.",
        ):
            schema.validate_edge(0, {"key1": 0})

    def test_set_node_attribute(self) -> None:
        schema = gr.Schema(groups={}, ungrouped=gr.GroupSchema())

        schema.set_node_attribute("key1", gr.Int())

        assert schema.ungrouped.nodes["key1"][0] == gr.Int()

        schema.set_node_attribute("key1", gr.Float(), gr.AttributeType.Continuous)

        assert schema.ungrouped.nodes["key1"][0] == gr.Float()

        schema.set_node_attribute(
            "key1", gr.Float(), gr.AttributeType.Continuous, "group1"
        )

        assert schema.group("group1").nodes["key1"][0] == gr.Float()

    def test_set_edge_attribute(self) -> None:
        schema = gr.Schema(groups={}, ungrouped=gr.GroupSchema())

        schema.set_edge_attribute("key1", gr.Bool())

        assert schema.ungrouped.edges["key1"][0] == gr.Bool()

        schema.set_edge_attribute("key1", gr.Float(), gr.AttributeType.Continuous)

        assert schema.ungrouped.edges["key1"][0] == gr.Float()

        schema.set_edge_attribute(
            "key1", gr.Float(), gr.AttributeType.Continuous, "group1"
        )

        assert schema.group("group1").edges["key1"][0] == gr.Float()

    def test_update_node_attribute(self) -> None:
        schema = gr.Schema(groups={}, ungrouped=gr.GroupSchema())

        schema.set_node_attribute("key1", gr.Int(), gr.AttributeType.Continuous)

        schema.update_node_attribute("key1", gr.Float())

        assert schema.ungrouped.nodes["key1"][0] == gr.Union(gr.Int(), gr.Float())

        schema.set_node_attribute(
            "key1", gr.Int(), gr.AttributeType.Continuous, "group1"
        )

        schema.update_node_attribute(
            "key1", gr.Float(), gr.AttributeType.Continuous, "group1"
        )

        assert schema.group("group1").nodes["key1"][0] == gr.Union(gr.Int(), gr.Float())

    def test_update_edge_attribute(self) -> None:
        schema = gr.Schema(groups={}, ungrouped=gr.GroupSchema())

        schema.set_edge_attribute("key1", gr.Bool(), gr.AttributeType.Categorical)

        schema.update_edge_attribute("key1", gr.String())

        assert schema.ungrouped.edges["key1"][0] == gr.Union(gr.Bool(), gr.String())

        schema.set_edge_attribute(
            "key1", gr.Bool(), gr.AttributeType.Categorical, "group1"
        )

        schema.update_edge_attribute(
            "key1", gr.String(), gr.AttributeType.Unstructured, "group1"
        )

        assert schema.group("group1").edges["key1"][0] == gr.Union(
            gr.Bool(), gr.String()
        )

    def test_remove_node_attribute(self) -> None:
        schema = gr.Schema(groups={}, ungrouped=gr.GroupSchema())

        schema.set_node_attribute("key1", gr.Int(), gr.AttributeType.Continuous)

        assert "key1" in schema.ungrouped.nodes

        schema.remove_node_attribute("key1")

        assert "key1" not in schema.ungrouped.nodes

        schema.set_node_attribute(
            "key1", gr.Int(), gr.AttributeType.Continuous, "group1"
        )

        assert "key1" in schema.group("group1").nodes

        schema.remove_node_attribute("key1", "group1")

        assert "key1" not in schema.group("group1").nodes

    def test_remove_edge_attribute(self) -> None:
        schema = gr.Schema(groups={}, ungrouped=gr.GroupSchema())

        schema.set_edge_attribute("key1", gr.Bool(), gr.AttributeType.Categorical)

        assert "key1" in schema.ungrouped.edges

        schema.remove_edge_attribute("key1")

        assert "key1" not in schema.ungrouped.edges

        schema.set_edge_attribute(
            "key1", gr.Bool(), gr.AttributeType.Categorical, "group1"
        )

        assert "key1" in schema.group("group1").edges

        schema.remove_edge_attribute("key1", "group1")

        assert "key1" not in schema.group("group1").edges

    def test_add_group(self) -> None:
        schema = gr.Schema(groups={}, ungrouped=gr.GroupSchema())

        schema.add_group(
            "group1",
            gr.GroupSchema(nodes={"key1": gr.Int()}, edges={"key1": gr.Float()}),
        )

        assert {"key1": (gr.Int(), gr.AttributeType.Continuous)} == schema.group(
            "group1"
        ).nodes
        assert {"key1": (gr.Float(), gr.AttributeType.Continuous)} == schema.group(
            "group1"
        ).edges

        with pytest.raises(ValueError, match=r"Group group1 already exists in schema."):
            schema.add_group("group1", gr.GroupSchema())

    def test_remove_group(self) -> None:
        schema = gr.Schema(
            groups={"group1": gr.GroupSchema()}, ungrouped=gr.GroupSchema()
        )

        assert "group1" in schema.groups

        schema.remove_group("group1")

        assert "group1" not in schema.groups

    def test_freeze_unfreeze(self) -> None:
        schema = gr.Schema(groups={}, ungrouped=gr.GroupSchema())

        schema.freeze()

        assert schema.schema_type == gr.SchemaType.Provided

        schema.unfreeze()

        assert schema.schema_type == gr.SchemaType.Inferred


if __name__ == "__main__":
    suite = unittest.TestSuite()

    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestAttributeType))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestGroupSchema))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestSchema))

    unittest.TextTestRunner(verbosity=2).run(suite)
