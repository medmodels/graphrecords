import unittest
from typing import List

import pytest

import graphrecords as gr
from graphrecords.plugins import (
    Plugin,
    PostAddEdgesContext,
    PostAddEdgesWithGroupContext,
    PostAddGroupContext,
    PostAddNodesContext,
    PostAddNodesWithGroupContext,
    PreAddEdgesContext,
    PreAddEdgesWithGroupContext,
    PreAddGroupContext,
    PreAddNodesContext,
    PreAddNodesWithGroupContext,
    PreSetSchemaContext,
)


class RecordingPlugin(Plugin):
    def __init__(self) -> None:
        self.calls: List[str] = []

    def initialize(self, graphrecord: gr.GraphRecord) -> None:
        self.calls.append("initialize")

    def pre_add_nodes(
        self, graphrecord: gr.GraphRecord, context: PreAddNodesContext
    ) -> PreAddNodesContext:
        self.calls.append("pre_add_nodes")
        return context

    def post_add_nodes(
        self, graphrecord: gr.GraphRecord, context: PostAddNodesContext
    ) -> None:
        self.calls.append("post_add_nodes")

    def pre_add_nodes_with_group(
        self, graphrecord: gr.GraphRecord, context: PreAddNodesWithGroupContext
    ) -> PreAddNodesWithGroupContext:
        self.calls.append("pre_add_nodes_with_group")
        return context

    def post_add_nodes_with_group(
        self, graphrecord: gr.GraphRecord, context: PostAddNodesWithGroupContext
    ) -> None:
        self.calls.append("post_add_nodes_with_group")

    def pre_add_edges(
        self, graphrecord: gr.GraphRecord, context: PreAddEdgesContext
    ) -> PreAddEdgesContext:
        self.calls.append("pre_add_edges")
        return context

    def post_add_edges(
        self, graphrecord: gr.GraphRecord, context: PostAddEdgesContext
    ) -> None:
        self.calls.append("post_add_edges")

    def pre_add_edges_with_group(
        self, graphrecord: gr.GraphRecord, context: PreAddEdgesWithGroupContext
    ) -> PreAddEdgesWithGroupContext:
        self.calls.append("pre_add_edges_with_group")
        return context

    def post_add_edges_with_group(
        self, graphrecord: gr.GraphRecord, context: PostAddEdgesWithGroupContext
    ) -> None:
        self.calls.append("post_add_edges_with_group")

    def pre_add_group(
        self, graphrecord: gr.GraphRecord, context: PreAddGroupContext
    ) -> PreAddGroupContext:
        self.calls.append("pre_add_group")
        return context

    def post_add_group(
        self, graphrecord: gr.GraphRecord, context: PostAddGroupContext
    ) -> None:
        self.calls.append("post_add_group")

    def pre_set_schema(
        self, graphrecord: gr.GraphRecord, context: PreSetSchemaContext
    ) -> PreSetSchemaContext:
        self.calls.append("pre_set_schema")
        return context

    def post_set_schema(self, graphrecord: gr.GraphRecord) -> None:
        self.calls.append("post_set_schema")


class TestGraphRecordBuilder(unittest.TestCase):
    def test_add_nodes(self) -> None:
        builder = gr.GraphRecord.builder().add_nodes([("node1", {})])

        assert len(builder._nodes) == 1

        builder.add_nodes([("node2", {})], group="group")

        assert len(builder._nodes) == 2

        graphrecord = builder.build()

        assert len(graphrecord.nodes) == 2
        assert len(graphrecord.groups) == 1
        assert graphrecord.groups_of_node("node2") == ["group"]

    def test_add_edges(self) -> None:
        builder = (
            gr.GraphRecord.builder()
            .add_nodes([("node1", {}), ("node2", {})])
            .add_edges([("node1", "node2", {})])
        )

        assert len(builder._edges) == 1

        builder.add_edges([("node2", "node1", {})], group="group")

        graphrecord = builder.build()

        assert len(graphrecord.nodes) == 2
        assert len(graphrecord.edges) == 2
        assert len(graphrecord.groups) == 1
        assert graphrecord.neighbors("node1") == ["node2"]
        assert graphrecord.groups_of_edge(1) == ["group"]

    def test_add_group(self) -> None:
        builder = (
            gr.GraphRecord.builder()
            .add_nodes(("0", {}))
            .add_group("group", nodes=["0"])
        )

        assert len(builder._groups) == 1

        graphrecord = builder.build()

        assert len(graphrecord.nodes) == 1
        assert len(graphrecord.edges) == 0
        assert len(graphrecord.groups) == 1
        assert graphrecord.groups[0] == "group"
        assert graphrecord.nodes_in_group("group") == ["0"]

        builder = gr.GraphRecord.builder().add_nodes(("0", {})).add_group("group")

        assert len(builder._groups) == 1

        graphrecord = builder.build()

        assert len(graphrecord.nodes) == 1
        assert len(graphrecord.edges) == 0
        assert len(graphrecord.groups) == 1
        assert graphrecord.groups[0] == "group"
        assert graphrecord.nodes_in_group("group") == []

        # Test adding a group twice. The second call should overwrite the first.
        builder = (
            gr.GraphRecord.builder()
            .add_nodes(("0", {}))
            .add_group("group", nodes=["0"])
            .add_group("group")
        )

        assert len(builder._groups) == 1

        graphrecord = builder.build()

        assert len(graphrecord.nodes) == 1
        assert len(graphrecord.edges) == 0
        assert len(graphrecord.groups) == 1
        assert graphrecord.groups[0] == "group"
        assert graphrecord.nodes_in_group("group") == []

    def test_with_schema(self) -> None:
        schema = gr.Schema(
            ungrouped=gr.GroupSchema(nodes={"attribute": gr.Int()}),
            schema_type=gr.SchemaType.Provided,
        )

        graphrecord = gr.GraphRecord.builder().with_schema(schema).build()

        graphrecord.add_nodes(("node1", {"attribute": 1}))

        with pytest.raises(
            ValueError,
            match=r"Attribute attribute of node with index node2 is of type String\. Expected Int\.",
        ):
            graphrecord.add_nodes(("node2", {"attribute": "1"}))

    def test_with_plugins(self) -> None:
        plugin = RecordingPlugin()

        graphrecord = (
            gr.GraphRecord.builder().with_plugins({"recorder": plugin}).build()
        )

        assert graphrecord.plugins == ["recorder"]
        assert "initialize" in plugin.calls

    def test_add_plugin(self) -> None:
        plugin = RecordingPlugin()

        graphrecord = (
            gr.GraphRecord.builder()
            .add_plugin("recorder", plugin)
            .add_nodes([("a", {})])
            .build()
        )

        assert graphrecord.plugins == ["recorder"]
        assert "initialize" in plugin.calls
        assert "pre_add_nodes" in plugin.calls

    def test_with_plugins_hooks_fire(self) -> None:
        plugin = RecordingPlugin()

        graphrecord = (
            gr.GraphRecord.builder()
            .with_plugins({"recorder": plugin})
            .add_nodes([("a", {}), ("b", {})])
            .add_nodes([("c", {})], group="node_group")
            .add_edges([("a", "b", {})])
            .add_edges([("b", "c", {})], group="edge_group")
            .add_group("empty_group")
            .with_schema(
                gr.Schema(
                    groups={
                        "node_group": gr.GroupSchema(),
                        "edge_group": gr.GroupSchema(),
                        "empty_group": gr.GroupSchema(),
                    },
                )
            )
            .build()
        )

        assert "pre_add_nodes" in plugin.calls
        assert "post_add_nodes" in plugin.calls
        assert "pre_add_nodes_with_group" in plugin.calls
        assert "post_add_nodes_with_group" in plugin.calls
        assert "pre_add_edges" in plugin.calls
        assert "post_add_edges" in plugin.calls
        assert "pre_add_edges_with_group" in plugin.calls
        assert "post_add_edges_with_group" in plugin.calls
        assert "pre_add_group" in plugin.calls
        assert "post_add_group" in plugin.calls
        assert "pre_set_schema" in plugin.calls
        assert "post_set_schema" in plugin.calls

        assert len(graphrecord.nodes) == 3
        assert len(graphrecord.edges) == 2

    def test_add_nodes_bypass_plugins(self) -> None:
        plugin = RecordingPlugin()

        graphrecord = (
            gr.GraphRecord.builder()
            .with_plugins({"recorder": plugin})
            .add_nodes([("a", {})], bypass_plugins=True)
            .add_nodes([("b", {})])
            .build()
        )

        assert plugin.calls.count("pre_add_nodes") == 1
        assert plugin.calls.count("post_add_nodes") == 1
        assert len(graphrecord.nodes) == 2

    def test_add_edges_bypass_plugins(self) -> None:
        plugin = RecordingPlugin()

        graphrecord = (
            gr.GraphRecord.builder()
            .with_plugins({"recorder": plugin})
            .add_nodes([("a", {}), ("b", {}), ("c", {})])
            .add_edges([("a", "b", {})], bypass_plugins=True)
            .add_edges([("b", "c", {})])
            .build()
        )

        assert plugin.calls.count("pre_add_edges") == 1
        assert plugin.calls.count("post_add_edges") == 1
        assert len(graphrecord.edges) == 2

    def test_add_group_bypass_plugins(self) -> None:
        plugin = RecordingPlugin()

        graphrecord = (
            gr.GraphRecord.builder()
            .with_plugins({"recorder": plugin})
            .add_nodes([("a", {})])
            .add_group("group", nodes=["a"], bypass_plugins=True)
            .build()
        )

        assert "pre_add_group" not in plugin.calls
        assert "post_add_group" not in plugin.calls
        assert graphrecord.nodes_in_group("group") == ["a"]

    def test_with_schema_bypass_plugins(self) -> None:
        plugin = RecordingPlugin()

        schema = gr.Schema(
            ungrouped=gr.GroupSchema(nodes={"attribute": gr.Int()}),
            schema_type=gr.SchemaType.Provided,
        )

        graphrecord = (
            gr.GraphRecord.builder()
            .with_plugins({"recorder": plugin})
            .with_schema(schema, bypass_plugins=True)
            .build()
        )

        assert "pre_set_schema" not in plugin.calls
        assert "post_set_schema" not in plugin.calls
        assert graphrecord.get_schema() is not None

    def test_add_group_already_exists_from_nodes(self) -> None:
        graphrecord = (
            gr.GraphRecord.builder()
            .add_nodes(("a", {}), group="group")
            .add_nodes(("b", {}))
            .add_group("group", nodes=["b"])
            .build()
        )

        assert sorted(graphrecord.nodes_in_group("group")) == ["a", "b"]

    def test_add_group_already_exists_from_edges(self) -> None:
        graphrecord = (
            gr.GraphRecord.builder()
            .add_nodes([("a", {}), ("b", {}), ("c", {})])
            .add_edges([("a", "b", {})], group="group")
            .add_edges([("b", "c", {})])
            .build()
        )

        assert graphrecord.edges_in_group("group") == [0]

    def test_add_group_skips_existing_nodes(self) -> None:
        graphrecord = (
            gr.GraphRecord.builder()
            .add_nodes(("a", {}), group="group")
            .add_group("group", nodes=["a"])
            .build()
        )

        assert graphrecord.nodes_in_group("group") == ["a"]


if __name__ == "__main__":
    run_test = unittest.TestLoader().loadTestsFromTestCase(TestGraphRecordBuilder)
    unittest.TextTestRunner(verbosity=2).run(run_test)
