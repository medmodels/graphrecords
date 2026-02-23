import unittest
from typing import List

import polars as pl

from graphrecords import GraphRecord
from graphrecords.plugins import (
    Plugin,
    PostAddEdgeContext,
    PostAddEdgesContext,
    PostAddEdgesDataframesContext,
    PostAddEdgesDataframesWithGroupContext,
    PostAddEdgesWithGroupContext,
    PostAddEdgeToGroupContext,
    PostAddEdgeWithGroupContext,
    PostAddGroupContext,
    PostAddNodeContext,
    PostAddNodesContext,
    PostAddNodesDataframesContext,
    PostAddNodesDataframesWithGroupContext,
    PostAddNodesWithGroupContext,
    PostAddNodeToGroupContext,
    PostAddNodeWithGroupContext,
    PostRemoveEdgeContext,
    PostRemoveEdgeFromGroupContext,
    PostRemoveGroupContext,
    PostRemoveNodeContext,
    PostRemoveNodeFromGroupContext,
    PreAddEdgeContext,
    PreAddEdgesContext,
    PreAddEdgesDataframesContext,
    PreAddEdgesDataframesWithGroupContext,
    PreAddEdgesWithGroupContext,
    PreAddEdgeToGroupContext,
    PreAddEdgeWithGroupContext,
    PreAddGroupContext,
    PreAddNodeContext,
    PreAddNodesContext,
    PreAddNodesDataframesContext,
    PreAddNodesDataframesWithGroupContext,
    PreAddNodesWithGroupContext,
    PreAddNodeToGroupContext,
    PreAddNodeWithGroupContext,
    PreRemoveEdgeContext,
    PreRemoveEdgeFromGroupContext,
    PreRemoveGroupContext,
    PreRemoveNodeContext,
    PreRemoveNodeFromGroupContext,
    PreSetSchemaContext,
    _PluginBridge,
)
from graphrecords.schema import Schema


class RecordingPlugin(Plugin):
    def __init__(self) -> None:
        self.calls: List[str] = []

    def initialize(self, graphrecord: GraphRecord) -> None:
        self.calls.append("initialize")

    def pre_set_schema(
        self, graphrecord: GraphRecord, context: PreSetSchemaContext
    ) -> PreSetSchemaContext:
        self.calls.append("pre_set_schema")
        return context

    def post_set_schema(self, graphrecord: GraphRecord) -> None:
        self.calls.append("post_set_schema")

    def pre_freeze_schema(self, graphrecord: GraphRecord) -> None:
        self.calls.append("pre_freeze_schema")

    def post_freeze_schema(self, graphrecord: GraphRecord) -> None:
        self.calls.append("post_freeze_schema")

    def pre_unfreeze_schema(self, graphrecord: GraphRecord) -> None:
        self.calls.append("pre_unfreeze_schema")

    def post_unfreeze_schema(self, graphrecord: GraphRecord) -> None:
        self.calls.append("post_unfreeze_schema")

    def pre_add_node(
        self, graphrecord: GraphRecord, context: PreAddNodeContext
    ) -> PreAddNodeContext:
        self.calls.append("pre_add_node")
        return context

    def post_add_node(
        self, graphrecord: GraphRecord, context: PostAddNodeContext
    ) -> None:
        self.calls.append("post_add_node")

    def pre_add_node_with_group(
        self, graphrecord: GraphRecord, context: PreAddNodeWithGroupContext
    ) -> PreAddNodeWithGroupContext:
        self.calls.append("pre_add_node_with_group")
        return context

    def post_add_node_with_group(
        self, graphrecord: GraphRecord, context: PostAddNodeWithGroupContext
    ) -> None:
        self.calls.append("post_add_node_with_group")

    def pre_remove_node(
        self, graphrecord: GraphRecord, context: PreRemoveNodeContext
    ) -> PreRemoveNodeContext:
        self.calls.append("pre_remove_node")
        return context

    def post_remove_node(
        self, graphrecord: GraphRecord, context: PostRemoveNodeContext
    ) -> None:
        self.calls.append("post_remove_node")

    def pre_add_nodes(
        self, graphrecord: GraphRecord, context: PreAddNodesContext
    ) -> PreAddNodesContext:
        self.calls.append("pre_add_nodes")
        return context

    def post_add_nodes(
        self, graphrecord: GraphRecord, context: PostAddNodesContext
    ) -> None:
        self.calls.append("post_add_nodes")

    def pre_add_nodes_with_group(
        self, graphrecord: GraphRecord, context: PreAddNodesWithGroupContext
    ) -> PreAddNodesWithGroupContext:
        self.calls.append("pre_add_nodes_with_group")
        return context

    def post_add_nodes_with_group(
        self, graphrecord: GraphRecord, context: PostAddNodesWithGroupContext
    ) -> None:
        self.calls.append("post_add_nodes_with_group")

    def pre_add_nodes_dataframes(
        self, graphrecord: GraphRecord, context: PreAddNodesDataframesContext
    ) -> PreAddNodesDataframesContext:
        self.calls.append("pre_add_nodes_dataframes")
        return context

    def post_add_nodes_dataframes(
        self, graphrecord: GraphRecord, context: PostAddNodesDataframesContext
    ) -> None:
        self.calls.append("post_add_nodes_dataframes")

    def pre_add_nodes_dataframes_with_group(
        self,
        graphrecord: GraphRecord,
        context: PreAddNodesDataframesWithGroupContext,
    ) -> PreAddNodesDataframesWithGroupContext:
        self.calls.append("pre_add_nodes_dataframes_with_group")
        return context

    def post_add_nodes_dataframes_with_group(
        self,
        graphrecord: GraphRecord,
        context: PostAddNodesDataframesWithGroupContext,
    ) -> None:
        self.calls.append("post_add_nodes_dataframes_with_group")

    def pre_add_edge(
        self, graphrecord: GraphRecord, context: PreAddEdgeContext
    ) -> PreAddEdgeContext:
        self.calls.append("pre_add_edge")
        return context

    def post_add_edge(
        self, graphrecord: GraphRecord, context: PostAddEdgeContext
    ) -> None:
        self.calls.append("post_add_edge")

    def pre_add_edge_with_group(
        self, graphrecord: GraphRecord, context: PreAddEdgeWithGroupContext
    ) -> PreAddEdgeWithGroupContext:
        self.calls.append("pre_add_edge_with_group")
        return context

    def post_add_edge_with_group(
        self, graphrecord: GraphRecord, context: PostAddEdgeWithGroupContext
    ) -> None:
        self.calls.append("post_add_edge_with_group")

    def pre_remove_edge(
        self, graphrecord: GraphRecord, context: PreRemoveEdgeContext
    ) -> PreRemoveEdgeContext:
        self.calls.append("pre_remove_edge")
        return context

    def post_remove_edge(
        self, graphrecord: GraphRecord, context: PostRemoveEdgeContext
    ) -> None:
        self.calls.append("post_remove_edge")

    def pre_add_edges(
        self, graphrecord: GraphRecord, context: PreAddEdgesContext
    ) -> PreAddEdgesContext:
        self.calls.append("pre_add_edges")
        return context

    def post_add_edges(
        self, graphrecord: GraphRecord, context: PostAddEdgesContext
    ) -> None:
        self.calls.append("post_add_edges")

    def pre_add_edges_with_group(
        self, graphrecord: GraphRecord, context: PreAddEdgesWithGroupContext
    ) -> PreAddEdgesWithGroupContext:
        self.calls.append("pre_add_edges_with_group")
        return context

    def post_add_edges_with_group(
        self, graphrecord: GraphRecord, context: PostAddEdgesWithGroupContext
    ) -> None:
        self.calls.append("post_add_edges_with_group")

    def pre_add_edges_dataframes(
        self, graphrecord: GraphRecord, context: PreAddEdgesDataframesContext
    ) -> PreAddEdgesDataframesContext:
        self.calls.append("pre_add_edges_dataframes")
        return context

    def post_add_edges_dataframes(
        self, graphrecord: GraphRecord, context: PostAddEdgesDataframesContext
    ) -> None:
        self.calls.append("post_add_edges_dataframes")

    def pre_add_edges_dataframes_with_group(
        self,
        graphrecord: GraphRecord,
        context: PreAddEdgesDataframesWithGroupContext,
    ) -> PreAddEdgesDataframesWithGroupContext:
        self.calls.append("pre_add_edges_dataframes_with_group")
        return context

    def post_add_edges_dataframes_with_group(
        self,
        graphrecord: GraphRecord,
        context: PostAddEdgesDataframesWithGroupContext,
    ) -> None:
        self.calls.append("post_add_edges_dataframes_with_group")

    def pre_add_group(
        self, graphrecord: GraphRecord, context: PreAddGroupContext
    ) -> PreAddGroupContext:
        self.calls.append("pre_add_group")
        return context

    def post_add_group(
        self, graphrecord: GraphRecord, context: PostAddGroupContext
    ) -> None:
        self.calls.append("post_add_group")

    def pre_remove_group(
        self, graphrecord: GraphRecord, context: PreRemoveGroupContext
    ) -> PreRemoveGroupContext:
        self.calls.append("pre_remove_group")
        return context

    def post_remove_group(
        self, graphrecord: GraphRecord, context: PostRemoveGroupContext
    ) -> None:
        self.calls.append("post_remove_group")

    def pre_add_node_to_group(
        self, graphrecord: GraphRecord, context: PreAddNodeToGroupContext
    ) -> PreAddNodeToGroupContext:
        self.calls.append("pre_add_node_to_group")
        return context

    def post_add_node_to_group(
        self, graphrecord: GraphRecord, context: PostAddNodeToGroupContext
    ) -> None:
        self.calls.append("post_add_node_to_group")

    def pre_add_edge_to_group(
        self, graphrecord: GraphRecord, context: PreAddEdgeToGroupContext
    ) -> PreAddEdgeToGroupContext:
        self.calls.append("pre_add_edge_to_group")
        return context

    def post_add_edge_to_group(
        self, graphrecord: GraphRecord, context: PostAddEdgeToGroupContext
    ) -> None:
        self.calls.append("post_add_edge_to_group")

    def pre_remove_node_from_group(
        self, graphrecord: GraphRecord, context: PreRemoveNodeFromGroupContext
    ) -> PreRemoveNodeFromGroupContext:
        self.calls.append("pre_remove_node_from_group")
        return context

    def post_remove_node_from_group(
        self, graphrecord: GraphRecord, context: PostRemoveNodeFromGroupContext
    ) -> None:
        self.calls.append("post_remove_node_from_group")

    def pre_remove_edge_from_group(
        self, graphrecord: GraphRecord, context: PreRemoveEdgeFromGroupContext
    ) -> PreRemoveEdgeFromGroupContext:
        self.calls.append("pre_remove_edge_from_group")
        return context

    def post_remove_edge_from_group(
        self, graphrecord: GraphRecord, context: PostRemoveEdgeFromGroupContext
    ) -> None:
        self.calls.append("post_remove_edge_from_group")

    def pre_clear(self, graphrecord: GraphRecord) -> None:
        self.calls.append("pre_clear")

    def post_clear(self, graphrecord: GraphRecord) -> None:
        self.calls.append("post_clear")


class TestContextConstruction(unittest.TestCase):
    def test_pre_set_schema_context(self) -> None:
        context = PreSetSchemaContext(Schema())

        assert isinstance(context.schema, Schema)

    def test_pre_add_node_context(self) -> None:
        context = PreAddNodeContext("a", {"x": 1})

        assert context.node_index == "a"
        assert context.attributes == {"x": 1}

    def test_post_add_node_context(self) -> None:
        context = PostAddNodeContext("a")

        assert context.node_index == "a"

    def test_pre_add_node_with_group_context(self) -> None:
        context = PreAddNodeWithGroupContext("a", {"x": 1}, "g")

        assert context.node_index == "a"
        assert context.attributes == {"x": 1}
        assert context.group == "g"

    def test_post_add_node_with_group_context(self) -> None:
        context = PostAddNodeWithGroupContext("a", "g")

        assert context.node_index == "a"
        assert context.group == "g"

    def test_pre_remove_node_context(self) -> None:
        context = PreRemoveNodeContext("a")

        assert context.node_index == "a"

    def test_post_remove_node_context(self) -> None:
        context = PostRemoveNodeContext("a")

        assert context.node_index == "a"

    def test_pre_add_nodes_context(self) -> None:
        context = PreAddNodesContext([("a", {"x": 1})])

        assert context.nodes == [("a", {"x": 1})]

    def test_post_add_nodes_context(self) -> None:
        context = PostAddNodesContext([("a", {"x": 1})])

        assert context.nodes == [("a", {"x": 1})]

    def test_pre_add_nodes_with_group_context(self) -> None:
        context = PreAddNodesWithGroupContext([("a", {"x": 1})], "g")

        assert context.nodes == [("a", {"x": 1})]
        assert context.group == "g"

    def test_post_add_nodes_with_group_context(self) -> None:
        context = PostAddNodesWithGroupContext([("a", {"x": 1})], "g")

        assert context.nodes == [("a", {"x": 1})]
        assert context.group == "g"

    def test_pre_add_nodes_dataframes_context(self) -> None:
        dataframe = pl.DataFrame({"idx": ["a"], "val": [1]})
        context = PreAddNodesDataframesContext([(dataframe, "idx")])

        assert len(context.nodes_dataframes) == 1

    def test_post_add_nodes_dataframes_context(self) -> None:
        dataframe = pl.DataFrame({"idx": ["a"], "val": [1]})
        context = PostAddNodesDataframesContext([(dataframe, "idx")])

        assert len(context.nodes_dataframes) == 1

    def test_pre_add_nodes_dataframes_with_group_context(self) -> None:
        dataframe = pl.DataFrame({"idx": ["a"], "val": [1]})
        context = PreAddNodesDataframesWithGroupContext([(dataframe, "idx")], "g")

        assert len(context.nodes_dataframes) == 1
        assert context.group == "g"

    def test_post_add_nodes_dataframes_with_group_context(self) -> None:
        dataframe = pl.DataFrame({"idx": ["a"], "val": [1]})
        context = PostAddNodesDataframesWithGroupContext([(dataframe, "idx")], "g")

        assert len(context.nodes_dataframes) == 1
        assert context.group == "g"

    def test_pre_add_edge_context(self) -> None:
        context = PreAddEdgeContext("a", "b", {"w": 1})

        assert context.source_node_index == "a"
        assert context.target_node_index == "b"
        assert context.attributes == {"w": 1}

    def test_post_add_edge_context(self) -> None:
        context = PostAddEdgeContext(0)

        assert context.edge_index == 0

    def test_pre_add_edge_with_group_context(self) -> None:
        context = PreAddEdgeWithGroupContext("a", "b", {"w": 1}, "g")

        assert context.source_node_index == "a"
        assert context.target_node_index == "b"
        assert context.attributes == {"w": 1}
        assert context.group == "g"

    def test_post_add_edge_with_group_context(self) -> None:
        context = PostAddEdgeWithGroupContext(0)

        assert context.edge_index == 0

    def test_pre_remove_edge_context(self) -> None:
        context = PreRemoveEdgeContext(0)

        assert context.edge_index == 0

    def test_post_remove_edge_context(self) -> None:
        context = PostRemoveEdgeContext(0)

        assert context.edge_index == 0

    def test_pre_add_edges_context(self) -> None:
        context = PreAddEdgesContext([("a", "b", {"w": 1})])

        assert context.edges == [("a", "b", {"w": 1})]

    def test_post_add_edges_context(self) -> None:
        context = PostAddEdgesContext([0])

        assert context.edge_indices == [0]

    def test_pre_add_edges_with_group_context(self) -> None:
        context = PreAddEdgesWithGroupContext([("a", "b", {})], "g")

        assert context.edges == [("a", "b", {})]
        assert context.group == "g"

    def test_post_add_edges_with_group_context(self) -> None:
        context = PostAddEdgesWithGroupContext([0])

        assert context.edge_indices == [0]

    def test_pre_add_edges_dataframes_context(self) -> None:
        dataframe = pl.DataFrame({"src": ["a"], "tgt": ["b"]})
        context = PreAddEdgesDataframesContext([(dataframe, "src", "tgt")])

        assert len(context.edges_dataframes) == 1

    def test_post_add_edges_dataframes_context(self) -> None:
        dataframe = pl.DataFrame({"src": ["a"], "tgt": ["b"]})
        context = PostAddEdgesDataframesContext([(dataframe, "src", "tgt")])

        assert len(context.edges_dataframes) == 1

    def test_pre_add_edges_dataframes_with_group_context(self) -> None:
        dataframe = pl.DataFrame({"src": ["a"], "tgt": ["b"]})
        context = PreAddEdgesDataframesWithGroupContext(
            [(dataframe, "src", "tgt")], "g"
        )

        assert len(context.edges_dataframes) == 1
        assert context.group == "g"

    def test_post_add_edges_dataframes_with_group_context(self) -> None:
        dataframe = pl.DataFrame({"src": ["a"], "tgt": ["b"]})
        context = PostAddEdgesDataframesWithGroupContext(
            [(dataframe, "src", "tgt")], "g"
        )

        assert len(context.edges_dataframes) == 1
        assert context.group == "g"

    def test_pre_add_group_context(self) -> None:
        context = PreAddGroupContext("g", ["a"], [0])

        assert context.group == "g"
        assert context.node_indices == ["a"]
        assert context.edge_indices == [0]

    def test_pre_add_group_context_none_optionals(self) -> None:
        context = PreAddGroupContext("g", None, None)

        assert context.group == "g"
        assert context.node_indices is None
        assert context.edge_indices is None

    def test_post_add_group_context(self) -> None:
        context = PostAddGroupContext("g", ["a"], [0])

        assert context.group == "g"
        assert context.node_indices == ["a"]
        assert context.edge_indices == [0]

    def test_post_add_group_context_none_optionals(self) -> None:
        context = PostAddGroupContext("g", None, None)

        assert context.group == "g"
        assert context.node_indices is None
        assert context.edge_indices is None

    def test_pre_remove_group_context(self) -> None:
        context = PreRemoveGroupContext("g")

        assert context.group == "g"

    def test_post_remove_group_context(self) -> None:
        context = PostRemoveGroupContext("g")

        assert context.group == "g"

    def test_pre_add_node_to_group_context(self) -> None:
        context = PreAddNodeToGroupContext("g", "a")

        assert context.group == "g"
        assert context.node_index == "a"

    def test_post_add_node_to_group_context(self) -> None:
        context = PostAddNodeToGroupContext("g", "a")

        assert context.group == "g"
        assert context.node_index == "a"

    def test_pre_add_edge_to_group_context(self) -> None:
        context = PreAddEdgeToGroupContext("g", 0)

        assert context.group == "g"
        assert context.edge_index == 0

    def test_post_add_edge_to_group_context(self) -> None:
        context = PostAddEdgeToGroupContext("g", 0)

        assert context.group == "g"
        assert context.edge_index == 0

    def test_pre_remove_node_from_group_context(self) -> None:
        context = PreRemoveNodeFromGroupContext("g", "a")

        assert context.group == "g"
        assert context.node_index == "a"

    def test_post_remove_node_from_group_context(self) -> None:
        context = PostRemoveNodeFromGroupContext("g", "a")

        assert context.group == "g"
        assert context.node_index == "a"

    def test_pre_remove_edge_from_group_context(self) -> None:
        context = PreRemoveEdgeFromGroupContext("g", 0)

        assert context.group == "g"
        assert context.edge_index == 0

    def test_post_remove_edge_from_group_context(self) -> None:
        context = PostRemoveEdgeFromGroupContext("g", 0)

        assert context.group == "g"
        assert context.edge_index == 0


class TestContextFromPyContext(unittest.TestCase):
    def test_pre_set_schema_from_py_context(self) -> None:
        original = PreSetSchemaContext(Schema())

        reconstructed = PreSetSchemaContext._from_py_pre_set_schema_context(
            original._py_pre_set_schema_context
        )

        assert isinstance(reconstructed.schema, Schema)

    def test_pre_add_node_from_py_context(self) -> None:
        original = PreAddNodeContext("a", {"x": 1})

        reconstructed = PreAddNodeContext._from_py_context(original._py_context)

        assert reconstructed.node_index == "a"
        assert reconstructed.attributes == {"x": 1}

    def test_post_add_node_from_py_context(self) -> None:
        original = PostAddNodeContext("a")

        reconstructed = PostAddNodeContext._from_py_context(original._py_context)

        assert reconstructed.node_index == "a"

    def test_pre_add_node_with_group_from_py_context(self) -> None:
        original = PreAddNodeWithGroupContext("a", {"x": 1}, "g")

        reconstructed = PreAddNodeWithGroupContext._from_py_context(
            original._py_context
        )

        assert reconstructed.node_index == "a"
        assert reconstructed.attributes == {"x": 1}
        assert reconstructed.group == "g"

    def test_post_add_node_with_group_from_py_context(self) -> None:
        original = PostAddNodeWithGroupContext("a", "g")

        reconstructed = PostAddNodeWithGroupContext._from_py_context(
            original._py_context
        )

        assert reconstructed.node_index == "a"
        assert reconstructed.group == "g"

    def test_pre_remove_node_from_py_context(self) -> None:
        original = PreRemoveNodeContext("a")

        reconstructed = PreRemoveNodeContext._from_py_context(original._py_context)

        assert reconstructed.node_index == "a"

    def test_post_remove_node_from_py_context(self) -> None:
        original = PostRemoveNodeContext("a")

        reconstructed = PostRemoveNodeContext._from_py_context(original._py_context)

        assert reconstructed.node_index == "a"

    def test_pre_add_nodes_from_py_context(self) -> None:
        original = PreAddNodesContext([("a", {"x": 1})])

        reconstructed = PreAddNodesContext._from_py_context(original._py_context)

        assert reconstructed.nodes == [("a", {"x": 1})]

    def test_post_add_nodes_from_py_context(self) -> None:
        original = PostAddNodesContext([("a", {"x": 1})])

        reconstructed = PostAddNodesContext._from_py_context(original._py_context)

        assert reconstructed.nodes == [("a", {"x": 1})]

    def test_pre_add_nodes_with_group_from_py_context(self) -> None:
        original = PreAddNodesWithGroupContext([("a", {})], "g")

        reconstructed = PreAddNodesWithGroupContext._from_py_context(
            original._py_context
        )

        assert reconstructed.nodes == [("a", {})]
        assert reconstructed.group == "g"

    def test_post_add_nodes_with_group_from_py_context(self) -> None:
        original = PostAddNodesWithGroupContext([("a", {})], "g")

        reconstructed = PostAddNodesWithGroupContext._from_py_context(
            original._py_context
        )

        assert reconstructed.nodes == [("a", {})]
        assert reconstructed.group == "g"

    def test_pre_add_nodes_dataframes_from_py_context(self) -> None:
        dataframe = pl.DataFrame({"idx": ["a"], "val": [1]})
        original = PreAddNodesDataframesContext([(dataframe, "idx")])

        reconstructed = PreAddNodesDataframesContext._from_py_context(
            original._py_context
        )

        assert len(reconstructed.nodes_dataframes) == 1

    def test_post_add_nodes_dataframes_from_py_context(self) -> None:
        dataframe = pl.DataFrame({"idx": ["a"], "val": [1]})
        original = PostAddNodesDataframesContext([(dataframe, "idx")])

        reconstructed = PostAddNodesDataframesContext._from_py_context(
            original._py_context
        )

        assert len(reconstructed.nodes_dataframes) == 1

    def test_pre_add_nodes_dataframes_with_group_from_py_context(self) -> None:
        dataframe = pl.DataFrame({"idx": ["a"], "val": [1]})
        original = PreAddNodesDataframesWithGroupContext([(dataframe, "idx")], "g")

        reconstructed = PreAddNodesDataframesWithGroupContext._from_py_context(
            original._py_context
        )

        assert len(reconstructed.nodes_dataframes) == 1
        assert reconstructed.group == "g"

    def test_post_add_nodes_dataframes_with_group_from_py_context(self) -> None:
        dataframe = pl.DataFrame({"idx": ["a"], "val": [1]})
        original = PostAddNodesDataframesWithGroupContext([(dataframe, "idx")], "g")

        reconstructed = PostAddNodesDataframesWithGroupContext._from_py_context(
            original._py_context
        )

        assert len(reconstructed.nodes_dataframes) == 1
        assert reconstructed.group == "g"

    def test_pre_add_edge_from_py_context(self) -> None:
        original = PreAddEdgeContext("a", "b", {"w": 1})

        reconstructed = PreAddEdgeContext._from_py_context(original._py_context)

        assert reconstructed.source_node_index == "a"
        assert reconstructed.target_node_index == "b"
        assert reconstructed.attributes == {"w": 1}

    def test_post_add_edge_from_py_context(self) -> None:
        original = PostAddEdgeContext(0)

        reconstructed = PostAddEdgeContext._from_py_context(original._py_context)

        assert reconstructed.edge_index == 0

    def test_pre_add_edge_with_group_from_py_context(self) -> None:
        original = PreAddEdgeWithGroupContext("a", "b", {"w": 1}, "g")

        reconstructed = PreAddEdgeWithGroupContext._from_py_context(
            original._py_context
        )

        assert reconstructed.source_node_index == "a"
        assert reconstructed.target_node_index == "b"
        assert reconstructed.attributes == {"w": 1}
        assert reconstructed.group == "g"

    def test_post_add_edge_with_group_from_py_context(self) -> None:
        original = PostAddEdgeWithGroupContext(0)

        reconstructed = PostAddEdgeWithGroupContext._from_py_context(
            original._py_context
        )

        assert reconstructed.edge_index == 0

    def test_pre_remove_edge_from_py_context(self) -> None:
        original = PreRemoveEdgeContext(0)

        reconstructed = PreRemoveEdgeContext._from_py_context(original._py_context)

        assert reconstructed.edge_index == 0

    def test_post_remove_edge_from_py_context(self) -> None:
        original = PostRemoveEdgeContext(0)

        reconstructed = PostRemoveEdgeContext._from_py_context(original._py_context)

        assert reconstructed.edge_index == 0

    def test_pre_add_edges_from_py_context(self) -> None:
        original = PreAddEdgesContext([("a", "b", {})])

        reconstructed = PreAddEdgesContext._from_py_context(original._py_context)

        assert reconstructed.edges == [("a", "b", {})]

    def test_post_add_edges_from_py_context(self) -> None:
        original = PostAddEdgesContext([0])

        reconstructed = PostAddEdgesContext._from_py_context(original._py_context)

        assert reconstructed.edge_indices == [0]

    def test_pre_add_edges_with_group_from_py_context(self) -> None:
        original = PreAddEdgesWithGroupContext([("a", "b", {})], "g")

        reconstructed = PreAddEdgesWithGroupContext._from_py_context(
            original._py_context
        )

        assert reconstructed.edges == [("a", "b", {})]
        assert reconstructed.group == "g"

    def test_post_add_edges_with_group_from_py_context(self) -> None:
        original = PostAddEdgesWithGroupContext([0])

        reconstructed = PostAddEdgesWithGroupContext._from_py_context(
            original._py_context
        )

        assert reconstructed.edge_indices == [0]

    def test_pre_add_edges_dataframes_from_py_context(self) -> None:
        dataframe = pl.DataFrame({"src": ["a"], "tgt": ["b"]})
        original = PreAddEdgesDataframesContext([(dataframe, "src", "tgt")])

        reconstructed = PreAddEdgesDataframesContext._from_py_context(
            original._py_context
        )

        assert len(reconstructed.edges_dataframes) == 1

    def test_post_add_edges_dataframes_from_py_context(self) -> None:
        dataframe = pl.DataFrame({"src": ["a"], "tgt": ["b"]})
        original = PostAddEdgesDataframesContext([(dataframe, "src", "tgt")])

        reconstructed = PostAddEdgesDataframesContext._from_py_context(
            original._py_context
        )

        assert len(reconstructed.edges_dataframes) == 1

    def test_pre_add_edges_dataframes_with_group_from_py_context(self) -> None:
        dataframe = pl.DataFrame({"src": ["a"], "tgt": ["b"]})
        original = PreAddEdgesDataframesWithGroupContext(
            [(dataframe, "src", "tgt")], "g"
        )

        reconstructed = PreAddEdgesDataframesWithGroupContext._from_py_context(
            original._py_context
        )

        assert len(reconstructed.edges_dataframes) == 1
        assert reconstructed.group == "g"

    def test_post_add_edges_dataframes_with_group_from_py_context(self) -> None:
        dataframe = pl.DataFrame({"src": ["a"], "tgt": ["b"]})
        original = PostAddEdgesDataframesWithGroupContext(
            [(dataframe, "src", "tgt")], "g"
        )

        reconstructed = PostAddEdgesDataframesWithGroupContext._from_py_context(
            original._py_context
        )

        assert len(reconstructed.edges_dataframes) == 1
        assert reconstructed.group == "g"

    def test_pre_add_group_from_py_context(self) -> None:
        original = PreAddGroupContext("g", ["a"], [0])

        reconstructed = PreAddGroupContext._from_py_context(original._py_context)

        assert reconstructed.group == "g"
        assert reconstructed.node_indices == ["a"]
        assert reconstructed.edge_indices == [0]

    def test_post_add_group_from_py_context(self) -> None:
        original = PostAddGroupContext("g", ["a"], [0])

        reconstructed = PostAddGroupContext._from_py_context(original._py_context)

        assert reconstructed.group == "g"
        assert reconstructed.node_indices == ["a"]
        assert reconstructed.edge_indices == [0]

    def test_pre_remove_group_from_py_context(self) -> None:
        original = PreRemoveGroupContext("g")

        reconstructed = PreRemoveGroupContext._from_py_context(original._py_context)

        assert reconstructed.group == "g"

    def test_post_remove_group_from_py_context(self) -> None:
        original = PostRemoveGroupContext("g")

        reconstructed = PostRemoveGroupContext._from_py_context(original._py_context)

        assert reconstructed.group == "g"

    def test_pre_add_node_to_group_from_py_context(self) -> None:
        original = PreAddNodeToGroupContext("g", "a")

        reconstructed = PreAddNodeToGroupContext._from_py_context(original._py_context)

        assert reconstructed.group == "g"
        assert reconstructed.node_index == "a"

    def test_post_add_node_to_group_from_py_context(self) -> None:
        original = PostAddNodeToGroupContext("g", "a")

        reconstructed = PostAddNodeToGroupContext._from_py_context(original._py_context)

        assert reconstructed.group == "g"
        assert reconstructed.node_index == "a"

    def test_pre_add_edge_to_group_from_py_context(self) -> None:
        original = PreAddEdgeToGroupContext("g", 0)

        reconstructed = PreAddEdgeToGroupContext._from_py_context(original._py_context)

        assert reconstructed.group == "g"
        assert reconstructed.edge_index == 0

    def test_post_add_edge_to_group_from_py_context(self) -> None:
        original = PostAddEdgeToGroupContext("g", 0)

        reconstructed = PostAddEdgeToGroupContext._from_py_context(original._py_context)

        assert reconstructed.group == "g"
        assert reconstructed.edge_index == 0

    def test_pre_remove_node_from_group_from_py_context(self) -> None:
        original = PreRemoveNodeFromGroupContext("g", "a")

        reconstructed = PreRemoveNodeFromGroupContext._from_py_context(
            original._py_context
        )

        assert reconstructed.group == "g"
        assert reconstructed.node_index == "a"

    def test_post_remove_node_from_group_from_py_context(self) -> None:
        original = PostRemoveNodeFromGroupContext("g", "a")

        reconstructed = PostRemoveNodeFromGroupContext._from_py_context(
            original._py_context
        )

        assert reconstructed.group == "g"
        assert reconstructed.node_index == "a"

    def test_pre_remove_edge_from_group_from_py_context(self) -> None:
        original = PreRemoveEdgeFromGroupContext("g", 0)

        reconstructed = PreRemoveEdgeFromGroupContext._from_py_context(
            original._py_context
        )

        assert reconstructed.group == "g"
        assert reconstructed.edge_index == 0

    def test_post_remove_edge_from_group_from_py_context(self) -> None:
        original = PostRemoveEdgeFromGroupContext("g", 0)

        reconstructed = PostRemoveEdgeFromGroupContext._from_py_context(
            original._py_context
        )

        assert reconstructed.group == "g"
        assert reconstructed.edge_index == 0


class TestPluginBaseDefaults(unittest.TestCase):
    def test_all_default_methods(self) -> None:
        plugin = Plugin()
        graphrecord = GraphRecord()

        plugin.initialize(graphrecord)

        schema_context = PreSetSchemaContext(Schema())
        assert plugin.pre_set_schema(graphrecord, schema_context) is schema_context
        plugin.post_set_schema(graphrecord)

        plugin.pre_freeze_schema(graphrecord)
        plugin.post_freeze_schema(graphrecord)
        plugin.pre_unfreeze_schema(graphrecord)
        plugin.post_unfreeze_schema(graphrecord)

        pre_add_node = PreAddNodeContext("a", {})
        assert plugin.pre_add_node(graphrecord, pre_add_node) is pre_add_node
        plugin.post_add_node(graphrecord, PostAddNodeContext("a"))

        pre_add_node_wg = PreAddNodeWithGroupContext("a", {}, "g")
        assert (
            plugin.pre_add_node_with_group(graphrecord, pre_add_node_wg)
            is pre_add_node_wg
        )
        plugin.post_add_node_with_group(
            graphrecord, PostAddNodeWithGroupContext("a", "g")
        )

        pre_remove_node = PreRemoveNodeContext("a")
        assert plugin.pre_remove_node(graphrecord, pre_remove_node) is pre_remove_node
        plugin.post_remove_node(graphrecord, PostRemoveNodeContext("a"))

        pre_add_nodes = PreAddNodesContext([("a", {})])
        assert plugin.pre_add_nodes(graphrecord, pre_add_nodes) is pre_add_nodes
        plugin.post_add_nodes(graphrecord, PostAddNodesContext([("a", {})]))

        pre_add_nodes_wg = PreAddNodesWithGroupContext([("a", {})], "g")
        assert (
            plugin.pre_add_nodes_with_group(graphrecord, pre_add_nodes_wg)
            is pre_add_nodes_wg
        )
        plugin.post_add_nodes_with_group(
            graphrecord, PostAddNodesWithGroupContext([("a", {})], "g")
        )

        dataframe = pl.DataFrame({"idx": ["a"], "val": [1]})

        pre_add_nodes_df = PreAddNodesDataframesContext([(dataframe, "idx")])
        assert (
            plugin.pre_add_nodes_dataframes(graphrecord, pre_add_nodes_df)
            is pre_add_nodes_df
        )
        plugin.post_add_nodes_dataframes(
            graphrecord, PostAddNodesDataframesContext([(dataframe, "idx")])
        )

        pre_add_nodes_df_wg = PreAddNodesDataframesWithGroupContext(
            [(dataframe, "idx")], "g"
        )
        assert (
            plugin.pre_add_nodes_dataframes_with_group(graphrecord, pre_add_nodes_df_wg)
            is pre_add_nodes_df_wg
        )
        plugin.post_add_nodes_dataframes_with_group(
            graphrecord,
            PostAddNodesDataframesWithGroupContext([(dataframe, "idx")], "g"),
        )

        pre_add_edge = PreAddEdgeContext("a", "b", {})
        assert plugin.pre_add_edge(graphrecord, pre_add_edge) is pre_add_edge
        plugin.post_add_edge(graphrecord, PostAddEdgeContext(0))

        pre_add_edge_wg = PreAddEdgeWithGroupContext("a", "b", {}, "g")
        assert (
            plugin.pre_add_edge_with_group(graphrecord, pre_add_edge_wg)
            is pre_add_edge_wg
        )
        plugin.post_add_edge_with_group(graphrecord, PostAddEdgeWithGroupContext(0))

        pre_remove_edge = PreRemoveEdgeContext(0)
        assert plugin.pre_remove_edge(graphrecord, pre_remove_edge) is pre_remove_edge
        plugin.post_remove_edge(graphrecord, PostRemoveEdgeContext(0))

        pre_add_edges = PreAddEdgesContext([("a", "b", {})])
        assert plugin.pre_add_edges(graphrecord, pre_add_edges) is pre_add_edges
        plugin.post_add_edges(graphrecord, PostAddEdgesContext([0]))

        pre_add_edges_wg = PreAddEdgesWithGroupContext([("a", "b", {})], "g")
        assert (
            plugin.pre_add_edges_with_group(graphrecord, pre_add_edges_wg)
            is pre_add_edges_wg
        )
        plugin.post_add_edges_with_group(graphrecord, PostAddEdgesWithGroupContext([0]))

        edge_dataframe = pl.DataFrame({"src": ["a"], "tgt": ["b"]})

        pre_add_edges_df = PreAddEdgesDataframesContext(
            [(edge_dataframe, "src", "tgt")]
        )
        assert (
            plugin.pre_add_edges_dataframes(graphrecord, pre_add_edges_df)
            is pre_add_edges_df
        )
        plugin.post_add_edges_dataframes(
            graphrecord,
            PostAddEdgesDataframesContext([(edge_dataframe, "src", "tgt")]),
        )

        pre_add_edges_df_wg = PreAddEdgesDataframesWithGroupContext(
            [(edge_dataframe, "src", "tgt")], "g"
        )
        assert (
            plugin.pre_add_edges_dataframes_with_group(graphrecord, pre_add_edges_df_wg)
            is pre_add_edges_df_wg
        )
        plugin.post_add_edges_dataframes_with_group(
            graphrecord,
            PostAddEdgesDataframesWithGroupContext(
                [(edge_dataframe, "src", "tgt")], "g"
            ),
        )

        pre_add_group = PreAddGroupContext("g", None, None)
        assert plugin.pre_add_group(graphrecord, pre_add_group) is pre_add_group
        plugin.post_add_group(graphrecord, PostAddGroupContext("g", None, None))

        pre_remove_group = PreRemoveGroupContext("g")
        assert (
            plugin.pre_remove_group(graphrecord, pre_remove_group) is pre_remove_group
        )
        plugin.post_remove_group(graphrecord, PostRemoveGroupContext("g"))

        pre_add_node_to_group = PreAddNodeToGroupContext("g", "a")
        assert (
            plugin.pre_add_node_to_group(graphrecord, pre_add_node_to_group)
            is pre_add_node_to_group
        )
        plugin.post_add_node_to_group(graphrecord, PostAddNodeToGroupContext("g", "a"))

        pre_add_edge_to_group = PreAddEdgeToGroupContext("g", 0)
        assert (
            plugin.pre_add_edge_to_group(graphrecord, pre_add_edge_to_group)
            is pre_add_edge_to_group
        )
        plugin.post_add_edge_to_group(graphrecord, PostAddEdgeToGroupContext("g", 0))

        pre_remove_node_from_group = PreRemoveNodeFromGroupContext("g", "a")
        assert (
            plugin.pre_remove_node_from_group(graphrecord, pre_remove_node_from_group)
            is pre_remove_node_from_group
        )
        plugin.post_remove_node_from_group(
            graphrecord, PostRemoveNodeFromGroupContext("g", "a")
        )

        pre_remove_edge_from_group = PreRemoveEdgeFromGroupContext("g", 0)
        assert (
            plugin.pre_remove_edge_from_group(graphrecord, pre_remove_edge_from_group)
            is pre_remove_edge_from_group
        )
        plugin.post_remove_edge_from_group(
            graphrecord, PostRemoveEdgeFromGroupContext("g", 0)
        )

        plugin.pre_clear(graphrecord)
        plugin.post_clear(graphrecord)


class TestPluginHooksFiring(unittest.TestCase):
    def test_initialize_fires(self) -> None:
        plugin = RecordingPlugin()

        GraphRecord.with_plugins([plugin])

        assert "initialize" in plugin.calls

    def test_set_schema_hooks(self) -> None:
        plugin = RecordingPlugin()
        graphrecord = GraphRecord.with_plugins([plugin])
        plugin.calls.clear()

        graphrecord.set_schema(Schema())

        assert plugin.calls == ["pre_set_schema", "post_set_schema"]

    def test_freeze_schema_hooks(self) -> None:
        plugin = RecordingPlugin()
        graphrecord = GraphRecord.with_plugins([plugin])
        plugin.calls.clear()

        graphrecord.freeze_schema()

        assert plugin.calls == ["pre_freeze_schema", "post_freeze_schema"]

    def test_unfreeze_schema_hooks(self) -> None:
        plugin = RecordingPlugin()
        graphrecord = GraphRecord.with_plugins([plugin])
        graphrecord.freeze_schema()
        plugin.calls.clear()

        graphrecord.unfreeze_schema()

        assert plugin.calls == ["pre_unfreeze_schema", "post_unfreeze_schema"]

    def test_add_nodes_hooks(self) -> None:
        plugin = RecordingPlugin()
        graphrecord = GraphRecord.with_plugins([plugin])
        plugin.calls.clear()

        graphrecord.add_nodes([("a", {"x": 1})])

        assert "pre_add_nodes" in plugin.calls
        assert "post_add_nodes" in plugin.calls

    def test_add_nodes_with_group_hooks(self) -> None:
        plugin = RecordingPlugin()
        graphrecord = GraphRecord.with_plugins([plugin])
        graphrecord.add_group("g")
        plugin.calls.clear()

        graphrecord.add_nodes([("a", {})], group="g")

        assert "pre_add_nodes_with_group" in plugin.calls
        assert "post_add_nodes_with_group" in plugin.calls

    def test_add_nodes_polars_hooks(self) -> None:
        plugin = RecordingPlugin()
        graphrecord = GraphRecord.with_plugins([plugin])
        plugin.calls.clear()

        dataframe = pl.DataFrame({"idx": ["a"], "val": [1]})
        graphrecord.add_nodes_polars([(dataframe, "idx")])

        assert "pre_add_nodes_dataframes" in plugin.calls
        assert "post_add_nodes_dataframes" in plugin.calls

    def test_add_nodes_polars_with_group_hooks(self) -> None:
        plugin = RecordingPlugin()
        graphrecord = GraphRecord.with_plugins([plugin])
        graphrecord.add_group("g")
        plugin.calls.clear()

        dataframe = pl.DataFrame({"idx": ["a"], "val": [1]})
        graphrecord.add_nodes_polars([(dataframe, "idx")], group="g")

        assert "pre_add_nodes_dataframes_with_group" in plugin.calls
        assert "post_add_nodes_dataframes_with_group" in plugin.calls

    def test_remove_node_hooks(self) -> None:
        plugin = RecordingPlugin()
        graphrecord = GraphRecord.with_plugins([plugin])
        graphrecord.add_nodes([("a", {})])
        plugin.calls.clear()

        graphrecord.remove_nodes(["a"])

        assert "pre_remove_node" in plugin.calls
        assert "post_remove_node" in plugin.calls

    def test_add_edges_hooks(self) -> None:
        plugin = RecordingPlugin()
        graphrecord = GraphRecord.with_plugins([plugin])
        graphrecord.add_nodes([("a", {}), ("b", {})])
        plugin.calls.clear()

        graphrecord.add_edges([("a", "b", {})])

        assert "pre_add_edges" in plugin.calls
        assert "post_add_edges" in plugin.calls

    def test_add_edges_with_group_hooks(self) -> None:
        plugin = RecordingPlugin()
        graphrecord = GraphRecord.with_plugins([plugin])
        graphrecord.add_nodes([("a", {}), ("b", {})])
        graphrecord.add_group("g", ["a", "b"])
        plugin.calls.clear()

        graphrecord.add_edges([("a", "b", {})], group="g")

        assert "pre_add_edges_with_group" in plugin.calls
        assert "post_add_edges_with_group" in plugin.calls

    def test_add_edges_polars_hooks(self) -> None:
        plugin = RecordingPlugin()
        graphrecord = GraphRecord.with_plugins([plugin])
        graphrecord.add_nodes([("a", {}), ("b", {})])
        plugin.calls.clear()

        dataframe = pl.DataFrame({"src": ["a"], "tgt": ["b"]})
        graphrecord.add_edges_polars([(dataframe, "src", "tgt")])

        assert "pre_add_edges_dataframes" in plugin.calls
        assert "post_add_edges_dataframes" in plugin.calls

    def test_add_edges_polars_with_group_hooks(self) -> None:
        plugin = RecordingPlugin()
        graphrecord = GraphRecord.with_plugins([plugin])
        graphrecord.add_nodes([("a", {}), ("b", {})])
        graphrecord.add_group("g", ["a", "b"])
        plugin.calls.clear()

        dataframe = pl.DataFrame({"src": ["a"], "tgt": ["b"]})
        graphrecord.add_edges_polars([(dataframe, "src", "tgt")], group="g")

        assert "pre_add_edges_dataframes_with_group" in plugin.calls
        assert "post_add_edges_dataframes_with_group" in plugin.calls

    def test_remove_edge_hooks(self) -> None:
        plugin = RecordingPlugin()
        graphrecord = GraphRecord.with_plugins([plugin])
        graphrecord.add_nodes([("a", {}), ("b", {})])
        graphrecord.add_edges([("a", "b", {})])
        plugin.calls.clear()

        graphrecord.remove_edges([0])

        assert "pre_remove_edge" in plugin.calls
        assert "post_remove_edge" in plugin.calls

    def test_add_group_hooks(self) -> None:
        plugin = RecordingPlugin()
        graphrecord = GraphRecord.with_plugins([plugin])
        plugin.calls.clear()

        graphrecord.add_group("g")

        assert "pre_add_group" in plugin.calls
        assert "post_add_group" in plugin.calls

    def test_remove_group_hooks(self) -> None:
        plugin = RecordingPlugin()
        graphrecord = GraphRecord.with_plugins([plugin])
        graphrecord.add_group("g")
        plugin.calls.clear()

        graphrecord.remove_groups(["g"])

        assert "pre_remove_group" in plugin.calls
        assert "post_remove_group" in plugin.calls

    def test_add_node_to_group_hooks(self) -> None:
        plugin = RecordingPlugin()
        graphrecord = GraphRecord.with_plugins([plugin])
        graphrecord.add_nodes([("a", {})])
        graphrecord.add_group("g")
        plugin.calls.clear()

        graphrecord.add_nodes_to_group("g", ["a"])

        assert "pre_add_node_to_group" in plugin.calls
        assert "post_add_node_to_group" in plugin.calls

    def test_add_edge_to_group_hooks(self) -> None:
        plugin = RecordingPlugin()
        graphrecord = GraphRecord.with_plugins([plugin])
        graphrecord.add_nodes([("a", {}), ("b", {})])
        edge_indices = graphrecord.add_edges([("a", "b", {})])
        graphrecord.add_group("g")
        plugin.calls.clear()

        graphrecord.add_edges_to_group("g", edge_indices)

        assert "pre_add_edge_to_group" in plugin.calls
        assert "post_add_edge_to_group" in plugin.calls

    def test_remove_node_from_group_hooks(self) -> None:
        plugin = RecordingPlugin()
        graphrecord = GraphRecord.with_plugins([plugin])
        graphrecord.add_nodes([("a", {})])
        graphrecord.add_group("g", ["a"])
        plugin.calls.clear()

        graphrecord.remove_nodes_from_group("g", ["a"])

        assert "pre_remove_node_from_group" in plugin.calls
        assert "post_remove_node_from_group" in plugin.calls

    def test_remove_edge_from_group_hooks(self) -> None:
        plugin = RecordingPlugin()
        graphrecord = GraphRecord.with_plugins([plugin])
        graphrecord.add_nodes([("a", {}), ("b", {})])
        edge_indices = graphrecord.add_edges([("a", "b", {})])
        graphrecord.add_group("g", edges=edge_indices)
        plugin.calls.clear()

        graphrecord.remove_edges_from_group("g", edge_indices)

        assert "pre_remove_edge_from_group" in plugin.calls
        assert "post_remove_edge_from_group" in plugin.calls

    def test_clear_hooks(self) -> None:
        plugin = RecordingPlugin()
        graphrecord = GraphRecord.with_plugins([plugin])
        graphrecord.add_nodes([("a", {})])
        plugin.calls.clear()

        graphrecord.clear()

        assert plugin.calls == ["pre_clear", "post_clear"]


class TestContextModification(unittest.TestCase):
    def test_pre_hook_can_rewrite_nodes(self) -> None:
        class RewritePlugin(Plugin):
            def pre_add_nodes(
                self, graphrecord: GraphRecord, context: PreAddNodesContext
            ) -> PreAddNodesContext:
                return PreAddNodesContext([("replaced", {"k": 99})])

        graphrecord = GraphRecord.with_plugins([RewritePlugin()])
        graphrecord.add_nodes([("original", {"k": 1})])

        assert "replaced" in graphrecord.nodes
        assert "original" not in graphrecord.nodes

    def test_pre_hook_can_redirect_edges(self) -> None:
        class RedirectPlugin(Plugin):
            def pre_add_edges(
                self, graphrecord: GraphRecord, context: PreAddEdgesContext
            ) -> PreAddEdgesContext:
                return PreAddEdgesContext([("a", "c", {})])

        graphrecord = GraphRecord.with_plugins([RedirectPlugin()])
        graphrecord.add_nodes([("a", {}), ("b", {}), ("c", {})])
        graphrecord.add_edges([("a", "b", {})])

        assert graphrecord.edge_count() == 1
        assert "c" in graphrecord.neighbors("a")
        assert "b" not in graphrecord.neighbors("a")


class TestMultiplePlugins(unittest.TestCase):
    def test_both_plugins_receive_hooks(self) -> None:
        first = RecordingPlugin()
        second = RecordingPlugin()
        graphrecord = GraphRecord.with_plugins([first, second])
        first.calls.clear()
        second.calls.clear()

        graphrecord.add_nodes([("a", {})])

        assert "pre_add_nodes" in first.calls
        assert "post_add_nodes" in first.calls
        assert "pre_add_nodes" in second.calls
        assert "post_add_nodes" in second.calls

    def test_pre_hooks_chain(self) -> None:
        class AppendPlugin(Plugin):
            def pre_add_nodes(
                self, graphrecord: GraphRecord, context: PreAddNodesContext
            ) -> PreAddNodesContext:
                return PreAddNodesContext([*context.nodes, ("extra", {})])

        graphrecord = GraphRecord.with_plugins([AppendPlugin()])
        graphrecord.add_nodes([("a", {})])

        assert "a" in graphrecord.nodes
        assert "extra" in graphrecord.nodes


class TestPluginBridgeSingularHooks(unittest.TestCase):
    def test_pre_add_node_bridge(self) -> None:
        plugin = RecordingPlugin()
        graphrecord = GraphRecord()
        bridge = _PluginBridge(plugin)
        context = PreAddNodeContext("a", {"x": 1})

        bridge.pre_add_node(graphrecord._graphrecord, context._py_context)

        assert "pre_add_node" in plugin.calls

    def test_post_add_node_bridge(self) -> None:
        plugin = RecordingPlugin()
        graphrecord = GraphRecord()
        bridge = _PluginBridge(plugin)
        context = PostAddNodeContext("a")

        bridge.post_add_node(graphrecord._graphrecord, context._py_context)

        assert "post_add_node" in plugin.calls

    def test_pre_add_node_with_group_bridge(self) -> None:
        plugin = RecordingPlugin()
        graphrecord = GraphRecord()
        bridge = _PluginBridge(plugin)
        context = PreAddNodeWithGroupContext("a", {"x": 1}, "g")

        bridge.pre_add_node_with_group(graphrecord._graphrecord, context._py_context)

        assert "pre_add_node_with_group" in plugin.calls

    def test_post_add_node_with_group_bridge(self) -> None:
        plugin = RecordingPlugin()
        graphrecord = GraphRecord()
        bridge = _PluginBridge(plugin)
        context = PostAddNodeWithGroupContext("a", "g")

        bridge.post_add_node_with_group(graphrecord._graphrecord, context._py_context)

        assert "post_add_node_with_group" in plugin.calls

    def test_pre_add_edge_bridge(self) -> None:
        plugin = RecordingPlugin()
        graphrecord = GraphRecord()
        bridge = _PluginBridge(plugin)
        context = PreAddEdgeContext("a", "b", {"w": 1})

        bridge.pre_add_edge(graphrecord._graphrecord, context._py_context)

        assert "pre_add_edge" in plugin.calls

    def test_post_add_edge_bridge(self) -> None:
        plugin = RecordingPlugin()
        graphrecord = GraphRecord()
        bridge = _PluginBridge(plugin)
        context = PostAddEdgeContext(0)

        bridge.post_add_edge(graphrecord._graphrecord, context._py_context)

        assert "post_add_edge" in plugin.calls

    def test_pre_add_edge_with_group_bridge(self) -> None:
        plugin = RecordingPlugin()
        graphrecord = GraphRecord()
        bridge = _PluginBridge(plugin)
        context = PreAddEdgeWithGroupContext("a", "b", {"w": 1}, "g")

        bridge.pre_add_edge_with_group(graphrecord._graphrecord, context._py_context)

        assert "pre_add_edge_with_group" in plugin.calls

    def test_post_add_edge_with_group_bridge(self) -> None:
        plugin = RecordingPlugin()
        graphrecord = GraphRecord()
        bridge = _PluginBridge(plugin)
        context = PostAddEdgeWithGroupContext(0)

        bridge.post_add_edge_with_group(graphrecord._graphrecord, context._py_context)

        assert "post_add_edge_with_group" in plugin.calls


if __name__ == "__main__":
    unittest.main()
