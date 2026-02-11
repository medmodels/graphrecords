from __future__ import annotations

import unittest
from typing import TYPE_CHECKING, List, Optional

import polars as pl

from graphrecords import GraphRecord
from graphrecords.plugins import (
    AddEdgesContext,
    AddEdgesPolarsContext,
    AddEdgesToGroupContext,
    AddGroupContext,
    AddNodesContext,
    AddNodesPolarsContext,
    AddNodesToGroupContext,
    Plugin,
    RemoveEdgesContext,
    RemoveEdgesFromGroupContext,
    RemoveGroupsContext,
    RemoveNodesContext,
    RemoveNodesFromGroupContext,
    SetSchemaContext,
)
from graphrecords.schema import Schema

if TYPE_CHECKING:
    from graphrecords.types import EdgeIndex


class RecordingPlugin(Plugin):
    def __init__(self) -> None:
        self.calls: List[str] = []
        self.last_context: object = None
        self.last_result: object = None
        self.last_graphrecord: Optional[GraphRecord] = None

    def pre_add_nodes(
        self, graphrecord: GraphRecord, context: AddNodesContext
    ) -> AddNodesContext:
        self.calls.append("pre_add_nodes")
        self.last_context = context
        self.last_graphrecord = graphrecord
        return context

    def post_add_nodes(
        self, graphrecord: GraphRecord, context: AddNodesContext
    ) -> None:
        self.calls.append("post_add_nodes")

    def pre_add_nodes_polars(
        self, graphrecord: GraphRecord, context: AddNodesPolarsContext
    ) -> AddNodesPolarsContext:
        self.calls.append("pre_add_nodes_polars")
        self.last_context = context
        return context

    def post_add_nodes_polars(
        self, graphrecord: GraphRecord, context: AddNodesPolarsContext
    ) -> None:
        self.calls.append("post_add_nodes_polars")

    def pre_add_edges(
        self, graphrecord: GraphRecord, context: AddEdgesContext
    ) -> AddEdgesContext:
        self.calls.append("pre_add_edges")
        self.last_context = context
        return context

    def post_add_edges(
        self,
        graphrecord: GraphRecord,
        context: AddEdgesContext,
        result: List[EdgeIndex],
    ) -> None:
        self.calls.append("post_add_edges")
        self.last_result = result

    def pre_clear(self, graphrecord: GraphRecord) -> None:
        self.calls.append("pre_clear")
        self.last_graphrecord = graphrecord

    def post_clear(self, graphrecord: GraphRecord) -> None:
        self.calls.append("post_clear")


class TestContexts(unittest.TestCase):
    def test_node_contexts(self) -> None:
        add = AddNodesContext(nodes=[("n", {})], group="g")
        assert add.nodes == [("n", {})]
        assert add.group == "g"

        add_polars = AddNodesPolarsContext(nodes=[], group=None)
        assert add_polars.nodes == []
        assert add_polars.group is None

        remove = RemoveNodesContext(nodes=["n"])
        assert remove.nodes == ["n"]

    def test_edge_contexts(self) -> None:
        add = AddEdgesContext(edges=[("a", "b", {})], group="g")
        assert add.edges == [("a", "b", {})]
        assert add.group == "g"

        add_polars = AddEdgesPolarsContext(edges=[], group=None)
        assert add_polars.edges == []
        assert add_polars.group is None

        remove = RemoveEdgesContext(edges=[0])
        assert remove.edges == [0]

    def test_group_contexts(self) -> None:
        add = AddGroupContext(group="g", nodes=["n"], edges=[0])
        assert add.group == "g"
        assert add.nodes == ["n"]
        assert add.edges == [0]

        add_none = AddGroupContext(group="g", nodes=None, edges=None)
        assert add_none.nodes is None
        assert add_none.edges is None

        remove = RemoveGroupsContext(groups=["g"])
        assert remove.groups == ["g"]

        add_nodes = AddNodesToGroupContext(group="g", nodes=["n"])
        assert add_nodes.group == "g"
        assert add_nodes.nodes == ["n"]

        add_edges = AddEdgesToGroupContext(group="g", edges=[0])
        assert add_edges.group == "g"
        assert add_edges.edges == [0]

        remove_nodes = RemoveNodesFromGroupContext(group="g", nodes=["n"])
        assert remove_nodes.group == "g"
        assert remove_nodes.nodes == ["n"]

        remove_edges = RemoveEdgesFromGroupContext(group="g", edges=[0])
        assert remove_edges.group == "g"
        assert remove_edges.edges == [0]

    def test_schema_context(self) -> None:
        schema = Schema()
        ctx = SetSchemaContext(schema=schema)
        assert ctx.schema is schema


class TestPluginHooks(unittest.TestCase):
    def setUp(self) -> None:
        self.plugin = RecordingPlugin()
        self.graphrecord = GraphRecord.with_plugins(self.plugin)

    def test_add_nodes_fires_hooks(self) -> None:
        self.graphrecord.add_nodes([("a", {"x": 1})])

        assert self.plugin.calls == ["pre_add_nodes", "post_add_nodes"]
        assert isinstance(self.plugin.last_context, AddNodesContext)
        assert self.plugin.last_context.nodes == [("a", {"x": 1})]
        assert self.plugin.last_context.group is None
        assert self.plugin.last_graphrecord is self.graphrecord

    def test_add_edges_passes_result_to_post_hook(self) -> None:
        self.graphrecord.add_nodes([("a", {}), ("b", {})])
        self.plugin.calls.clear()

        edge_indices = self.graphrecord.add_edges([("a", "b", {"w": 1})])

        assert self.plugin.calls == ["pre_add_edges", "post_add_edges"]
        assert isinstance(self.plugin.last_context, AddEdgesContext)
        assert self.plugin.last_context.edges == [("a", "b", {"w": 1})]
        assert self.plugin.last_result == edge_indices

    def test_polars_hooks_fire(self) -> None:
        df = pl.DataFrame({"idx": ["a"], "val": [1]})
        self.graphrecord.add_nodes_polars([(df, "idx")])

        assert self.plugin.calls == [
            "pre_add_nodes_polars",
            "post_add_nodes_polars",
        ]
        assert isinstance(self.plugin.last_context, AddNodesPolarsContext)

    def test_context_free_hooks_fire(self) -> None:
        self.graphrecord.add_nodes([("a", {})])
        self.plugin.calls.clear()

        self.graphrecord.clear()

        assert self.plugin.calls == ["pre_clear", "post_clear"]
        assert self.plugin.last_graphrecord is self.graphrecord

    def test_with_plugins_accepts_single_and_list(self) -> None:
        single = GraphRecord.with_plugins(RecordingPlugin())
        single.add_nodes([("a", {})])
        assert "a" in single.nodes

        multi = GraphRecord.with_plugins([RecordingPlugin()])
        multi.add_nodes([("a", {})])
        assert "a" in multi.nodes


class TestContextModification(unittest.TestCase):
    def test_pre_hook_can_rewrite_nodes(self) -> None:
        class RewritePlugin(Plugin):
            def pre_add_nodes(
                self, graphrecord: GraphRecord, context: AddNodesContext
            ) -> AddNodesContext:
                context.nodes = [("replaced", {"k": 99})]
                return context

        graphrecord = GraphRecord.with_plugins(RewritePlugin())
        graphrecord.add_nodes([("original", {"k": 1})])

        assert "replaced" in graphrecord.nodes
        assert "original" not in graphrecord.nodes

    def test_pre_hook_can_redirect_edges(self) -> None:
        class RedirectPlugin(Plugin):
            def pre_add_edges(
                self, graphrecord: GraphRecord, context: AddEdgesContext
            ) -> AddEdgesContext:
                context.edges = [("a", "c", {})]
                return context

        graphrecord = GraphRecord.with_plugins(RedirectPlugin())
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

        graphrecord.add_nodes([("a", {})])

        assert first.calls == ["pre_add_nodes", "post_add_nodes"]
        assert second.calls == ["pre_add_nodes", "post_add_nodes"]

    def test_pre_hooks_chain(self) -> None:
        class AppendPlugin(Plugin):
            def pre_add_nodes(
                self, graphrecord: GraphRecord, context: AddNodesContext
            ) -> AddNodesContext:
                context.nodes = [*context.nodes, ("extra", {})]
                return context

        recording = RecordingPlugin()
        graphrecord = GraphRecord.with_plugins([AppendPlugin(), recording])
        graphrecord.add_nodes([("a", {})])

        assert isinstance(recording.last_context, AddNodesContext)
        assert recording.last_context.nodes == [("a", {}), ("extra", {})]
        assert "extra" in graphrecord.nodes


class TestDefaultPlugin(unittest.TestCase):
    def test_all_operations_pass_through(self) -> None:
        graphrecord = GraphRecord.with_plugins(Plugin())

        graphrecord.set_schema(Schema())
        graphrecord.freeze_schema()
        graphrecord.unfreeze_schema()

        graphrecord.add_nodes([("a", {"x": 1}), ("b", {}), ("c", {})])

        df_nodes = pl.DataFrame({"idx": ["d"], "val": [2]})
        graphrecord.add_nodes_polars([(df_nodes, "idx")])

        graphrecord.add_edges([("a", "b", {})])

        df_edges = pl.DataFrame({"src": ["b"], "tgt": ["c"]})
        graphrecord.add_edges_polars([(df_edges, "src", "tgt")])

        graphrecord.add_group("g", ["a"], [0])
        graphrecord.add_nodes_to_group("g", ["b"])
        graphrecord.add_edges_to_group("g", [1])
        graphrecord.remove_nodes_from_group("g", ["b"])
        graphrecord.remove_edges_from_group("g", [1])
        graphrecord.remove_groups(["g"])

        graphrecord.remove_edges([0])
        graphrecord.remove_nodes(["a"])

        graphrecord.clear()

        assert graphrecord.nodes == []
        assert graphrecord.edges == []


if __name__ == "__main__":
    suite = unittest.TestSuite()
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestContexts))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestPluginHooks))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestContextModification))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestMultiplePlugins))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestDefaultPlugin))
    unittest.TextTestRunner(verbosity=2).run(suite)
