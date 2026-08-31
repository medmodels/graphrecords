from __future__ import annotations

import pickle
import unittest
from typing import TYPE_CHECKING, ClassVar, List, Tuple

import polars as pl
import pytest

from graphrecords import GraphRecord
from graphrecords.datatype import Int, String
from graphrecords.plugins import (
    AddEdges,
    AddEdgesInGroup,
    AddEdgesToGroup,
    AddGroup,
    AddNodes,
    AddNodesInGroup,
    AddNodesToGroup,
    Clear,
    EdgeBatch,
    FreezeSchema,
    NodeBatch,
    Plugin,
    RemoveEdgeAttributes,
    RemoveEdges,
    RemoveEdgesFromGroup,
    RemoveGroups,
    RemoveNodeAttributes,
    RemoveNodes,
    RemoveNodesFromGroup,
    ReplaceEdgeAttributes,
    ReplaceNodeAttributes,
    SetEdgeAttributes,
    SetNodeAttributes,
    SetSchema,
    UnfreezeSchema,
)
from graphrecords.schema import (
    AttributeDataType,
    AttributeType,
    GroupSchema,
    Schema,
    SchemaType,
)
from graphrecords.types import EdgeIndex

if TYPE_CHECKING:
    from graphrecords.plugins import Change
    from graphrecords.types import Attributes, NodeIndex


def create_graphrecord() -> GraphRecord:
    record = GraphRecord()
    record = record.add_nodes([("lorem", {"ipsum": 1}), ("dolor", {"ipsum": 2})])
    record = record.add_edge("lorem", "dolor", {"sit": "amet"})
    return record.add_group("consectetur")


class RecordingPlugin(Plugin):
    def __init__(self) -> None:
        self.calls: List[str] = []
        self.records: List[GraphRecord] = []
        self.payloads: List[object] = []
        self.observed: List[Tuple[GraphRecord, GraphRecord]] = []
        self.lifecycle: List[Tuple[str, GraphRecord]] = []

    def _change(self, name: str, record: GraphRecord, payload: object) -> None:
        self.calls.append(name)
        self.records.append(record)
        self.payloads.append(payload)

    def _observation(
        self, name: str, previous: GraphRecord, candidate: GraphRecord
    ) -> None:
        self.calls.append(name)
        self.observed.append((previous, candidate))

    def initialize(self, record: GraphRecord) -> None:
        self.lifecycle.append(("initialize", record))

    def finalize(self, record: GraphRecord) -> None:
        self.lifecycle.append(("finalize", record))

    def on_add_nodes(self, record: GraphRecord, addition: AddNodes) -> None:
        self._change("on_add_nodes", record, addition)

    def post_add_nodes(self, previous: GraphRecord, candidate: GraphRecord) -> None:
        self._observation("post_add_nodes", previous, candidate)

    def on_add_nodes_in_group(
        self, record: GraphRecord, addition: AddNodesInGroup
    ) -> None:
        self._change("on_add_nodes_in_group", record, addition)

    def post_add_nodes_in_group(
        self, previous: GraphRecord, candidate: GraphRecord
    ) -> None:
        self._observation("post_add_nodes_in_group", previous, candidate)

    def on_add_edges(self, record: GraphRecord, addition: AddEdges) -> None:
        self._change("on_add_edges", record, addition)

    def post_add_edges(self, previous: GraphRecord, candidate: GraphRecord) -> None:
        self._observation("post_add_edges", previous, candidate)

    def on_add_edges_in_group(
        self, record: GraphRecord, addition: AddEdgesInGroup
    ) -> None:
        self._change("on_add_edges_in_group", record, addition)

    def post_add_edges_in_group(
        self, previous: GraphRecord, candidate: GraphRecord
    ) -> None:
        self._observation("post_add_edges_in_group", previous, candidate)

    def on_remove_nodes(self, record: GraphRecord, removal: RemoveNodes) -> None:
        self._change("on_remove_nodes", record, removal)

    def post_remove_nodes(self, previous: GraphRecord, candidate: GraphRecord) -> None:
        self._observation("post_remove_nodes", previous, candidate)

    def on_remove_edges(self, record: GraphRecord, removal: RemoveEdges) -> None:
        self._change("on_remove_edges", record, removal)

    def post_remove_edges(self, previous: GraphRecord, candidate: GraphRecord) -> None:
        self._observation("post_remove_edges", previous, candidate)

    def on_set_node_attributes(
        self, record: GraphRecord, assignment: SetNodeAttributes
    ) -> None:
        self._change("on_set_node_attributes", record, assignment)

    def post_set_node_attributes(
        self, previous: GraphRecord, candidate: GraphRecord
    ) -> None:
        self._observation("post_set_node_attributes", previous, candidate)

    def on_replace_node_attributes(
        self, record: GraphRecord, assignment: ReplaceNodeAttributes
    ) -> None:
        self._change("on_replace_node_attributes", record, assignment)

    def post_replace_node_attributes(
        self, previous: GraphRecord, candidate: GraphRecord
    ) -> None:
        self._observation("post_replace_node_attributes", previous, candidate)

    def on_remove_node_attributes(
        self, record: GraphRecord, removal: RemoveNodeAttributes
    ) -> None:
        self._change("on_remove_node_attributes", record, removal)

    def post_remove_node_attributes(
        self, previous: GraphRecord, candidate: GraphRecord
    ) -> None:
        self._observation("post_remove_node_attributes", previous, candidate)

    def on_set_edge_attributes(
        self, record: GraphRecord, assignment: SetEdgeAttributes
    ) -> None:
        self._change("on_set_edge_attributes", record, assignment)

    def post_set_edge_attributes(
        self, previous: GraphRecord, candidate: GraphRecord
    ) -> None:
        self._observation("post_set_edge_attributes", previous, candidate)

    def on_replace_edge_attributes(
        self, record: GraphRecord, assignment: ReplaceEdgeAttributes
    ) -> None:
        self._change("on_replace_edge_attributes", record, assignment)

    def post_replace_edge_attributes(
        self, previous: GraphRecord, candidate: GraphRecord
    ) -> None:
        self._observation("post_replace_edge_attributes", previous, candidate)

    def on_remove_edge_attributes(
        self, record: GraphRecord, removal: RemoveEdgeAttributes
    ) -> None:
        self._change("on_remove_edge_attributes", record, removal)

    def post_remove_edge_attributes(
        self, previous: GraphRecord, candidate: GraphRecord
    ) -> None:
        self._observation("post_remove_edge_attributes", previous, candidate)

    def on_add_group(self, record: GraphRecord, addition: AddGroup) -> None:
        self._change("on_add_group", record, addition)

    def post_add_group(self, previous: GraphRecord, candidate: GraphRecord) -> None:
        self._observation("post_add_group", previous, candidate)

    def on_remove_groups(self, record: GraphRecord, removal: RemoveGroups) -> None:
        self._change("on_remove_groups", record, removal)

    def post_remove_groups(self, previous: GraphRecord, candidate: GraphRecord) -> None:
        self._observation("post_remove_groups", previous, candidate)

    def on_add_nodes_to_group(
        self, record: GraphRecord, membership: AddNodesToGroup
    ) -> None:
        self._change("on_add_nodes_to_group", record, membership)

    def post_add_nodes_to_group(
        self, previous: GraphRecord, candidate: GraphRecord
    ) -> None:
        self._observation("post_add_nodes_to_group", previous, candidate)

    def on_remove_nodes_from_group(
        self, record: GraphRecord, membership: RemoveNodesFromGroup
    ) -> None:
        self._change("on_remove_nodes_from_group", record, membership)

    def post_remove_nodes_from_group(
        self, previous: GraphRecord, candidate: GraphRecord
    ) -> None:
        self._observation("post_remove_nodes_from_group", previous, candidate)

    def on_add_edges_to_group(
        self, record: GraphRecord, membership: AddEdgesToGroup
    ) -> None:
        self._change("on_add_edges_to_group", record, membership)

    def post_add_edges_to_group(
        self, previous: GraphRecord, candidate: GraphRecord
    ) -> None:
        self._observation("post_add_edges_to_group", previous, candidate)

    def on_remove_edges_from_group(
        self, record: GraphRecord, membership: RemoveEdgesFromGroup
    ) -> None:
        self._change("on_remove_edges_from_group", record, membership)

    def post_remove_edges_from_group(
        self, previous: GraphRecord, candidate: GraphRecord
    ) -> None:
        self._observation("post_remove_edges_from_group", previous, candidate)

    def on_set_schema(self, record: GraphRecord, schema_change: SetSchema) -> None:
        self._change("on_set_schema", record, schema_change)

    def post_set_schema(self, previous: GraphRecord, candidate: GraphRecord) -> None:
        self._observation("post_set_schema", previous, candidate)

    def on_freeze_schema(
        self, record: GraphRecord, schema_change: FreezeSchema
    ) -> None:
        self._change("on_freeze_schema", record, schema_change)

    def post_freeze_schema(self, previous: GraphRecord, candidate: GraphRecord) -> None:
        self._observation("post_freeze_schema", previous, candidate)

    def on_unfreeze_schema(
        self, record: GraphRecord, schema_change: UnfreezeSchema
    ) -> None:
        self._change("on_unfreeze_schema", record, schema_change)

    def post_unfreeze_schema(
        self, previous: GraphRecord, candidate: GraphRecord
    ) -> None:
        self._observation("post_unfreeze_schema", previous, candidate)

    def on_clear(self, record: GraphRecord, clearing: Clear) -> None:
        self._change("on_clear", record, clearing)

    def post_clear(self, previous: GraphRecord, candidate: GraphRecord) -> None:
        self._observation("post_clear", previous, candidate)


class SingleHookPlugin(Plugin):
    def __init__(self) -> None:
        self.batch_sizes: List[int] = []

    def on_add_nodes(self, record: GraphRecord, addition: AddNodes) -> None:
        self.batch_sizes.append(len(addition.batch))


class LookupRecordingPlugin(Plugin):
    def __init__(self) -> None:
        self.lookups: List[str] = []

    def __getattr__(self, name: str) -> None:
        self.lookups.append(name)
        raise AttributeError(name)


class ReturningPlugin(Plugin):
    def on_add_nodes(self, record: GraphRecord, addition: AddNodes) -> AddNodes:
        return addition


class TransformingPlugin(Plugin):
    def on_add_nodes(self, record: GraphRecord, addition: AddNodes) -> AddNodes:
        return AddNodes(
            NodeBatch(
                [
                    (node_index, {**attributes, "sed": 4})
                    for node_index, attributes in addition.batch
                ]
            )
        )

    def on_add_nodes_in_group(
        self, record: GraphRecord, addition: AddNodesInGroup
    ) -> AddNodesInGroup:
        return AddNodesInGroup(
            NodeBatch(
                [
                    (node_index, {**attributes, "sed": 4})
                    for node_index, attributes in addition.batch
                ]
            ),
            addition.group_index,
        )

    def on_add_edges(self, record: GraphRecord, addition: AddEdges) -> AddEdges:
        return AddEdges(
            EdgeBatch(
                [
                    (source_node_index, target_node_index, {**attributes, "sed": 4})
                    for source_node_index, target_node_index, attributes in addition.batch
                ]
            )
        )

    def on_add_edges_in_group(
        self, record: GraphRecord, addition: AddEdgesInGroup
    ) -> AddEdgesInGroup:
        return AddEdgesInGroup(
            EdgeBatch(
                [
                    (source_node_index, target_node_index, {**attributes, "sed": 4})
                    for source_node_index, target_node_index, attributes in addition.batch
                ]
            ),
            addition.group_index,
        )

    def on_remove_nodes(self, record: GraphRecord, removal: RemoveNodes) -> RemoveNodes:
        return RemoveNodes(removal.node_indices[:1])

    def on_remove_edges(self, record: GraphRecord, removal: RemoveEdges) -> RemoveEdges:
        return RemoveEdges(removal.edge_indices[:1])

    def on_set_node_attributes(
        self, record: GraphRecord, assignment: SetNodeAttributes
    ) -> SetNodeAttributes:
        return SetNodeAttributes(
            assignment.node_indices, {**assignment.attributes, "sed": 4}
        )

    def on_replace_node_attributes(
        self, record: GraphRecord, assignment: ReplaceNodeAttributes
    ) -> ReplaceNodeAttributes:
        return ReplaceNodeAttributes(
            assignment.node_indices, {**assignment.attributes, "sed": 4}
        )

    def on_remove_node_attributes(
        self, record: GraphRecord, removal: RemoveNodeAttributes
    ) -> RemoveNodeAttributes:
        return RemoveNodeAttributes(removal.node_indices, removal.attribute_names[:1])

    def on_set_edge_attributes(
        self, record: GraphRecord, assignment: SetEdgeAttributes
    ) -> SetEdgeAttributes:
        return SetEdgeAttributes(
            assignment.edge_indices, {**assignment.attributes, "sed": 4}
        )

    def on_replace_edge_attributes(
        self, record: GraphRecord, assignment: ReplaceEdgeAttributes
    ) -> ReplaceEdgeAttributes:
        return ReplaceEdgeAttributes(
            assignment.edge_indices, {**assignment.attributes, "sed": 4}
        )

    def on_remove_edge_attributes(
        self, record: GraphRecord, removal: RemoveEdgeAttributes
    ) -> RemoveEdgeAttributes:
        return RemoveEdgeAttributes(removal.edge_indices, removal.attribute_names[:1])

    def on_add_group(self, record: GraphRecord, addition: AddGroup) -> AddGroup:
        return AddGroup("elit")

    def on_remove_groups(
        self, record: GraphRecord, removal: RemoveGroups
    ) -> RemoveGroups:
        return RemoveGroups(removal.group_indices[:1])

    def on_add_nodes_to_group(
        self, record: GraphRecord, membership: AddNodesToGroup
    ) -> AddNodesToGroup:
        return AddNodesToGroup(membership.node_indices[:1], membership.group_index)

    def on_remove_nodes_from_group(
        self, record: GraphRecord, membership: RemoveNodesFromGroup
    ) -> RemoveNodesFromGroup:
        return RemoveNodesFromGroup(membership.node_indices[:1], membership.group_index)

    def on_add_edges_to_group(
        self, record: GraphRecord, membership: AddEdgesToGroup
    ) -> AddEdgesToGroup:
        return AddEdgesToGroup(membership.edge_indices[:1], membership.group_index)

    def on_remove_edges_from_group(
        self, record: GraphRecord, membership: RemoveEdgesFromGroup
    ) -> RemoveEdgesFromGroup:
        return RemoveEdgesFromGroup(membership.edge_indices[:1], membership.group_index)

    def on_set_schema(self, record: GraphRecord, schema_change: SetSchema) -> SetSchema:
        return SetSchema(Schema.infer(record))

    def on_freeze_schema(
        self, record: GraphRecord, schema_change: FreezeSchema
    ) -> List[Change]:
        return [AddGroup("elit"), FreezeSchema()]

    def on_unfreeze_schema(
        self, record: GraphRecord, schema_change: UnfreezeSchema
    ) -> List[Change]:
        return [UnfreezeSchema(), AddGroup("elit")]

    def on_clear(self, record: GraphRecord, clearing: Clear) -> List[Change]:
        return [Clear(), AddGroup("elit")]


class ExpandingPlugin(Plugin):
    def on_add_nodes(self, record: GraphRecord, addition: AddNodes) -> List[Change]:
        return [addition, AddGroup("elit")]


class SwallowingPlugin(Plugin):
    def on_add_nodes(self, record: GraphRecord, addition: AddNodes) -> List[Change]:
        return []


class LifecyclePlugin(Plugin):
    def initialize(self, record: GraphRecord) -> AddGroup:
        return AddGroup("elit")

    def finalize(self, record: GraphRecord) -> RemoveGroups:
        return RemoveGroups(["elit"])


class InvalidReturningPlugin(Plugin):
    def on_add_nodes(self, record: GraphRecord, addition: AddNodes) -> int:
        return 4


class InvalidObservingPlugin(Plugin):
    def post_add_nodes(self, previous: GraphRecord, candidate: GraphRecord) -> int:
        return 4


class RaisingPlugin(Plugin):
    failure: ClassVar[LookupError] = LookupError("lorem", "ipsum")

    def on_add_nodes(self, record: GraphRecord, addition: AddNodes) -> None:
        raise RaisingPlugin.failure


class PicklePlugin(Plugin):
    calls: ClassVar[List[str]] = []

    def on_add_nodes(self, record: GraphRecord, addition: AddNodes) -> None:
        PicklePlugin.calls.append("on_add_nodes")

    def post_add_nodes(self, previous: GraphRecord, candidate: GraphRecord) -> None:
        PicklePlugin.calls.append("post_add_nodes")


def create_node_batch(nodes: List[Tuple[NodeIndex, Attributes]]) -> NodeBatch:
    plugin = RecordingPlugin()
    GraphRecord().add_plugin("observer", plugin).add_nodes(nodes)
    payload = plugin.payloads[0]

    assert isinstance(payload, AddNodes)

    return payload.batch


def create_edge_batch(
    edges: List[Tuple[NodeIndex, NodeIndex, Attributes]],
) -> EdgeBatch:
    plugin = RecordingPlugin()
    create_graphrecord().add_plugin("observer", plugin).add_edges(edges)
    payload = plugin.payloads[0]

    assert isinstance(payload, AddEdges)

    return payload.batch


class TestNodeBatch(unittest.TestCase):
    def test_init(self) -> None:
        from_rows = NodeBatch([("sit", {"amet": 1}), ("consectetur", {"amet": 2})])
        from_frame = NodeBatch(
            (pl.DataFrame({"node_index": ["sit"], "amet": [1]}), "node_index")
        )

        assert list(from_rows) == [("sit", {"amet": 1}), ("consectetur", {"amet": 2})]
        assert list(from_frame) == [("sit", {"amet": 1})]

    def test_len(self) -> None:
        batch = create_node_batch([("sit", {"amet": 1}), ("consectetur", {"amet": 2})])

        assert len(batch) == 2
        assert len(create_node_batch([])) == 0

    def test_is_empty(self) -> None:
        batch = create_node_batch([("sit", {"amet": 1})])

        assert not batch.is_empty()
        assert create_node_batch([]).is_empty()

    def test_attribute_values(self) -> None:
        batch = create_node_batch([("sit", {"amet": 1}), ("consectetur", {"elit": 2})])

        assert batch.attribute_values("amet") == [("sit", 1)]
        assert batch.attribute_values("elit") == [("consectetur", 2)]
        assert batch.attribute_values("adipiscing") == []

    def test_iter(self) -> None:
        batch = create_node_batch([("sit", {"amet": 1}), ("consectetur", {"elit": 2})])

        iterator = iter(batch)

        assert iter(iterator) is iterator
        assert next(iterator) == ("sit", {"amet": 1})
        assert list(iterator) == [("consectetur", {"elit": 2})]
        assert list(batch) == [("sit", {"amet": 1}), ("consectetur", {"elit": 2})]


class TestEdgeBatch(unittest.TestCase):
    def test_init(self) -> None:
        from_rows = EdgeBatch(
            [("lorem", "dolor", {"amet": 1}), ("dolor", "lorem", {"amet": 2})]
        )
        from_frame = EdgeBatch(
            (
                pl.DataFrame(
                    {
                        "source_node_index": ["lorem"],
                        "target_node_index": ["dolor"],
                        "amet": [1],
                    }
                ),
                "source_node_index",
                "target_node_index",
            )
        )

        assert list(from_rows) == [
            ("lorem", "dolor", {"amet": 1}),
            ("dolor", "lorem", {"amet": 2}),
        ]
        assert list(from_frame) == [("lorem", "dolor", {"amet": 1})]

    def test_len(self) -> None:
        batch = create_edge_batch(
            [("lorem", "dolor", {"amet": 1}), ("dolor", "lorem", {"amet": 2})]
        )

        assert len(batch) == 2
        assert len(create_edge_batch([])) == 0

    def test_is_empty(self) -> None:
        batch = create_edge_batch([("lorem", "dolor", {"amet": 1})])

        assert not batch.is_empty()
        assert create_edge_batch([]).is_empty()

    def test_attribute_values(self) -> None:
        batch = create_edge_batch(
            [("lorem", "dolor", {"amet": 1}), ("dolor", "lorem", {"elit": 2})]
        )

        assert batch.attribute_values("amet") == [("lorem", "dolor", 1)]
        assert batch.attribute_values("elit") == [("dolor", "lorem", 2)]
        assert batch.attribute_values("adipiscing") == []

    def test_iter(self) -> None:
        batch = create_edge_batch(
            [("lorem", "dolor", {"amet": 1}), ("dolor", "lorem", {"elit": 2})]
        )

        iterator = iter(batch)

        assert iter(iterator) is iterator
        assert next(iterator) == ("lorem", "dolor", {"amet": 1})
        assert list(iterator) == [("dolor", "lorem", {"elit": 2})]
        assert list(batch) == [
            ("lorem", "dolor", {"amet": 1}),
            ("dolor", "lorem", {"elit": 2}),
        ]


class TestChangeHooks(unittest.TestCase):
    def test_add_nodes(self) -> None:
        plugin = RecordingPlugin()
        record = create_graphrecord().add_plugin("observer", plugin)

        changed = record.add_nodes([("sit", {"amet": 3}), ("elit", {"amet": 4})])

        assert plugin.calls == ["on_add_nodes", "post_add_nodes"]

        observed_record = plugin.records[0]
        payload = plugin.payloads[0]

        assert isinstance(observed_record, GraphRecord)
        assert isinstance(payload, AddNodes)
        assert observed_record.node_count() == 2
        assert isinstance(payload.batch, NodeBatch)
        assert len(payload.batch) == 2
        assert payload.batch.attribute_values("amet") == [("sit", 3), ("elit", 4)]

        previous, candidate = plugin.observed[0]

        assert isinstance(previous, GraphRecord)
        assert isinstance(candidate, GraphRecord)
        assert previous.node_count() == 2
        assert candidate.node_count() == 4
        assert candidate == changed

    def test_add_nodes_in_group(self) -> None:
        plugin = RecordingPlugin()
        record = create_graphrecord().add_plugin("observer", plugin)

        changed = record.add_nodes_in_group([("sit", {"amet": 3})], "consectetur")

        assert plugin.calls == ["on_add_nodes_in_group", "post_add_nodes_in_group"]

        payload = plugin.payloads[0]

        assert isinstance(plugin.records[0], GraphRecord)
        assert isinstance(payload, AddNodesInGroup)
        assert isinstance(payload.batch, NodeBatch)
        assert list(payload.batch) == [("sit", {"amet": 3})]
        assert payload.group_index == "consectetur"

        previous, candidate = plugin.observed[0]

        assert isinstance(previous, GraphRecord)
        assert isinstance(candidate, GraphRecord)
        assert previous.group("consectetur").node_count() == 0
        assert candidate.group("consectetur").nodes() == ["sit"]
        assert candidate == changed

    def test_add_edges(self) -> None:
        plugin = RecordingPlugin()
        record = create_graphrecord().add_plugin("observer", plugin)

        changed = record.add_edges([("dolor", "lorem", {"sed": 3})])

        assert plugin.calls == ["on_add_edges", "post_add_edges"]

        payload = plugin.payloads[0]

        assert isinstance(plugin.records[0], GraphRecord)
        assert isinstance(payload, AddEdges)
        assert isinstance(payload.batch, EdgeBatch)
        assert len(payload.batch) == 1
        assert payload.batch.attribute_values("sed") == [("dolor", "lorem", 3)]

        previous, candidate = plugin.observed[0]

        assert isinstance(previous, GraphRecord)
        assert isinstance(candidate, GraphRecord)
        assert previous.edge_count() == 1
        assert candidate.edge_count() == 2
        assert candidate == changed

    def test_add_edges_in_group(self) -> None:
        plugin = RecordingPlugin()
        record = create_graphrecord().add_plugin("observer", plugin)

        changed = record.add_edges_in_group(
            [("dolor", "lorem", {"sed": 3})], "consectetur"
        )

        assert plugin.calls == ["on_add_edges_in_group", "post_add_edges_in_group"]

        payload = plugin.payloads[0]

        assert isinstance(plugin.records[0], GraphRecord)
        assert isinstance(payload, AddEdgesInGroup)
        assert isinstance(payload.batch, EdgeBatch)
        assert list(payload.batch) == [("dolor", "lorem", {"sed": 3})]
        assert payload.group_index == "consectetur"

        previous, candidate = plugin.observed[0]

        assert isinstance(previous, GraphRecord)
        assert isinstance(candidate, GraphRecord)
        assert previous.group("consectetur").edge_count() == 0
        assert candidate.group("consectetur").edge_count() == 1
        assert candidate == changed

    def test_remove_nodes(self) -> None:
        plugin = RecordingPlugin()
        record = create_graphrecord().add_plugin("observer", plugin)

        changed = record.remove_nodes("lorem")

        assert plugin.calls == ["on_remove_nodes", "post_remove_nodes"]

        payload = plugin.payloads[0]

        assert isinstance(plugin.records[0], GraphRecord)
        assert isinstance(payload, RemoveNodes)
        assert payload.node_indices == ["lorem"]

        previous, candidate = plugin.observed[0]

        assert isinstance(previous, GraphRecord)
        assert isinstance(candidate, GraphRecord)
        assert previous.node_indices() == ["lorem", "dolor"]
        assert candidate.node_indices() == ["dolor"]
        assert candidate == changed

    def test_remove_edges(self) -> None:
        plugin = RecordingPlugin()
        record = create_graphrecord().add_plugin("observer", plugin)
        edge_index = record.edge_indices()[0]

        changed = record.remove_edges(edge_index)

        assert plugin.calls == ["on_remove_edges", "post_remove_edges"]

        payload = plugin.payloads[0]

        assert isinstance(plugin.records[0], GraphRecord)
        assert isinstance(payload, RemoveEdges)
        assert isinstance(payload.edge_indices[0], EdgeIndex)
        assert payload.edge_indices == [edge_index]

        previous, candidate = plugin.observed[0]

        assert isinstance(previous, GraphRecord)
        assert isinstance(candidate, GraphRecord)
        assert previous.edge_count() == 1
        assert candidate.edge_count() == 0
        assert candidate == changed

    def test_set_node_attributes(self) -> None:
        plugin = RecordingPlugin()
        record = create_graphrecord().add_plugin("observer", plugin)

        changed = record.set_node_attributes("lorem", {"sed": 3})

        assert plugin.calls == ["on_set_node_attributes", "post_set_node_attributes"]

        payload = plugin.payloads[0]

        assert isinstance(plugin.records[0], GraphRecord)
        assert isinstance(payload, SetNodeAttributes)
        assert payload.node_indices == ["lorem"]
        assert payload.attributes == {"sed": 3}

        previous, candidate = plugin.observed[0]

        assert isinstance(previous, GraphRecord)
        assert isinstance(candidate, GraphRecord)
        assert previous.node("lorem").attributes() == {"ipsum": 1}
        assert candidate.node("lorem").attributes() == {"ipsum": 1, "sed": 3}
        assert candidate == changed

    def test_replace_node_attributes(self) -> None:
        plugin = RecordingPlugin()
        record = create_graphrecord().add_plugin("observer", plugin)

        changed = record.replace_node_attributes("lorem", {"sed": 3})

        assert plugin.calls == [
            "on_replace_node_attributes",
            "post_replace_node_attributes",
        ]

        payload = plugin.payloads[0]

        assert isinstance(plugin.records[0], GraphRecord)
        assert isinstance(payload, ReplaceNodeAttributes)
        assert payload.node_indices == ["lorem"]
        assert payload.attributes == {"sed": 3}

        previous, candidate = plugin.observed[0]

        assert isinstance(previous, GraphRecord)
        assert isinstance(candidate, GraphRecord)
        assert previous.node("lorem").attributes() == {"ipsum": 1}
        assert candidate.node("lorem").attributes() == {"sed": 3}
        assert candidate == changed

    def test_remove_node_attributes(self) -> None:
        plugin = RecordingPlugin()
        record = create_graphrecord().add_plugin("observer", plugin)

        changed = record.remove_node_attributes("lorem", ["ipsum"])

        assert plugin.calls == [
            "on_remove_node_attributes",
            "post_remove_node_attributes",
        ]

        payload = plugin.payloads[0]

        assert isinstance(plugin.records[0], GraphRecord)
        assert isinstance(payload, RemoveNodeAttributes)
        assert payload.node_indices == ["lorem"]
        assert payload.attribute_names == ["ipsum"]

        previous, candidate = plugin.observed[0]

        assert isinstance(previous, GraphRecord)
        assert isinstance(candidate, GraphRecord)
        assert previous.node("lorem").attributes() == {"ipsum": 1}
        assert candidate.node("lorem").attributes() == {}
        assert candidate == changed

    def test_set_edge_attributes(self) -> None:
        plugin = RecordingPlugin()
        record = create_graphrecord().add_plugin("observer", plugin)
        edge_index = record.edge_indices()[0]

        changed = record.set_edge_attributes(edge_index, {"sed": 3})

        assert plugin.calls == ["on_set_edge_attributes", "post_set_edge_attributes"]

        payload = plugin.payloads[0]

        assert isinstance(plugin.records[0], GraphRecord)
        assert isinstance(payload, SetEdgeAttributes)
        assert isinstance(payload.edge_indices[0], EdgeIndex)
        assert payload.edge_indices == [edge_index]
        assert payload.attributes == {"sed": 3}

        previous, candidate = plugin.observed[0]

        assert isinstance(previous, GraphRecord)
        assert isinstance(candidate, GraphRecord)
        assert previous.edge(edge_index).attributes() == {"sit": "amet"}
        assert candidate.edge(edge_index).attributes() == {"sit": "amet", "sed": 3}
        assert candidate == changed

    def test_replace_edge_attributes(self) -> None:
        plugin = RecordingPlugin()
        record = create_graphrecord().add_plugin("observer", plugin)
        edge_index = record.edge_indices()[0]

        changed = record.replace_edge_attributes(edge_index, {"sed": 3})

        assert plugin.calls == [
            "on_replace_edge_attributes",
            "post_replace_edge_attributes",
        ]

        payload = plugin.payloads[0]

        assert isinstance(plugin.records[0], GraphRecord)
        assert isinstance(payload, ReplaceEdgeAttributes)
        assert isinstance(payload.edge_indices[0], EdgeIndex)
        assert payload.edge_indices == [edge_index]
        assert payload.attributes == {"sed": 3}

        previous, candidate = plugin.observed[0]

        assert isinstance(previous, GraphRecord)
        assert isinstance(candidate, GraphRecord)
        assert previous.edge(edge_index).attributes() == {"sit": "amet"}
        assert candidate.edge(edge_index).attributes() == {"sed": 3}
        assert candidate == changed

    def test_remove_edge_attributes(self) -> None:
        plugin = RecordingPlugin()
        record = create_graphrecord().add_plugin("observer", plugin)
        edge_index = record.edge_indices()[0]

        changed = record.remove_edge_attributes(edge_index, ["sit"])

        assert plugin.calls == [
            "on_remove_edge_attributes",
            "post_remove_edge_attributes",
        ]

        payload = plugin.payloads[0]

        assert isinstance(plugin.records[0], GraphRecord)
        assert isinstance(payload, RemoveEdgeAttributes)
        assert isinstance(payload.edge_indices[0], EdgeIndex)
        assert payload.edge_indices == [edge_index]
        assert payload.attribute_names == ["sit"]

        previous, candidate = plugin.observed[0]

        assert isinstance(previous, GraphRecord)
        assert isinstance(candidate, GraphRecord)
        assert previous.edge(edge_index).attributes() == {"sit": "amet"}
        assert candidate.edge(edge_index).attributes() == {}
        assert candidate == changed

    def test_add_group(self) -> None:
        plugin = RecordingPlugin()
        record = create_graphrecord().add_plugin("observer", plugin)

        changed = record.add_group("adipiscing")

        assert plugin.calls == ["on_add_group", "post_add_group"]

        payload = plugin.payloads[0]

        assert isinstance(plugin.records[0], GraphRecord)
        assert isinstance(payload, AddGroup)
        assert payload.group_index == "adipiscing"

        previous, candidate = plugin.observed[0]

        assert isinstance(previous, GraphRecord)
        assert isinstance(candidate, GraphRecord)
        assert previous.group_indices() == ["consectetur"]
        assert candidate.group_indices() == ["consectetur", "adipiscing"]
        assert candidate == changed

    def test_remove_groups(self) -> None:
        plugin = RecordingPlugin()
        record = create_graphrecord().add_plugin("observer", plugin)

        changed = record.remove_groups("consectetur")

        assert plugin.calls == ["on_remove_groups", "post_remove_groups"]

        payload = plugin.payloads[0]

        assert isinstance(plugin.records[0], GraphRecord)
        assert isinstance(payload, RemoveGroups)
        assert payload.group_indices == ["consectetur"]

        previous, candidate = plugin.observed[0]

        assert isinstance(previous, GraphRecord)
        assert isinstance(candidate, GraphRecord)
        assert previous.group_indices() == ["consectetur"]
        assert candidate.group_indices() == []
        assert candidate == changed

    def test_add_nodes_to_group(self) -> None:
        plugin = RecordingPlugin()
        record = create_graphrecord().add_plugin("observer", plugin)

        changed = record.add_nodes_to_group("lorem", "consectetur")

        assert plugin.calls == ["on_add_nodes_to_group", "post_add_nodes_to_group"]

        payload = plugin.payloads[0]

        assert isinstance(plugin.records[0], GraphRecord)
        assert isinstance(payload, AddNodesToGroup)
        assert payload.node_indices == ["lorem"]
        assert payload.group_index == "consectetur"

        previous, candidate = plugin.observed[0]

        assert isinstance(previous, GraphRecord)
        assert isinstance(candidate, GraphRecord)
        assert previous.group("consectetur").nodes() == []
        assert candidate.group("consectetur").nodes() == ["lorem"]
        assert candidate == changed

    def test_remove_nodes_from_group(self) -> None:
        plugin = RecordingPlugin()
        record = create_graphrecord().add_nodes_to_group("lorem", "consectetur")
        record = record.add_plugin("observer", plugin)

        changed = record.remove_nodes_from_group("lorem", "consectetur")

        assert plugin.calls == [
            "on_remove_nodes_from_group",
            "post_remove_nodes_from_group",
        ]

        payload = plugin.payloads[0]

        assert isinstance(plugin.records[0], GraphRecord)
        assert isinstance(payload, RemoveNodesFromGroup)
        assert payload.node_indices == ["lorem"]
        assert payload.group_index == "consectetur"

        previous, candidate = plugin.observed[0]

        assert isinstance(previous, GraphRecord)
        assert isinstance(candidate, GraphRecord)
        assert previous.group("consectetur").nodes() == ["lorem"]
        assert candidate.group("consectetur").nodes() == []
        assert candidate == changed

    def test_add_edges_to_group(self) -> None:
        plugin = RecordingPlugin()
        record = create_graphrecord().add_plugin("observer", plugin)
        edge_index = record.edge_indices()[0]

        changed = record.add_edges_to_group(edge_index, "consectetur")

        assert plugin.calls == ["on_add_edges_to_group", "post_add_edges_to_group"]

        payload = plugin.payloads[0]

        assert isinstance(plugin.records[0], GraphRecord)
        assert isinstance(payload, AddEdgesToGroup)
        assert isinstance(payload.edge_indices[0], EdgeIndex)
        assert payload.edge_indices == [edge_index]
        assert payload.group_index == "consectetur"

        previous, candidate = plugin.observed[0]

        assert isinstance(previous, GraphRecord)
        assert isinstance(candidate, GraphRecord)
        assert previous.group("consectetur").edges() == []
        assert candidate.group("consectetur").edges() == [edge_index]
        assert candidate == changed

    def test_remove_edges_from_group(self) -> None:
        plugin = RecordingPlugin()
        record = create_graphrecord()
        edge_index = record.edge_indices()[0]
        record = record.add_edges_to_group(edge_index, "consectetur")
        record = record.add_plugin("observer", plugin)

        changed = record.remove_edges_from_group(edge_index, "consectetur")

        assert plugin.calls == [
            "on_remove_edges_from_group",
            "post_remove_edges_from_group",
        ]

        payload = plugin.payloads[0]

        assert isinstance(plugin.records[0], GraphRecord)
        assert isinstance(payload, RemoveEdgesFromGroup)
        assert isinstance(payload.edge_indices[0], EdgeIndex)
        assert payload.edge_indices == [edge_index]
        assert payload.group_index == "consectetur"

        previous, candidate = plugin.observed[0]

        assert isinstance(previous, GraphRecord)
        assert isinstance(candidate, GraphRecord)
        assert previous.group("consectetur").edges() == [edge_index]
        assert candidate.group("consectetur").edges() == []
        assert candidate == changed

    def test_set_schema(self) -> None:
        plugin = RecordingPlugin()
        record = create_graphrecord().add_plugin("observer", plugin)
        schema = Schema(
            ungrouped=GroupSchema(
                nodes={"ipsum": AttributeDataType(Int(), AttributeType.Continuous)},
                edges={"sit": AttributeDataType(String(), AttributeType.Unstructured)},
            )
        )

        changed = record.set_schema(schema)

        assert plugin.calls == ["on_set_schema", "post_set_schema"]

        payload = plugin.payloads[0]

        assert isinstance(plugin.records[0], GraphRecord)
        assert isinstance(payload, SetSchema)
        assert isinstance(payload.schema, Schema)
        assert payload.schema.schema_type == SchemaType.Provided
        assert payload.schema.ungrouped.nodes == {
            "ipsum": AttributeDataType(Int(), AttributeType.Continuous)
        }

        previous, candidate = plugin.observed[0]

        assert isinstance(previous, GraphRecord)
        assert isinstance(candidate, GraphRecord)
        assert previous.schema.schema_type == SchemaType.Inferred
        assert candidate.schema.schema_type == SchemaType.Provided
        assert candidate == changed

    def test_freeze_schema(self) -> None:
        plugin = RecordingPlugin()
        record = create_graphrecord().add_plugin("observer", plugin)

        changed = record.freeze_schema()

        assert plugin.calls == ["on_freeze_schema", "post_freeze_schema"]

        payload = plugin.payloads[0]

        assert isinstance(plugin.records[0], GraphRecord)
        assert isinstance(payload, FreezeSchema)

        previous, candidate = plugin.observed[0]

        assert isinstance(previous, GraphRecord)
        assert isinstance(candidate, GraphRecord)
        assert previous.schema.schema_type == SchemaType.Inferred
        assert candidate.schema.schema_type == SchemaType.Provided
        assert candidate == changed

    def test_unfreeze_schema(self) -> None:
        plugin = RecordingPlugin()
        record = create_graphrecord().freeze_schema()
        record = record.add_plugin("observer", plugin)

        changed = record.unfreeze_schema()

        assert plugin.calls == ["on_unfreeze_schema", "post_unfreeze_schema"]

        payload = plugin.payloads[0]

        assert isinstance(plugin.records[0], GraphRecord)
        assert isinstance(payload, UnfreezeSchema)

        previous, candidate = plugin.observed[0]

        assert isinstance(previous, GraphRecord)
        assert isinstance(candidate, GraphRecord)
        assert previous.schema.schema_type == SchemaType.Provided
        assert candidate.schema.schema_type == SchemaType.Inferred
        assert candidate == changed

    def test_clear(self) -> None:
        plugin = RecordingPlugin()
        record = create_graphrecord().add_plugin("observer", plugin)

        changed = record.clear()

        assert plugin.calls == ["on_clear", "post_clear"]

        payload = plugin.payloads[0]

        assert isinstance(plugin.records[0], GraphRecord)
        assert isinstance(payload, Clear)

        previous, candidate = plugin.observed[0]

        assert isinstance(previous, GraphRecord)
        assert isinstance(candidate, GraphRecord)
        assert previous.node_count() == 2
        assert candidate.node_count() == 0
        assert candidate.group_count() == 0
        assert candidate == changed


class TestTransformation(unittest.TestCase):
    def test_add_nodes(self) -> None:
        record = create_graphrecord().add_plugin("transformer", TransformingPlugin())

        changed = record.add_nodes([("sit", {"amet": 3})])

        assert changed.node("sit").attributes() == {"amet": 3, "sed": 4}

    def test_add_nodes_in_group(self) -> None:
        record = create_graphrecord().add_plugin("transformer", TransformingPlugin())

        changed = record.add_nodes_in_group([("sit", {"amet": 3})], "consectetur")

        assert changed.node("sit").attributes() == {"amet": 3, "sed": 4}
        assert changed.group("consectetur").nodes() == ["sit"]

    def test_add_edges(self) -> None:
        record = create_graphrecord()
        existing_edge_index = record.edge_indices()[0]
        record = record.add_plugin("transformer", TransformingPlugin())

        changed = record.add_edges([("dolor", "lorem", {"amet": 3})])
        edge_index = next(
            index for index in changed.edge_indices() if index != existing_edge_index
        )

        assert changed.edge(edge_index).attributes() == {"amet": 3, "sed": 4}

    def test_add_edges_in_group(self) -> None:
        record = create_graphrecord()
        existing_edge_index = record.edge_indices()[0]
        record = record.add_plugin("transformer", TransformingPlugin())

        changed = record.add_edges_in_group(
            [("dolor", "lorem", {"amet": 3})], "consectetur"
        )
        edge_index = next(
            index for index in changed.edge_indices() if index != existing_edge_index
        )

        assert changed.edge(edge_index).attributes() == {"amet": 3, "sed": 4}
        assert changed.group("consectetur").edges() == [edge_index]

    def test_remove_nodes(self) -> None:
        record = create_graphrecord().add_plugin("transformer", TransformingPlugin())

        changed = record.remove_nodes(["lorem", "dolor"])

        assert changed.node_indices() == ["dolor"]

    def test_remove_edges(self) -> None:
        record = create_graphrecord().add_edges([("dolor", "lorem", {"amet": 3})])
        edge_indices = record.edge_indices()
        record = record.add_plugin("transformer", TransformingPlugin())

        changed = record.remove_edges(edge_indices)

        assert changed.edge_indices() == edge_indices[1:]

    def test_set_node_attributes(self) -> None:
        record = create_graphrecord().add_plugin("transformer", TransformingPlugin())

        changed = record.set_node_attributes("lorem", {"amet": 3})

        assert changed.node("lorem").attributes() == {"ipsum": 1, "amet": 3, "sed": 4}

    def test_replace_node_attributes(self) -> None:
        record = create_graphrecord().add_plugin("transformer", TransformingPlugin())

        changed = record.replace_node_attributes("lorem", {"amet": 3})

        assert changed.node("lorem").attributes() == {"amet": 3, "sed": 4}

    def test_remove_node_attributes(self) -> None:
        record = create_graphrecord().set_node_attributes("lorem", {"amet": 3})
        record = record.add_plugin("transformer", TransformingPlugin())

        changed = record.remove_node_attributes("lorem", ["ipsum", "amet"])

        assert changed.node("lorem").attributes() == {"amet": 3}

    def test_set_edge_attributes(self) -> None:
        record = create_graphrecord()
        edge_index = record.edge_indices()[0]
        record = record.add_plugin("transformer", TransformingPlugin())

        changed = record.set_edge_attributes(edge_index, {"amet": 3})

        assert changed.edge(edge_index).attributes() == {
            "sit": "amet",
            "amet": 3,
            "sed": 4,
        }

    def test_replace_edge_attributes(self) -> None:
        record = create_graphrecord()
        edge_index = record.edge_indices()[0]
        record = record.add_plugin("transformer", TransformingPlugin())

        changed = record.replace_edge_attributes(edge_index, {"amet": 3})

        assert changed.edge(edge_index).attributes() == {"amet": 3, "sed": 4}

    def test_remove_edge_attributes(self) -> None:
        record = create_graphrecord()
        edge_index = record.edge_indices()[0]
        record = record.set_edge_attributes(edge_index, {"amet": 3})
        record = record.add_plugin("transformer", TransformingPlugin())

        changed = record.remove_edge_attributes(edge_index, ["sit", "amet"])

        assert changed.edge(edge_index).attributes() == {"amet": 3}

    def test_add_group(self) -> None:
        record = create_graphrecord().add_plugin("transformer", TransformingPlugin())

        changed = record.add_group("adipiscing")

        assert changed.group_indices() == ["consectetur", "elit"]

    def test_remove_groups(self) -> None:
        record = create_graphrecord().add_group("adipiscing")
        record = record.add_plugin("transformer", TransformingPlugin())

        changed = record.remove_groups(["consectetur", "adipiscing"])

        assert changed.group_indices() == ["adipiscing"]

    def test_add_nodes_to_group(self) -> None:
        record = create_graphrecord().add_plugin("transformer", TransformingPlugin())

        changed = record.add_nodes_to_group(["lorem", "dolor"], "consectetur")

        assert changed.group("consectetur").nodes() == ["lorem"]

    def test_remove_nodes_from_group(self) -> None:
        record = create_graphrecord().add_nodes_to_group(
            ["lorem", "dolor"], "consectetur"
        )
        record = record.add_plugin("transformer", TransformingPlugin())

        changed = record.remove_nodes_from_group(["lorem", "dolor"], "consectetur")

        assert changed.group("consectetur").nodes() == ["dolor"]

    def test_add_edges_to_group(self) -> None:
        record = create_graphrecord().add_edges([("dolor", "lorem", {"amet": 3})])
        edge_indices = record.edge_indices()
        record = record.add_plugin("transformer", TransformingPlugin())

        changed = record.add_edges_to_group(edge_indices, "consectetur")

        assert changed.group("consectetur").edges() == edge_indices[:1]

    def test_remove_edges_from_group(self) -> None:
        record = create_graphrecord().add_edges([("dolor", "lorem", {"amet": 3})])
        edge_indices = record.edge_indices()
        record = record.add_edges_to_group(edge_indices, "consectetur")
        record = record.add_plugin("transformer", TransformingPlugin())

        changed = record.remove_edges_from_group(edge_indices, "consectetur")

        assert changed.group("consectetur").edges() == edge_indices[1:]

    def test_set_schema(self) -> None:
        record = create_graphrecord().add_plugin("transformer", TransformingPlugin())
        schema = Schema(
            ungrouped=GroupSchema(
                nodes={"ipsum": AttributeDataType(Int(), AttributeType.Continuous)},
                edges={"sit": AttributeDataType(String(), AttributeType.Unstructured)},
            )
        )

        changed = record.set_schema(schema)

        assert changed.schema.schema_type == SchemaType.Inferred

    def test_freeze_schema(self) -> None:
        record = create_graphrecord().add_plugin("transformer", TransformingPlugin())

        changed = record.freeze_schema()

        assert changed.schema.schema_type == SchemaType.Provided
        assert changed.group_indices() == ["consectetur", "elit"]

    def test_unfreeze_schema(self) -> None:
        record = create_graphrecord().freeze_schema()
        record = record.add_plugin("transformer", TransformingPlugin())

        changed = record.unfreeze_schema()

        assert changed.schema.schema_type == SchemaType.Inferred
        assert changed.group_indices() == ["consectetur", "elit"]

    def test_clear(self) -> None:
        record = create_graphrecord().add_plugin("transformer", TransformingPlugin())

        changed = record.clear()

        assert changed.node_count() == 0
        assert changed.group_indices() == ["elit"]


class TestPlugin(unittest.TestCase):
    def test_add_plugin(self) -> None:
        plugin = RecordingPlugin()
        record = create_graphrecord()

        changed = record.add_plugin("observer", plugin)

        assert changed.plugins == ["observer"]
        assert record.plugins == []

        hook_name, observed_record = plugin.lifecycle[0]

        assert hook_name == "initialize"
        assert isinstance(observed_record, GraphRecord)
        assert observed_record.plugins == ["observer"]
        assert observed_record == changed
        assert plugin.calls == []

    def test_invalid_add_plugin(self) -> None:
        record = create_graphrecord().add_plugin("observer", RecordingPlugin())

        with pytest.raises(
            KeyError, match='Plugin with name `"observer"` already exists'
        ):
            record.add_plugin("observer", RecordingPlugin())

    def test_remove_plugin(self) -> None:
        plugin = RecordingPlugin()
        record = create_graphrecord().add_plugin("observer", plugin)

        changed = record.remove_plugin("observer")

        assert changed.plugins == []
        assert record.plugins == ["observer"]

        hook_name, observed_record = plugin.lifecycle[1]

        assert hook_name == "finalize"
        assert isinstance(observed_record, GraphRecord)
        assert observed_record.plugins == ["observer"]
        assert plugin.calls == []

    def test_invalid_remove_plugin(self) -> None:
        record = create_graphrecord()

        with pytest.raises(
            KeyError, match='Plugin with name `"auditor"` does not exist'
        ):
            record.remove_plugin("auditor")

    def test_plugins(self) -> None:
        observer = RecordingPlugin()
        auditor = SingleHookPlugin()
        record = create_graphrecord().add_plugin("observer", observer)
        record = record.add_plugin("auditor", auditor)

        record = record.add_nodes([("sit", {"amet": 3})])

        assert record.plugins == ["observer", "auditor"]
        assert record.remove_plugin("observer").plugins == ["auditor"]
        assert observer.calls == ["on_add_nodes", "post_add_nodes"]
        assert auditor.batch_sizes == [1]

    def test_undefined_hooks(self) -> None:
        plugin = SingleHookPlugin()
        record = create_graphrecord().add_plugin("observer", plugin)

        record = record.add_nodes([("sit", {"amet": 3})])
        record = record.add_edges([("dolor", "lorem", {"sed": 4})])
        record = record.add_nodes_to_group("lorem", "consectetur")
        record = record.remove_nodes("sit")
        record = record.clear()
        record = record.remove_plugin("observer")

        assert plugin.batch_sizes == [1]
        assert record.node_count() == 0

    def test_hook_lookup(self) -> None:
        plugin = LookupRecordingPlugin()
        record = create_graphrecord().add_plugin("observer", plugin)

        record = record.add_nodes([("sit", {"amet": 3})])
        record = record.remove_plugin("observer")

        assert plugin.lookups == [
            "initialize",
            "on_add_nodes",
            "post_add_nodes",
            "finalize",
        ]
        assert record.node_count() == 3

    def test_returning_hook(self) -> None:
        record = create_graphrecord().add_plugin("observer", ReturningPlugin())

        changed = record.add_nodes([("sit", {"amet": 3})])

        assert changed.node_count() == 3
        assert changed.node("sit").attributes() == {"amet": 3}

    def test_expanding_hook(self) -> None:
        record = create_graphrecord().add_plugin("observer", ExpandingPlugin())

        changed = record.add_nodes([("sit", {"amet": 3})])

        assert changed.node_count() == 3
        assert changed.group_indices() == ["consectetur", "elit"]

    def test_swallowing_hook(self) -> None:
        record = create_graphrecord().add_plugin("observer", SwallowingPlugin())

        changed = record.add_nodes([("sit", {"amet": 3})])

        assert changed.node_count() == 2
        assert changed == record

    def test_lifecycle_hooks(self) -> None:
        record = create_graphrecord().add_plugin("observer", LifecyclePlugin())

        assert record.group_indices() == ["consectetur", "elit"]

        removed = record.remove_plugin("observer")

        assert removed.group_indices() == ["consectetur"]

    def test_invalid_returning_hook(self) -> None:
        record = create_graphrecord().add_plugin("observer", InvalidReturningPlugin())

        with pytest.raises(
            TypeError,
            match="Plugin hooks must return a change, a list of changes, or None",
        ):
            record.add_nodes([("sit", {"amet": 3})])

    def test_invalid_observing_hook(self) -> None:
        record = create_graphrecord().add_plugin("observer", InvalidObservingPlugin())

        with pytest.raises(TypeError, match="Plugin observer hooks must return None"):
            record.add_nodes([("sit", {"amet": 3})])

    def test_failing_hook(self) -> None:
        record = create_graphrecord().add_plugin("observer", RaisingPlugin())

        with pytest.raises(LookupError, match="lorem") as raised:
            record.add_nodes([("sit", {"amet": 3})])

        assert raised.value is RaisingPlugin.failure
        assert raised.value.args == ("lorem", "ipsum")

    def test_pickle(self) -> None:
        record = create_graphrecord().add_plugin("observer", PicklePlugin())
        PicklePlugin.calls.clear()

        restored = pickle.loads(pickle.dumps(record))

        assert restored == record
        assert restored.plugins == ["observer"]
        assert PicklePlugin.calls == []

        restored = restored.add_nodes([("sit", {"amet": 3})])

        assert PicklePlugin.calls == ["on_add_nodes", "post_add_nodes"]
        assert restored.node_count() == 3


if __name__ == "__main__":
    suite = unittest.TestSuite()

    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestNodeBatch))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestEdgeBatch))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestChangeHooks))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestTransformation))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestPlugin))

    unittest.TextTestRunner(verbosity=2).run(suite)
