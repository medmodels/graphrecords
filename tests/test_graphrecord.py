import copy
import pickle
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List, Optional, Tuple, cast

import polars as pl
import pytest

from graphrecords import (
    AddNodes,
    AttributeDataType,
    AttributeType,
    Drop,
    EdgeDirection,
    GraphRecord,
    GroupSchema,
    OnConflict,
    Option,
    Plugin,
    Schema,
    SchemaType,
    String,
    edges,
    groups,
    nodes,
)
from graphrecords.graphrecord import (
    ArrowStream,
    ArrowTables,
    EdgeCollector,
    EdgeView,
    GroupView,
    NodeCollector,
    NodeView,
    PolarsFrames,
    RecordBatch,
    RonFile,
    Writer,
)
from graphrecords.querying import Series
from graphrecords.types import Attributes, EdgeIndex, NodeIndex, Value


def create_nodes() -> List[Tuple[NodeIndex, Attributes]]:
    return [
        ("0", {"lorem": "ipsum", "dolor": "sit"}),
        ("1", {"amet": "consectetur"}),
        ("2", {"lorem": "adipiscing"}),
        ("3", {}),
    ]


def create_edges() -> List[Tuple[NodeIndex, NodeIndex, Attributes]]:
    return [
        ("0", "1", {"sed": "do", "eiusmod": "tempor"}),
        ("1", "0", {"sed": "incididunt"}),
        ("1", "2", {"ut": "labore"}),
        ("0", "3", {}),
    ]


def create_graphrecord() -> GraphRecord:
    record = GraphRecord().add_nodes(create_nodes()).add_edges(create_edges())
    record = record.add_group("magna").add_nodes_to_group(["0", "1"], "magna")
    record = record.add_edges_to_group(record.edge_indices()[0], "magna")
    return record.add_group("aliqua").add_nodes_to_group("2", "aliqua")


def create_nodes_frame() -> pl.DataFrame:
    return pl.DataFrame({"node_index": ["4", "5"], "enim": ["veniam", "quis"]})


def create_edges_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "source_node_index": ["4", "5"],
            "target_node_index": ["5", "4"],
            "nostrud": ["exercitation", "ullamco"],
        }
    )


def create_schema() -> Schema:
    return Schema(
        ungrouped=GroupSchema(
            nodes={
                "lorem": AttributeDataType(Option(String()), AttributeType.Unstructured)
            }
        ),
        schema_type=SchemaType.Provided,
    )


class NodeRows(NodeCollector):
    def collect_nodes(self) -> List[Tuple[NodeIndex, Attributes]]:
        return create_nodes()


class EdgeRows(EdgeCollector):
    def collect_edges(self) -> List[Tuple[NodeIndex, NodeIndex, Attributes]]:
        return create_edges()


class ArrowTable(ArrowStream):
    def __init__(self, record_batch: RecordBatch) -> None:
        self._record_batch = record_batch

    def __arrow_c_stream__(self, requested_schema: Optional[object] = None) -> object:
        return self._record_batch.__arrow_c_stream__(requested_schema)


class CountingWriter(Writer[Tuple[int, int]]):
    def write(self, record: GraphRecord) -> Tuple[int, int]:
        return record.node_count(), record.edge_count()


class FailingWriter(Writer[None]):
    def write(self, record: GraphRecord) -> None:
        msg = f"lorem ipsum {record.node_count()}"
        raise RuntimeError(msg)


class IntegerScalar:
    def __index__(self) -> int:
        return 4


class FloatScalar:
    def __float__(self) -> float:
        return 1.5


class RecordingPlugin(Plugin):
    def __init__(self) -> None:
        self.calls: List[str] = []

    def initialize(self, record: GraphRecord) -> None:
        self.calls.append(f"initialize:{record.node_count()}")

    def finalize(self, record: GraphRecord) -> None:
        self.calls.append(f"finalize:{record.node_count()}")

    def pre_add_nodes(self, record: GraphRecord, addition: AddNodes) -> None:
        self.calls.append(f"pre_add_nodes:{len(addition.batch)}")


class TestOnConflict(unittest.TestCase):
    def test_from_py_on_conflict(self) -> None:
        for on_conflict in OnConflict:
            assert (
                OnConflict._from_py_on_conflict(on_conflict._into_py_on_conflict())
                == on_conflict
            )

    def test_into_py_on_conflict(self) -> None:
        assert (
            OnConflict.Raise._into_py_on_conflict()
            == OnConflict.Raise._into_py_on_conflict()
        )
        assert (
            OnConflict.Raise._into_py_on_conflict()
            != OnConflict.KeepSelf._into_py_on_conflict()
        )
        assert (
            OnConflict.KeepSelf._into_py_on_conflict()
            != OnConflict.KeepOther._into_py_on_conflict()
        )

    def test_repr(self) -> None:
        assert repr(OnConflict.Raise) == "OnConflict.Raise"
        assert repr(OnConflict.KeepSelf) == "OnConflict.KeepSelf"
        assert repr(OnConflict.KeepOther) == "OnConflict.KeepOther"

    def test_str(self) -> None:
        assert str(OnConflict.Raise) == "Raise"
        assert str(OnConflict.KeepSelf) == "KeepSelf"
        assert str(OnConflict.KeepOther) == "KeepOther"


class TestNodeCollector(unittest.TestCase):
    def test_collect_nodes(self) -> None:
        collector = NodeRows()

        assert collector.collect_nodes() == create_nodes()


class TestEdgeCollector(unittest.TestCase):
    def test_collect_edges(self) -> None:
        collector = EdgeRows()

        assert collector.collect_edges() == create_edges()


class TestArrowStream(unittest.TestCase):
    def test_arrow_c_stream(self) -> None:
        table = ArrowTable(create_graphrecord().to_arrow()["groups"]["magna"]["nodes"])

        assert GraphRecord().add_nodes((table, "node_index")).node_indices() == [
            "0",
            "1",
        ]


class TestWriter(unittest.TestCase):
    def test_write(self) -> None:
        writer = CountingWriter()

        assert writer.write(create_graphrecord()) == (4, 4)


class TestRecordBatch(unittest.TestCase):
    def test_arrow_c_array(self) -> None:
        record_batch = create_graphrecord().to_arrow()["ungrouped"]["nodes"]

        schema_capsule, array_capsule = record_batch.__arrow_c_array__()

        assert type(schema_capsule).__name__ == "PyCapsule"
        assert type(array_capsule).__name__ == "PyCapsule"
        assert len(record_batch.__arrow_c_array__(None)) == 2

    def test_arrow_c_stream(self) -> None:
        record_batch = create_graphrecord().to_arrow()["groups"]["magna"]["nodes"]

        frame = pl.DataFrame(record_batch)

        assert frame.columns == ["node_index", "amet", "dolor", "lorem"]
        assert frame.height == 2
        assert (
            pl.DataFrame(create_graphrecord().to_arrow()["ungrouped"]["edges"]).height
            == 3
        )

    def test_len(self) -> None:
        export = create_graphrecord().to_arrow()

        assert len(export["ungrouped"]["nodes"]) == 1
        assert len(export["ungrouped"]["edges"]) == 3
        assert len(export["groups"]["magna"]["nodes"]) == 2
        assert len(export["groups"]["magna"]["edges"]) == 1


class TestRonFile(unittest.TestCase):
    def test_write(self) -> None:
        record = create_graphrecord()

        with TemporaryDirectory() as directory:
            path = Path(directory) / "record.ron"

            assert RonFile(str(path)).write(record) is None
            assert path.exists()


class TestPolarsFrames(unittest.TestCase):
    def test_write(self) -> None:
        export = PolarsFrames().write(create_graphrecord())

        assert sorted(export) == ["groups", "ungrouped"]
        assert export["ungrouped"]["nodes"].height == 1
        assert export["groups"]["magna"]["nodes"].height == 2


class TestArrowTables(unittest.TestCase):
    def test_write(self) -> None:
        export = ArrowTables().write(create_graphrecord())

        assert sorted(export) == ["groups", "ungrouped"]
        assert len(export["ungrouped"]["nodes"]) == 1
        assert len(export["groups"]["magna"]["nodes"]) == 2


class TestNodeView(unittest.TestCase):
    def test_index(self) -> None:
        record = create_graphrecord()

        assert record.node("0").index() == "0"
        assert record.node("3").index() == "3"

    def test_attribute(self) -> None:
        record = create_graphrecord()

        assert record.node("0").attribute("lorem") == "ipsum"
        assert record.node("0").attribute("dolor") == "sit"
        assert record.add_node("4", {"amet": None}).node("4").attribute("amet") is None

    def test_invalid_attribute(self) -> None:
        record = create_graphrecord()

        with pytest.raises(KeyError, match="does not exist on node"):
            record.node("0").attribute("amet")

        with pytest.raises(KeyError, match="does not exist on node"):
            record.node("3").attribute("lorem")

    def test_attributes(self) -> None:
        record = create_graphrecord()

        assert record.node("0").attributes() == {"lorem": "ipsum", "dolor": "sit"}
        assert record.node("1").attributes() == {"amet": "consectetur"}
        assert record.node("3").attributes() == {}

    def test_groups(self) -> None:
        record = create_graphrecord()

        assert record.node("0").groups() == ["magna"]
        assert record.node("2").groups() == ["aliqua"]
        assert record.node("3").groups() == []

    def test_edges(self) -> None:
        record = create_graphrecord()
        view = record.node("0")

        assert view.edges() == [
            record.edge_indices()[0],
            record.edge_indices()[3],
            record.edge_indices()[1],
        ]
        assert view.edges(EdgeDirection.Outgoing) == [
            record.edge_indices()[0],
            record.edge_indices()[3],
        ]
        assert view.edges(EdgeDirection.Incoming) == [record.edge_indices()[1]]
        assert view.edges(EdgeDirection.Both) == view.edges()

    def test_neighbors(self) -> None:
        record = create_graphrecord()
        view = record.node("0")

        assert view.neighbors() == ["1", "3"]
        assert view.neighbors(EdgeDirection.Outgoing) == ["1", "3"]
        assert view.neighbors(EdgeDirection.Incoming) == ["1"]
        assert record.node("3").neighbors() == ["0"]

    def test_degree(self) -> None:
        record = create_graphrecord()
        view = record.node("0")

        assert view.degree() == 3
        assert view.degree(EdgeDirection.Outgoing) == 2
        assert view.degree(EdgeDirection.Incoming) == 1
        assert record.node("3").degree() == 1

    def test_edges_to(self) -> None:
        record = create_graphrecord()
        view = record.node("0")

        assert view.edges_to("1") == [record.edge_indices()[0]]
        assert view.edges_to("1", EdgeDirection.Incoming) == [record.edge_indices()[1]]
        assert view.edges_to("1", EdgeDirection.Both) == [
            record.edge_indices()[0],
            record.edge_indices()[1],
        ]
        assert view.edges_to(nodes().sort_by(nodes().index()).last()) == [
            record.edge_indices()[3]
        ]
        assert view.edges_to("2") == []

    def test_invalid_edges_to(self) -> None:
        record = create_graphrecord()

        with pytest.raises(IndexError, match="Cannot find node with index"):
            record.node("0").edges_to("99")

    def test_repr(self) -> None:
        record = create_graphrecord()

        assert repr(record.node("0")) == 'NodeView("0")'
        assert repr(record.node("3")) == 'NodeView("3")'

    def test_view(self) -> None:
        record = create_graphrecord()

        assert isinstance(record.node("0"), NodeView)


class TestEdgeView(unittest.TestCase):
    def test_index(self) -> None:
        record = create_graphrecord()
        edge_index = record.edge_indices()[0]

        assert record.edge(edge_index).index() == edge_index
        assert isinstance(record.edge(edge_index).index(), EdgeIndex)

    def test_source(self) -> None:
        record = create_graphrecord()

        assert record.edge(record.edge_indices()[0]).source() == "0"
        assert record.edge(record.edge_indices()[1]).source() == "1"

    def test_target(self) -> None:
        record = create_graphrecord()

        assert record.edge(record.edge_indices()[0]).target() == "1"
        assert record.edge(record.edge_indices()[1]).target() == "0"

    def test_attribute(self) -> None:
        record = create_graphrecord()
        edge_index = record.edge_indices()[0]
        view = record.edge(edge_index)

        assert view.attribute("sed") == "do"
        assert view.attribute("eiusmod") == "tempor"
        assert (
            record.set_edge_attributes(edge_index, {"ut": None})
            .edge(edge_index)
            .attribute("ut")
            is None
        )

    def test_invalid_attribute(self) -> None:
        record = create_graphrecord()
        view = record.edge(record.edge_indices()[0])

        with pytest.raises(KeyError, match="does not exist on edge"):
            view.attribute("ut")

    def test_attributes(self) -> None:
        record = create_graphrecord()

        assert record.edge(record.edge_indices()[0]).attributes() == {
            "sed": "do",
            "eiusmod": "tempor",
        }
        assert record.edge(record.edge_indices()[3]).attributes() == {}

    def test_groups(self) -> None:
        record = create_graphrecord()

        assert record.edge(record.edge_indices()[0]).groups() == ["magna"]
        assert record.edge(record.edge_indices()[1]).groups() == []

    def test_repr(self) -> None:
        record = create_graphrecord()
        edge_index = record.edge_indices()[0]

        assert repr(record.edge(edge_index)) == f"EdgeView({edge_index})"

    def test_view(self) -> None:
        record = create_graphrecord()

        assert isinstance(record.edge(record.edge_indices()[0]), EdgeView)


class TestGroupView(unittest.TestCase):
    def test_index(self) -> None:
        record = create_graphrecord()

        assert record.group("magna").index() == "magna"
        assert record.group("aliqua").index() == "aliqua"

    def test_nodes(self) -> None:
        record = create_graphrecord()

        assert record.group("magna").nodes() == ["0", "1"]
        assert record.group("aliqua").nodes() == ["2"]

    def test_edges(self) -> None:
        record = create_graphrecord()

        assert record.group("magna").edges() == [record.edge_indices()[0]]
        assert record.group("aliqua").edges() == []

    def test_node_count(self) -> None:
        record = create_graphrecord()

        assert record.group("magna").node_count() == 2
        assert record.group("aliqua").node_count() == 1

    def test_edge_count(self) -> None:
        record = create_graphrecord()

        assert record.group("magna").edge_count() == 1
        assert record.group("aliqua").edge_count() == 0

    def test_repr(self) -> None:
        record = create_graphrecord()

        assert repr(record.group("magna")) == 'GroupView("magna")'

    def test_view(self) -> None:
        record = create_graphrecord()

        assert isinstance(record.group("magna"), GroupView)


class TestGraphRecord(unittest.TestCase):
    def test_init(self) -> None:
        record = GraphRecord()

        assert record.node_count() == 0
        assert record.edge_count() == 0
        assert record.group_count() == 0
        assert record.plugins == []

    def test_from_py_graphrecord(self) -> None:
        record = create_graphrecord()

        rebuilt = GraphRecord._from_py_graphrecord(record._py_graphrecord)

        assert isinstance(rebuilt, GraphRecord)
        assert rebuilt == record

    def test_with_schema(self) -> None:
        record = GraphRecord.with_schema(create_schema())

        assert record.schema.schema_type == SchemaType.Provided
        assert list(record.schema.ungrouped.nodes) == ["lorem"]
        assert record.add_node("0", {"lorem": "ipsum"}).node_count() == 1

    def test_invalid_with_schema(self) -> None:
        record = GraphRecord.with_schema(create_schema())

        with pytest.raises(ValueError, match="do not exist in schema"):
            record.add_node("0", {"dolor": "sit"})

    def test_from_ron(self) -> None:
        record = create_graphrecord()

        with TemporaryDirectory() as directory:
            path = Path(directory) / "record.ron"
            record.to_ron(str(path))

            assert GraphRecord.from_ron(str(path)) == record

            record.to_ron(path)

            assert GraphRecord.from_ron(path) == record

    def test_invalid_from_ron(self) -> None:
        with TemporaryDirectory() as directory:
            path = str(Path(directory) / "missing.ron")

            with pytest.raises(OSError, match="Failed to read file"):
                GraphRecord.from_ron(path)

    def test_plugins(self) -> None:
        record = create_graphrecord()

        assert record.plugins == []
        assert record.add_plugin("ipsum", RecordingPlugin()).plugins == ["ipsum"]

    def test_plugin_entries(self) -> None:
        record = create_graphrecord()
        plugin = RecordingPlugin()

        assert record.plugin_entries == {}
        assert record.add_plugin("ipsum", plugin).plugin_entries == {"ipsum": plugin}

    def test_add_plugin(self) -> None:
        record = create_graphrecord()
        plugin = RecordingPlugin()

        extended = record.add_plugin("ipsum", plugin)

        assert extended.plugins == ["ipsum"]
        assert record.plugins == []
        assert plugin.calls == ["initialize:4"]
        assert extended.add_node("4", {}).node_count() == 5

    def test_invalid_add_plugin(self) -> None:
        record = create_graphrecord().add_plugin("ipsum", RecordingPlugin())

        with pytest.raises(KeyError, match="already exists"):
            record.add_plugin("ipsum", RecordingPlugin())

    def test_remove_plugin(self) -> None:
        plugin = RecordingPlugin()
        record = create_graphrecord().add_plugin("ipsum", plugin)

        reduced = record.remove_plugin("ipsum")

        assert reduced.plugins == []
        assert record.plugins == ["ipsum"]
        assert plugin.calls == ["initialize:4", "finalize:4"]

    def test_invalid_remove_plugin(self) -> None:
        record = create_graphrecord()

        with pytest.raises(KeyError, match="does not exist"):
            record.remove_plugin("ipsum")

    def test_add_nodes(self) -> None:
        record = GraphRecord()

        from_rows = record.add_nodes(create_nodes())
        from_collector = record.add_nodes(NodeRows())
        from_frame = record.add_nodes((create_nodes_frame(), "node_index"))
        from_batch = record.add_nodes(
            (create_graphrecord().to_arrow()["groups"]["magna"]["nodes"], "node_index")
        )
        from_arrow = record.add_nodes(
            (
                ArrowTable(
                    create_graphrecord().to_arrow()["groups"]["aliqua"]["nodes"]
                ),
                "node_index",
            )
        )

        assert from_rows.node_indices() == ["0", "1", "2", "3"]
        assert from_collector.node_indices() == ["0", "1", "2", "3"]
        assert from_frame.node_indices() == ["4", "5"]
        assert from_frame.node("4").attributes() == {"enim": "veniam"}
        assert from_batch.node_indices() == ["0", "1"]
        assert from_batch.node("0").attributes() == {
            "lorem": "ipsum",
            "dolor": "sit",
            "amet": None,
        }
        assert from_arrow.node_indices() == ["2"]
        assert record.node_count() == 0

    def test_invalid_add_nodes(self) -> None:
        record = create_graphrecord()

        with pytest.raises(ValueError, match="already exists"):
            record.add_nodes([("0", {})])

    def test_add_node(self) -> None:
        record = create_graphrecord()

        assert record.add_node("4", {"enim": "veniam"}).node("4").attributes() == {
            "enim": "veniam"
        }
        assert record.add_node(4, {}).node_indices() == ["0", "1", "2", "3", 4]
        assert record.add_node(
            cast("NodeIndex", IntegerScalar()), {"enim": cast("Value", FloatScalar())}
        ).node(4).attributes() == {"enim": 1.5}
        assert record.node_count() == 4

    def test_invalid_add_node(self) -> None:
        record = create_graphrecord()

        with pytest.raises(ValueError, match="already exists"):
            record.add_node("0", {})

        with pytest.raises(ValueError, match="No node selected"):
            record.add_node(
                nodes()
                .filter(nodes().index().equal_to("99"))
                .sort_by(nodes().index())
                .first(),
                {},
            )

    def test_add_nodes_in_group(self) -> None:
        record = GraphRecord().add_group("magna")

        extended = record.add_nodes_in_group(create_nodes(), "magna")

        assert extended.group("magna").nodes() == ["0", "1", "2", "3"]
        assert record.group("magna").nodes() == []
        assert record.add_nodes_in_group(
            (create_nodes_frame(), "node_index"),
            groups().sort_by(groups().index()).first(),
        ).group("magna").nodes() == ["4", "5"]

    def test_invalid_add_nodes_in_group(self) -> None:
        record = create_graphrecord()

        with pytest.raises(ValueError, match="already exists"):
            record.add_nodes_in_group([("0", {})], "magna")

    def test_add_node_in_group(self) -> None:
        record = create_graphrecord()

        extended = record.add_node_in_group("4", {"enim": "veniam"}, "magna")

        assert extended.group("magna").nodes() == ["0", "1", "4"]
        assert record.group("magna").nodes() == ["0", "1"]
        assert record.add_node_in_group(
            "4",
            {},
            groups()
            .filter(groups().index().equal_to("aliqua"))
            .sort_by(groups().index())
            .first(),
        ).group("aliqua").nodes() == ["2", "4"]

    def test_invalid_add_node_in_group(self) -> None:
        record = create_graphrecord()

        with pytest.raises(ValueError, match="No group selected"):
            record.add_node_in_group(
                "4",
                {},
                groups()
                .filter(groups().index().equal_to("enim"))
                .sort_by(groups().index())
                .first(),
            )

    def test_add_edges(self) -> None:
        record = GraphRecord().add_nodes(create_nodes())
        frame_record = GraphRecord().add_nodes((create_nodes_frame(), "node_index"))
        arrow_edges = ArrowTable(create_graphrecord().to_arrow()["ungrouped"]["edges"])

        from_rows = record.add_edges(create_edges())
        from_collector = record.add_edges(EdgeRows())
        from_frame = frame_record.add_edges(
            (create_edges_frame(), "source_node_index", "target_node_index")
        )
        from_arrow = record.add_edges(
            (arrow_edges, "source_node_index", "target_node_index")
        )

        assert from_rows.edge_count() == 4
        assert from_collector.edge_count() == 4
        assert from_frame.edge_count() == 2
        assert from_frame.edge(from_frame.edge_indices()[0]).attributes() == {
            "nostrud": "exercitation"
        }
        assert from_arrow.edge_count() == 3
        assert record.edge_count() == 0

    def test_invalid_add_edges(self) -> None:
        record = create_graphrecord()

        with pytest.raises(IndexError, match="Cannot find node with index"):
            record.add_edges([("0", "99", {})])

    def test_add_edge(self) -> None:
        record = create_graphrecord()

        extended = record.add_edge("2", "3", {"enim": "veniam"})

        assert extended.edge_count() == 5
        assert extended.edge(extended.edge_indices()[4]).attributes() == {
            "enim": "veniam"
        }
        assert record.edge_count() == 4
        assert (
            record.add_edge(
                nodes().sort_by(nodes().index()).first(),
                nodes().sort_by(nodes().index()).last(),
                {},
            ).edge_count()
            == 5
        )

    def test_invalid_add_edge(self) -> None:
        record = create_graphrecord()

        with pytest.raises(IndexError, match="Cannot find nodes with indices"):
            record.add_edge("0", "99", {})

    def test_add_edges_in_group(self) -> None:
        record = GraphRecord().add_nodes(create_nodes()).add_group("magna")

        extended = record.add_edges_in_group(create_edges(), "magna")

        assert extended.group("magna").edge_count() == 4
        assert record.group("magna").edge_count() == 0
        assert (
            record.add_edges_in_group(
                EdgeRows(), groups().sort_by(groups().index()).first()
            )
            .group("magna")
            .edge_count()
            == 4
        )

    def test_invalid_add_edges_in_group(self) -> None:
        record = create_graphrecord()

        with pytest.raises(IndexError, match="Cannot find node with index"):
            record.add_edges_in_group([("0", "99", {})], "magna")

    def test_add_edge_in_group(self) -> None:
        record = create_graphrecord()

        extended = record.add_edge_in_group("2", "3", {"enim": "veniam"}, "magna")

        assert extended.group("magna").edge_count() == 2
        assert record.group("magna").edge_count() == 1
        assert (
            record.add_edge_in_group(
                nodes().sort_by(nodes().index()).first(),
                "2",
                {},
                groups()
                .filter(groups().index().equal_to("aliqua"))
                .sort_by(groups().index())
                .first(),
            )
            .group("aliqua")
            .edge_count()
            == 1
        )

    def test_remove_nodes(self) -> None:
        record = create_graphrecord()

        assert record.remove_nodes("0").node_indices() == ["1", "2", "3"]
        assert record.remove_nodes(["0", "1"]).node_indices() == ["2", "3"]
        assert record.remove_nodes(
            nodes().filter(nodes().index().equal_to("2"))
        ).node_indices() == ["0", "1", "3"]
        assert record.remove_nodes(
            record.nodes().filter(nodes().index().equal_to("3"))
        ).node_indices() == ["0", "1", "2"]
        assert record.remove_nodes(
            edges().via_source_node().index()
        ).node_indices() == ["2", "3"]
        assert record.remove_nodes("0").edge_count() == 1
        assert record.node_indices() == ["0", "1", "2", "3"]

    def test_invalid_remove_nodes(self) -> None:
        record = create_graphrecord()

        with pytest.raises(IndexError, match="Cannot find nodes with indices"):
            record.remove_nodes("99")

    def test_remove_edges(self) -> None:
        record = create_graphrecord()

        assert record.remove_edges(record.edge_indices()[0]).edge_count() == 3
        assert record.remove_edges(record.edge_indices()[:2]).edge_count() == 2
        assert (
            record.remove_edges(
                edges().filter(edges().has_attribute("sed"))
            ).edge_count()
            == 2
        )
        assert (
            record.remove_edges(
                record.edges().filter(edges().has_attribute("ut"))
            ).edge_count()
            == 3
        )
        assert record.edge_count() == 4

    def test_invalid_remove_edges(self) -> None:
        record = create_graphrecord()
        foreign = GraphRecord().add_nodes([("0", {})]).add_edges([("0", "0", {})])

        with pytest.raises(IndexError, match="Cannot find edges with indices"):
            record.remove_edges(foreign.edge_indices()[0])

    def test_keep_nodes(self) -> None:
        record = create_graphrecord()

        assert record.keep_nodes("0").node_indices() == ["0"]
        assert record.keep_nodes(["0", "1"]).node_indices() == ["0", "1"]
        assert record.keep_nodes(["0", "1"]).edge_count() == 2
        assert record.keep_nodes(
            nodes().filter(nodes().index().is_in(["2", "3"]))
        ).node_indices() == ["2", "3"]
        assert record.keep_nodes(
            record.nodes().filter(nodes().index().equal_to("0"))
        ).node_indices() == ["0"]
        assert record.keep_nodes(
            nodes().has_attribute("lorem").on_missing(Drop())
        ).node_indices() == ["0", "2"]
        assert record.node_indices() == ["0", "1", "2", "3"]

    def test_invalid_keep_nodes(self) -> None:
        record = create_graphrecord()

        with pytest.raises(IndexError, match="Cannot find nodes with indices"):
            record.keep_nodes("99")

    def test_keep_edges(self) -> None:
        record = create_graphrecord()

        assert record.keep_edges(record.edge_indices()[0]).edge_count() == 1
        assert record.keep_edges(record.edge_indices()[:2]).edge_count() == 2
        assert (
            record.keep_edges(edges().filter(edges().has_attribute("sed"))).edge_count()
            == 2
        )
        assert (
            record.keep_edges(
                edges().has_attribute("sed").on_missing(Drop())
            ).edge_count()
            == 2
        )
        assert (
            record.keep_edges(
                record.edges().filter(edges().has_attribute("ut"))
            ).edge_count()
            == 1
        )
        assert record.keep_edges(record.edge_indices()[0]).node_count() == 4
        assert record.edge_count() == 4

    def test_invalid_keep_edges(self) -> None:
        record = create_graphrecord()
        foreign = GraphRecord().add_nodes([("0", {})]).add_edges([("0", "0", {})])

        with pytest.raises(IndexError, match="Cannot find edges with indices"):
            record.keep_edges(foreign.edge_indices()[0])

    def test_keep_groups(self) -> None:
        record = create_graphrecord()

        assert record.keep_groups("magna").group_indices() == ["magna"]
        assert record.keep_groups("magna").group("magna").nodes() == ["0", "1"]
        assert record.keep_groups(["magna", "aliqua"]).group_indices() == [
            "magna",
            "aliqua",
        ]
        assert record.keep_groups(
            groups().filter(groups().index().equal_to("aliqua"))
        ).group_indices() == ["aliqua"]
        assert record.keep_groups(
            record.groups().filter(groups().index().equal_to("magna"))
        ).group_indices() == ["magna"]
        assert record.group_indices() == ["magna", "aliqua"]

    def test_invalid_keep_groups(self) -> None:
        record = create_graphrecord()

        with pytest.raises(IndexError, match="Cannot find groups with indices"):
            record.keep_groups("enim")

    def test_intersect(self) -> None:
        record = create_graphrecord()
        other = (
            GraphRecord()
            .add_nodes([("1", {"amet": "consectetur"}), ("2", {"lorem": "adipiscing"})])
            .add_group("magna")
            .add_nodes_to_group("1", "magna")
        )

        shared = record.intersect(other)

        assert shared.node_indices() == ["1", "2"]
        assert shared.edge_count() == 1
        assert shared.group("magna").nodes() == ["1"]
        assert record.node_indices() == ["0", "1", "2", "3"]

    def test_difference(self) -> None:
        record = create_graphrecord()
        other = GraphRecord().add_nodes([("1", {}), ("2", {})])

        remaining = record.difference(other)

        assert remaining.node_indices() == ["0", "3"]
        assert remaining.edge_count() == 1
        assert record.node_indices() == ["0", "1", "2", "3"]

    def test_merge(self) -> None:
        record = create_graphrecord()
        other = GraphRecord().add_nodes(
            [("0", {"lorem": "elit"}), ("4", {"enim": "veniam"})]
        )

        assert record.merge(other, OnConflict.KeepSelf).node("0").attributes() == {
            "lorem": "ipsum",
            "dolor": "sit",
        }
        assert record.merge(other, OnConflict.KeepOther).node("0").attributes() == {
            "lorem": "elit",
            "dolor": "sit",
        }
        assert record.merge(other, OnConflict.KeepSelf).node_indices() == [
            "0",
            "1",
            "2",
            "3",
            "4",
        ]
        assert record.merge(GraphRecord().add_nodes([("4", {})])).node_count() == 5
        assert record.node_count() == 4

    def test_invalid_merge(self) -> None:
        record = create_graphrecord()
        other = GraphRecord().add_nodes([("0", {"lorem": "elit"})])

        with pytest.raises(ValueError, match="conflicts between"):
            record.merge(other, OnConflict.Raise)

    def test_set_node_attributes(self) -> None:
        record = create_graphrecord()

        assert record.set_node_attributes("0", {"lorem": "elit"}).node(
            "0"
        ).attributes() == {"lorem": "elit", "dolor": "sit"}
        assert record.set_node_attributes(["1", "2"], {"enim": "veniam"}).node(
            "1"
        ).attributes() == {"amet": "consectetur", "enim": "veniam"}
        assert record.set_node_attributes(
            nodes().filter(nodes().index().equal_to("3")), {"enim": "veniam"}
        ).node("3").attributes() == {"enim": "veniam"}
        assert record.set_node_attributes(
            record.nodes().filter(nodes().index().equal_to("3")), {"enim": "veniam"}
        ).node("3").attributes() == {"enim": "veniam"}
        assert record.node("0").attributes() == {"lorem": "ipsum", "dolor": "sit"}

    def test_invalid_set_node_attributes(self) -> None:
        record = create_graphrecord()

        with pytest.raises(IndexError, match="Cannot find nodes with indices"):
            record.set_node_attributes("99", {"enim": "veniam"})

    def test_replace_node_attributes(self) -> None:
        record = create_graphrecord()

        assert record.replace_node_attributes("0", {"enim": "veniam"}).node(
            "0"
        ).attributes() == {"enim": "veniam"}
        assert (
            record.replace_node_attributes(["0", "1"], {}).node("1").attributes() == {}
        )
        assert record.replace_node_attributes(
            nodes().filter(nodes().index().equal_to("2")), {"enim": "veniam"}
        ).node("2").attributes() == {"enim": "veniam"}
        assert record.node("0").attributes() == {"lorem": "ipsum", "dolor": "sit"}

    def test_invalid_replace_node_attributes(self) -> None:
        record = create_graphrecord()

        with pytest.raises(IndexError, match="Cannot find nodes with indices"):
            record.replace_node_attributes("99", {})

    def test_remove_node_attributes(self) -> None:
        record = create_graphrecord()

        assert record.remove_node_attributes("0", ["lorem"]).node("0").attributes() == {
            "dolor": "sit"
        }
        assert record.remove_node_attributes("0", {"lorem"}).node("0").attributes() == {
            "dolor": "sit"
        }
        assert (
            record.remove_node_attributes("0", (name for name in ["lorem", "dolor"]))
            .node("0")
            .attributes()
            == {}
        )
        assert (
            record.remove_node_attributes("0", ["lorem", "dolor"])
            .node("0")
            .attributes()
            == {}
        )
        assert (
            record.remove_node_attributes(
                nodes().filter(nodes().index().is_in(["0", "2"])), ["lorem"]
            )
            .node("2")
            .attributes()
            == {}
        )
        assert record.node("0").attributes() == {"lorem": "ipsum", "dolor": "sit"}

    def test_invalid_remove_node_attributes(self) -> None:
        record = create_graphrecord()

        with pytest.raises(KeyError, match="does not exist on node"):
            record.remove_node_attributes("0", ["enim"])

        with pytest.raises(TypeError, match="Expected attribute names"):
            record.remove_node_attributes("0", "lorem")

        with pytest.raises(TypeError, match="Expected attribute names"):
            record.remove_node_attributes("0", b"lorem")

    def test_set_edge_attributes(self) -> None:
        record = create_graphrecord()
        edge_index = record.edge_indices()[0]

        assert record.set_edge_attributes(edge_index, {"sed": "elit"}).edge(
            edge_index
        ).attributes() == {"sed": "elit", "eiusmod": "tempor"}
        assert record.set_edge_attributes(
            record.edge_indices()[:2], {"enim": "veniam"}
        ).edge(record.edge_indices()[1]).attributes() == {
            "sed": "incididunt",
            "enim": "veniam",
        }
        assert record.set_edge_attributes(
            edges().filter(edges().has_attribute("ut")), {"enim": "veniam"}
        ).edge(record.edge_indices()[2]).attributes() == {
            "ut": "labore",
            "enim": "veniam",
        }
        assert record.edge(edge_index).attributes() == {
            "sed": "do",
            "eiusmod": "tempor",
        }

    def test_invalid_set_edge_attributes(self) -> None:
        record = create_graphrecord()
        foreign = GraphRecord().add_nodes([("0", {})]).add_edges([("0", "0", {})])

        with pytest.raises(IndexError, match="Cannot find edges with indices"):
            record.set_edge_attributes(foreign.edge_indices()[0], {})

    def test_replace_edge_attributes(self) -> None:
        record = create_graphrecord()
        edge_index = record.edge_indices()[0]

        assert record.replace_edge_attributes(edge_index, {"enim": "veniam"}).edge(
            edge_index
        ).attributes() == {"enim": "veniam"}
        assert (
            record.replace_edge_attributes(record.edge_indices()[:2], {})
            .edge(record.edge_indices()[1])
            .attributes()
            == {}
        )
        assert record.replace_edge_attributes(
            record.edges().filter(edges().has_attribute("ut")), {"enim": "veniam"}
        ).edge(record.edge_indices()[2]).attributes() == {"enim": "veniam"}
        assert record.edge(edge_index).attributes() == {
            "sed": "do",
            "eiusmod": "tempor",
        }

    def test_invalid_replace_edge_attributes(self) -> None:
        record = create_graphrecord()
        foreign = GraphRecord().add_nodes([("0", {})]).add_edges([("0", "0", {})])

        with pytest.raises(IndexError, match="Cannot find edges with indices"):
            record.replace_edge_attributes(foreign.edge_indices()[0], {})

    def test_remove_edge_attributes(self) -> None:
        record = create_graphrecord()
        edge_index = record.edge_indices()[0]

        assert record.remove_edge_attributes(edge_index, ["sed"]).edge(
            edge_index
        ).attributes() == {"eiusmod": "tempor"}
        assert (
            record.remove_edge_attributes(record.edge_indices()[:2], ["sed"])
            .edge(record.edge_indices()[1])
            .attributes()
            == {}
        )
        assert (
            record.remove_edge_attributes(
                edges().filter(edges().has_attribute("ut")), ["ut"]
            )
            .edge(record.edge_indices()[2])
            .attributes()
            == {}
        )
        assert record.edge(edge_index).attributes() == {
            "sed": "do",
            "eiusmod": "tempor",
        }

    def test_invalid_remove_edge_attributes(self) -> None:
        record = create_graphrecord()

        with pytest.raises(KeyError, match="does not exist on edge"):
            record.remove_edge_attributes(record.edge_indices()[0], ["enim"])

    def test_add_group(self) -> None:
        record = create_graphrecord()

        extended = record.add_group("enim")

        assert extended.group_indices() == ["magna", "aliqua", "enim"]
        assert extended.group("enim").node_count() == 0
        assert record.group_indices() == ["magna", "aliqua"]
        assert record.add_group(4).group_indices() == ["magna", "aliqua", 4]

    def test_invalid_add_group(self) -> None:
        record = create_graphrecord()

        with pytest.raises(ValueError, match="already exists"):
            record.add_group("magna")

    def test_remove_groups(self) -> None:
        record = create_graphrecord()

        reduced = record.remove_groups("magna")

        assert reduced.group_indices() == ["aliqua"]
        assert reduced.node_indices() == ["0", "1", "2", "3"]
        assert record.remove_groups(["magna", "aliqua"]).group_indices() == []
        assert record.remove_groups(
            groups().filter(groups().index().equal_to("aliqua"))
        ).group_indices() == ["magna"]
        assert record.group_indices() == ["magna", "aliqua"]

    def test_invalid_remove_groups(self) -> None:
        record = create_graphrecord()

        with pytest.raises(IndexError, match="Cannot find groups with indices"):
            record.remove_groups("enim")

    def test_add_nodes_to_group(self) -> None:
        record = create_graphrecord()

        assert record.add_nodes_to_group("3", "magna").group("magna").nodes() == [
            "0",
            "1",
            "3",
        ]
        assert (
            record.add_nodes_to_group(["2", "3"], "magna").group("magna").node_count()
            == 4
        )
        assert record.add_nodes_to_group(
            nodes().filter(nodes().index().equal_to("3")),
            groups()
            .filter(groups().index().equal_to("aliqua"))
            .sort_by(groups().index())
            .first(),
        ).group("aliqua").nodes() == ["2", "3"]
        assert record.add_nodes_to_group("3", "tempor").group("tempor").nodes() == ["3"]
        assert record.group("magna").nodes() == ["0", "1"]

    def test_invalid_add_nodes_to_group(self) -> None:
        record = create_graphrecord()

        with pytest.raises(ValueError, match="already in group"):
            record.add_nodes_to_group("0", "magna")

    def test_remove_nodes_from_group(self) -> None:
        record = create_graphrecord()

        reduced = record.remove_nodes_from_group("0", "magna")

        assert reduced.group("magna").nodes() == ["1"]
        assert reduced.node_indices() == ["0", "1", "2", "3"]
        assert (
            record.remove_nodes_from_group(["0", "1"], "magna").group("magna").nodes()
            == []
        )
        assert record.remove_nodes_from_group(
            record.nodes().filter(nodes().index().equal_to("0")), "magna"
        ).group("magna").nodes() == ["1"]
        assert record.group("magna").nodes() == ["0", "1"]

    def test_invalid_remove_nodes_from_group(self) -> None:
        record = create_graphrecord()

        with pytest.raises(ValueError, match="not in group"):
            record.remove_nodes_from_group("3", "magna")

    def test_add_edges_to_group(self) -> None:
        record = create_graphrecord()

        assert (
            record.add_edges_to_group(record.edge_indices()[1], "magna")
            .group("magna")
            .edge_count()
            == 2
        )
        assert (
            record.add_edges_to_group(record.edge_indices()[1:], "magna")
            .group("magna")
            .edge_count()
            == 4
        )
        assert record.add_edges_to_group(
            edges().filter(edges().has_attribute("ut")), "aliqua"
        ).group("aliqua").edges() == [record.edge_indices()[2]]
        assert record.add_edges_to_group(record.edge_indices()[0], "tempor").group(
            "tempor"
        ).edges() == [record.edge_indices()[0]]
        assert record.group("magna").edge_count() == 1

    def test_invalid_add_edges_to_group(self) -> None:
        record = create_graphrecord()

        with pytest.raises(ValueError, match="already in group"):
            record.add_edges_to_group(record.edge_indices()[0], "magna")

    def test_remove_edges_from_group(self) -> None:
        record = create_graphrecord()

        reduced = record.remove_edges_from_group(record.edge_indices()[0], "magna")

        assert reduced.group("magna").edges() == []
        assert reduced.edge_count() == 4
        assert (
            record.remove_edges_from_group(
                record.edges().filter(edges().has_attribute("eiusmod")), "magna"
            )
            .group("magna")
            .edge_count()
            == 0
        )
        assert record.group("magna").edge_count() == 1

    def test_invalid_remove_edges_from_group(self) -> None:
        record = create_graphrecord()

        with pytest.raises(ValueError, match="not in group"):
            record.remove_edges_from_group(record.edge_indices()[1], "magna")

    def test_schema(self) -> None:
        record = create_graphrecord()

        assert record.schema.schema_type == SchemaType.Inferred
        assert sorted(record.schema.ungrouped.nodes) == ["amet", "dolor", "lorem"]
        assert sorted(record.schema.groups) == ["aliqua", "magna"]
        assert GraphRecord().schema.ungrouped.nodes == {}

    def test_set_schema(self) -> None:
        record = GraphRecord().add_node("0", {"lorem": "ipsum"})

        adopted = record.set_schema(create_schema())

        assert adopted.schema.schema_type == SchemaType.Provided
        assert list(adopted.schema.ungrouped.nodes) == ["lorem"]
        assert record.schema.schema_type == SchemaType.Inferred

    def test_invalid_set_schema(self) -> None:
        record = create_graphrecord()

        with pytest.raises(ValueError, match="is not defined in the schema"):
            record.set_schema(create_schema())

    def test_freeze_schema(self) -> None:
        record = create_graphrecord()

        frozen = record.freeze_schema()

        assert frozen.schema.schema_type == SchemaType.Provided
        assert record.schema.schema_type == SchemaType.Inferred

        with pytest.raises(ValueError, match="do not exist in schema"):
            frozen.add_node("4", {"enim": "veniam"})

    def test_unfreeze_schema(self) -> None:
        record = create_graphrecord().freeze_schema()

        unfrozen = record.unfreeze_schema()

        assert unfrozen.schema.schema_type == SchemaType.Inferred
        assert record.schema.schema_type == SchemaType.Provided
        assert unfrozen.add_node("4", {"enim": "veniam"}).node_count() == 5

    def test_clear(self) -> None:
        record = (
            create_graphrecord().add_plugin("ipsum", RecordingPlugin()).freeze_schema()
        )

        cleared = record.clear()

        assert cleared.node_count() == 0
        assert cleared.edge_count() == 0
        assert cleared.group_count() == 0
        assert cleared.plugins == ["ipsum"]
        assert cleared.schema.schema_type == SchemaType.Provided
        assert record.node_count() == 4

    def test_compact(self) -> None:
        record = create_graphrecord().remove_nodes("0")

        compacted = record.compact()

        assert compacted.node_indices() == ["1", "2", "3"]
        assert compacted.edge_count() == 1
        assert compacted.group("magna").nodes() == ["1"]
        assert compacted.edge_indices() != record.edge_indices()
        assert record.node_indices() == ["1", "2", "3"]
        assert record.edge_count() == 1

    def test_node_count(self) -> None:
        assert create_graphrecord().node_count() == 4
        assert GraphRecord().node_count() == 0

    def test_edge_count(self) -> None:
        assert create_graphrecord().edge_count() == 4
        assert GraphRecord().edge_count() == 0

    def test_group_count(self) -> None:
        assert create_graphrecord().group_count() == 2
        assert GraphRecord().group_count() == 0

    def test_contains_node(self) -> None:
        record = create_graphrecord()

        assert record.contains_node("0")
        assert not record.contains_node("99")

    def test_contains_edge(self) -> None:
        record = create_graphrecord()
        foreign = GraphRecord().add_nodes([("0", {})]).add_edges([("0", "0", {})])

        assert record.contains_edge(record.edge_indices()[0])
        assert not record.contains_edge(foreign.edge_indices()[0])

    def test_contains_group(self) -> None:
        record = create_graphrecord()

        assert record.contains_group("magna")
        assert not record.contains_group("enim")

    def test_node_indices(self) -> None:
        assert create_graphrecord().node_indices() == ["0", "1", "2", "3"]
        assert GraphRecord().node_indices() == []

    def test_edge_indices(self) -> None:
        record = create_graphrecord()

        edge_indices = record.edge_indices()

        assert len(edge_indices) == 4
        assert all(isinstance(edge_index, EdgeIndex) for edge_index in edge_indices)
        assert edge_indices == record.edge_indices()
        assert GraphRecord().edge_indices() == []

    def test_group_indices(self) -> None:
        assert create_graphrecord().group_indices() == ["magna", "aliqua"]
        assert GraphRecord().group_indices() == []

    def test_nodes(self) -> None:
        record = create_graphrecord()

        series = record.nodes()

        assert isinstance(series, Series)
        assert [element[0] for element in series.index().evaluate()] == [
            "0",
            "1",
            "2",
            "3",
        ]

    def test_edges(self) -> None:
        record = create_graphrecord()

        series = record.edges()

        assert isinstance(series, Series)
        assert [element[0] for element in series.index().evaluate()] == (
            record.edge_indices()
        )

    def test_groups(self) -> None:
        record = create_graphrecord()

        series = record.groups()

        assert isinstance(series, Series)
        assert [element[0] for element in series.index().evaluate()] == [
            "magna",
            "aliqua",
        ]

    def test_query(self) -> None:
        record = create_graphrecord()

        series = record.query(nodes().filter(nodes().index().is_in(["0", "2"])))

        assert isinstance(series, Series)
        assert [element[0] for element in series.index().evaluate()] == ["0", "2"]

    def test_node(self) -> None:
        record = create_graphrecord()

        assert isinstance(record.node("0"), NodeView)
        assert record.node("0").index() == "0"

    def test_invalid_node(self) -> None:
        record = create_graphrecord()

        with pytest.raises(IndexError, match="Cannot find node with index"):
            record.node("99")

    def test_edge(self) -> None:
        record = create_graphrecord()

        assert isinstance(record.edge(record.edge_indices()[0]), EdgeView)
        assert record.edge(record.edge_indices()[0]).source() == "0"

    def test_invalid_edge(self) -> None:
        record = create_graphrecord()
        foreign = GraphRecord().add_nodes([("0", {})]).add_edges([("0", "0", {})])

        with pytest.raises(IndexError, match="Cannot find edge with index"):
            record.edge(foreign.edge_indices()[0])

    def test_group(self) -> None:
        record = create_graphrecord()

        assert isinstance(record.group("magna"), GroupView)
        assert record.group("magna").index() == "magna"

    def test_invalid_group(self) -> None:
        record = create_graphrecord()

        with pytest.raises(IndexError, match="Cannot find group with index"):
            record.group("enim")

    def test_export(self) -> None:
        record = create_graphrecord()

        assert record.export(CountingWriter()) == (4, 4)
        assert sorted(record.export(PolarsFrames())) == ["groups", "ungrouped"]
        assert len(record.export(ArrowTables())["ungrouped"]["nodes"]) == 1

        with TemporaryDirectory() as directory:
            path = Path(directory) / "record.ron"

            assert record.export(RonFile(str(path))) is None
            assert GraphRecord.from_ron(str(path)).node_count() == 4

    def test_invalid_export(self) -> None:
        record = create_graphrecord()

        with pytest.raises(RuntimeError, match="lorem ipsum 4"):
            record.export(FailingWriter())

    def test_to_polars(self) -> None:
        record = create_graphrecord()

        export = record.to_polars()

        assert sorted(export) == ["groups", "ungrouped"]
        assert export["ungrouped"]["nodes"].columns == [
            "node_index",
            "amet",
            "dolor",
            "lorem",
        ]
        assert export["ungrouped"]["nodes"].height == 1
        assert export["ungrouped"]["edges"].columns == [
            "source_node_index",
            "target_node_index",
            "eiusmod",
            "sed",
            "ut",
        ]
        assert export["ungrouped"]["edges"].height == 3
        assert sorted(export["groups"]) == ["aliqua", "magna"]
        assert export["groups"]["magna"]["nodes"].height == 2
        assert export["groups"]["magna"]["edges"].height == 1

    def test_to_arrow(self) -> None:
        record = create_graphrecord()

        export = record.to_arrow()

        assert sorted(export) == ["groups", "ungrouped"]
        assert isinstance(export["ungrouped"]["nodes"], RecordBatch)
        assert isinstance(export["ungrouped"]["edges"], RecordBatch)
        assert sorted(export["groups"]) == ["aliqua", "magna"]
        assert isinstance(export["groups"]["aliqua"]["nodes"], RecordBatch)
        assert len(export["groups"]["aliqua"]["edges"]) == 0

    def test_to_pandas(self) -> None:
        record = create_graphrecord()

        export = record.to_pandas()

        assert sorted(export) == ["groups", "ungrouped"]
        assert list(export["ungrouped"]["nodes"].columns) == [
            "node_index",
            "amet",
            "dolor",
            "lorem",
        ]
        assert len(export["ungrouped"]["nodes"]) == 1
        assert len(export["ungrouped"]["edges"]) == 3
        assert sorted(export["groups"]) == ["aliqua", "magna"]
        assert len(export["groups"]["magna"]["nodes"]) == 2
        assert len(export["groups"]["aliqua"]["edges"]) == 0

    def test_to_ron(self) -> None:
        record = create_graphrecord()

        with TemporaryDirectory() as directory:
            path = Path(directory) / "record.ron"
            record.to_ron(str(path))

            assert path.exists()
            assert GraphRecord.from_ron(str(path)).node_indices() == [
                "0",
                "1",
                "2",
                "3",
            ]

    def test_invalid_to_ron(self) -> None:
        record = create_graphrecord()

        with (
            TemporaryDirectory() as directory,
            pytest.raises(OSError, match="Failed to write file"),
        ):
            record.to_ron(directory)

    def test_overview(self) -> None:
        record = create_graphrecord()

        overview = record.overview()

        assert sorted(overview.grouped_overviews) == ["aliqua", "magna"]
        assert overview.ungrouped_overview.node_overview.count == 1
        assert overview.ungrouped_overview.edge_overview.count == 3
        assert overview.grouped_overviews["magna"].node_overview.count == 2
        assert record.overview(None).ungrouped_overview.node_overview.count == 1
        assert record.overview(4).ungrouped_overview.node_overview.count == 1

    def test_group_overview(self) -> None:
        record = create_graphrecord()

        group_overview = record.group_overview("magna")

        assert group_overview.node_overview.count == 2
        assert group_overview.edge_overview.count == 1
        assert record.group_overview("magna", None).node_overview.count == 2
        assert record.group_overview("aliqua", 4).edge_overview.count == 0

    def test_invalid_group_overview(self) -> None:
        record = create_graphrecord()

        with pytest.raises(IndexError, match="Cannot find group with index"):
            record.group_overview("enim")

    def test_eq(self) -> None:
        record = create_graphrecord()

        assert record == copy.copy(record)
        assert record == record.add_node("4", {}).remove_nodes("4")
        assert record != record.add_node("4", {})
        assert record != GraphRecord()
        assert GraphRecord() == GraphRecord()
        assert record.__eq__("lorem") is NotImplemented

    def test_hash(self) -> None:
        record = create_graphrecord()

        with pytest.raises(TypeError, match="unhashable type"):
            hash(record)

    def test_copy(self) -> None:
        record = create_graphrecord()

        copied = copy.copy(record)

        assert copied is not record
        assert copied == record
        assert copied.add_node("4", {}).node_count() == 5
        assert record.node_count() == 4

    def test_deepcopy(self) -> None:
        record = create_graphrecord()

        copied = copy.deepcopy(record)

        assert copied is not record
        assert copied == record
        assert record.__deepcopy__() == record
        assert record.__deepcopy__({}) == record

    def test_reduce(self) -> None:
        record = create_graphrecord()

        restored = pickle.loads(pickle.dumps(record))

        assert restored == record
        assert restored.node_indices() == ["0", "1", "2", "3"]

        with_plugin = record.add_plugin("ipsum", RecordingPlugin())
        restored_with_plugin = pickle.loads(pickle.dumps(with_plugin))

        assert restored_with_plugin.plugins == ["ipsum"]
        assert restored_with_plugin == with_plugin

    def test_repr(self) -> None:
        record = create_graphrecord()

        assert "Node Overview" in repr(record)
        assert "Edge Overview" in repr(record)
        assert "magna" in repr(record)
        assert "Ungrouped" in repr(record)
        assert "Node Overview" in repr(GraphRecord())
