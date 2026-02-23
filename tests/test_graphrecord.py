import tempfile
import unittest
from typing import List, Tuple

import pandas as pd
import polars as pl
import pytest

from graphrecords import GraphRecord
from graphrecords._graphrecords import PyGraphRecord
from graphrecords.builder import GraphRecordBuilder
from graphrecords.datatype import Int
from graphrecords.graphrecord import EdgesDirected
from graphrecords.plugins import Plugin, PostAddNodesContext, PreAddNodesContext
from graphrecords.querying import (
    EdgeAttributesTreeOperand,
    EdgeIndexOperand,
    EdgeIndicesOperand,
    EdgeMultipleAttributesWithIndexOperand,
    EdgeMultipleValuesWithIndexOperand,
    EdgeOperand,
    EdgeSingleAttributeWithIndexOperand,
    EdgeSingleValueWithIndexOperand,
    NodeAttributesTreeOperand,
    NodeIndexOperand,
    NodeIndicesOperand,
    NodeMultipleAttributesWithIndexOperand,
    NodeMultipleValuesWithIndexOperand,
    NodeOperand,
    NodeSingleAttributeWithIndexOperand,
    NodeSingleValueWithIndexOperand,
    QueryReturnOperand,
)
from graphrecords.schema import AttributeType, GroupSchema, Schema, SchemaType
from graphrecords.types import (
    AttributesInput,
    NodeIndex,
    is_edge_index_list,
    is_node_index_list,
)


# TODO(#397): Change AttributesInput to Attributes
def create_nodes() -> List[Tuple[NodeIndex, AttributesInput]]:
    return [
        ("0", {"lorem": "ipsum", "dolor": "sit"}),
        ("1", {"amet": "consectetur"}),
        ("2", {"adipiscing": "elit"}),
        ("3", {}),
    ]


# TODO(#397): Change AttributesInput to Attributes
def create_edges() -> List[Tuple[NodeIndex, NodeIndex, AttributesInput]]:
    return [
        ("0", "1", {"sed": "do", "eiusmod": "tempor"}),
        ("1", "0", {"sed": "do", "eiusmod": "tempor"}),
        ("1", "2", {"incididunt": "ut"}),
        ("0", "3", {}),
    ]


def create_pandas_nodes_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "index": ["0", "1"],
            "attribute": [1, 2],
        }
    )


def create_second_pandas_nodes_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "index": ["2", "3"],
            "attribute": [2, 3],
        }
    )


def create_pandas_edges_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "source": ["0", "1"],
            "target": ["1", "0"],
            "attribute": [1, 2],
        }
    )


def create_second_pandas_edges_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "source": ["0", "1"],
            "target": ["1", "0"],
            "attribute": [2, 3],
        }
    )


def create_graphrecord() -> GraphRecord:
    return GraphRecord.from_tuples(create_nodes(), create_edges())


class TestGraphRecord(unittest.TestCase):
    def test_from_py_graphrecord(self) -> None:
        py_graphrecord = PyGraphRecord()

        graphrecord = GraphRecord._from_py_graphrecord(py_graphrecord)
        assert isinstance(graphrecord, GraphRecord)
        assert graphrecord.node_count() == 0
        assert graphrecord.edge_count() == 0

    def test_builder(self) -> None:
        graphrecord_builder = GraphRecord().builder()

        assert isinstance(graphrecord_builder, GraphRecordBuilder)

        nodes = create_nodes()

        graphrecord = graphrecord_builder.add_nodes(nodes=nodes).build()

        assert isinstance(graphrecord, GraphRecord)
        assert graphrecord.node_count() == len(nodes)
        assert graphrecord.edge_count() == 0

    def test_from_tuples(self) -> None:
        graphrecord = create_graphrecord()

        assert graphrecord.node_count() == 4
        assert graphrecord.edge_count() == 4

    def test_invalid_from_tuples(self) -> None:
        nodes = create_nodes()

        # Adding an edge pointing to a non-existent node should fail
        with pytest.raises(IndexError):
            GraphRecord.from_tuples(nodes, [("0", "50", {})])

        # Adding an edge from a non-existing node should fail
        with pytest.raises(IndexError):
            GraphRecord.from_tuples(nodes, [("50", "0", {})])

    def test_from_pandas(self) -> None:
        graphrecord = GraphRecord.from_pandas(
            (create_pandas_nodes_dataframe(), "index"),
        )

        assert graphrecord.node_count() == 2
        assert graphrecord.edge_count() == 0

        graphrecord = GraphRecord.from_pandas(
            [
                (create_pandas_nodes_dataframe(), "index"),
                (create_second_pandas_nodes_dataframe(), "index"),
            ],
        )

        assert graphrecord.node_count() == 4
        assert graphrecord.edge_count() == 0

        graphrecord = GraphRecord.from_pandas(
            (create_pandas_nodes_dataframe(), "index"),
            (create_pandas_edges_dataframe(), "source", "target"),
        )

        assert graphrecord.node_count() == 2
        assert graphrecord.edge_count() == 2

        graphrecord = GraphRecord.from_pandas(
            [
                (create_pandas_nodes_dataframe(), "index"),
                (create_second_pandas_nodes_dataframe(), "index"),
            ],
            (create_pandas_edges_dataframe(), "source", "target"),
        )

        assert graphrecord.node_count() == 4
        assert graphrecord.edge_count() == 2

        graphrecord = GraphRecord.from_pandas(
            (create_pandas_nodes_dataframe(), "index"),
            [
                (create_pandas_edges_dataframe(), "source", "target"),
                (create_second_pandas_edges_dataframe(), "source", "target"),
            ],
        )

        assert graphrecord.node_count() == 2
        assert graphrecord.edge_count() == 4

        graphrecord = GraphRecord.from_pandas(
            [
                (create_pandas_nodes_dataframe(), "index"),
                (create_second_pandas_nodes_dataframe(), "index"),
            ],
            [
                (create_pandas_edges_dataframe(), "source", "target"),
                (create_second_pandas_edges_dataframe(), "source", "target"),
            ],
        )

        assert graphrecord.node_count() == 4
        assert graphrecord.edge_count() == 4

    def test_from_polars(self) -> None:
        nodes = pl.from_pandas(create_pandas_nodes_dataframe())
        second_nodes = pl.from_pandas(create_second_pandas_nodes_dataframe())
        edges = pl.from_pandas(create_pandas_edges_dataframe())
        second_edges = pl.from_pandas(create_second_pandas_edges_dataframe())

        graphrecord = GraphRecord.from_polars(
            (nodes, "index"), (edges, "source", "target")
        )

        assert graphrecord.node_count() == 2
        assert graphrecord.edge_count() == 2

        graphrecord = GraphRecord.from_polars(
            [(nodes, "index"), (second_nodes, "index")], (edges, "source", "target")
        )

        assert graphrecord.node_count() == 4
        assert graphrecord.edge_count() == 2

        graphrecord = GraphRecord.from_polars(
            (nodes, "index"),
            [(edges, "source", "target"), (second_edges, "source", "target")],
        )

        assert graphrecord.node_count() == 2
        assert graphrecord.edge_count() == 4

        graphrecord = GraphRecord.from_polars(
            [(nodes, "index"), (second_nodes, "index")],
            [(edges, "source", "target"), (second_edges, "source", "target")],
        )

        assert graphrecord.node_count() == 4
        assert graphrecord.edge_count() == 4

        graphrecord = GraphRecord.from_polars(
            (nodes, "index"),
        )

        assert graphrecord.node_count() == 2
        assert graphrecord.edge_count() == 0

    def test_invalid_from_polars(self) -> None:
        nodes = pl.from_pandas(create_pandas_nodes_dataframe())
        second_nodes = pl.from_pandas(create_second_pandas_nodes_dataframe())
        edges = pl.from_pandas(create_pandas_edges_dataframe())
        second_edges = pl.from_pandas(create_second_pandas_edges_dataframe())

        # Providing the wrong node index column name should fail
        with pytest.raises(RuntimeError):
            GraphRecord.from_polars((nodes, "invalid"), (edges, "source", "target"))

        # Providing the wrong node index column name should fail
        with pytest.raises(RuntimeError):
            GraphRecord.from_polars(
                [(nodes, "index"), (second_nodes, "invalid")],
                (edges, "source", "target"),
            )

        # Providing the wrong source index column name should fail
        with pytest.raises(RuntimeError):
            GraphRecord.from_polars((nodes, "index"), (edges, "invalid", "target"))

        # Providing the wrong source index column name should fail
        with pytest.raises(RuntimeError):
            GraphRecord.from_polars(
                (nodes, "index"),
                [(edges, "source", "target"), (second_edges, "invalid", "target")],
            )

        # Providing the wrong target index column name should fail
        with pytest.raises(RuntimeError):
            GraphRecord.from_polars((nodes, "index"), (edges, "source", "invalid"))

        # Providing the wrong target index column name should fail
        with pytest.raises(RuntimeError):
            GraphRecord.from_polars(
                (nodes, "index"),
                [(edges, "source", "target"), (edges, "source", "invalid")],
            )

    def test_ron(self) -> None:
        graphrecord = create_graphrecord()

        with tempfile.NamedTemporaryFile() as f:
            graphrecord.to_ron(f.name)

            loaded_graphrecord = GraphRecord.from_ron(f.name)

        assert graphrecord.node_count() == loaded_graphrecord.node_count()
        assert graphrecord.edge_count() == loaded_graphrecord.edge_count()

    def test_to_polars(self) -> None:
        graphrecord = create_graphrecord()

        export = graphrecord.to_polars()

        assert "ungrouped" in export
        assert "nodes" in export["ungrouped"]
        assert "edges" in export["ungrouped"]

        nodes_df = export["ungrouped"]["nodes"]
        edges_df = export["ungrouped"]["edges"]

        assert isinstance(nodes_df, pl.DataFrame)
        assert isinstance(edges_df, pl.DataFrame)

        assert nodes_df.shape[0] == graphrecord.node_count()
        assert edges_df.shape[0] == graphrecord.edge_count()

    def test_to_pandas(self) -> None:
        graphrecord = create_graphrecord()

        export = graphrecord.to_pandas()

        assert "ungrouped" in export
        assert "nodes" in export["ungrouped"]
        assert "edges" in export["ungrouped"]

        nodes_df = export["ungrouped"]["nodes"]
        edges_df = export["ungrouped"]["edges"]

        assert isinstance(nodes_df, pd.DataFrame)
        assert isinstance(edges_df, pd.DataFrame)

        assert nodes_df.shape[0] == graphrecord.node_count()
        assert edges_df.shape[0] == graphrecord.edge_count()

    def test_schema(self) -> None:
        graphrecord = GraphRecord()

        group_schema = GroupSchema(
            nodes={"attribute": Int()}, edges={"attribute": Int()}
        )

        graphrecord.add_nodes([("0", {"attribute": 1}), ("1", {"attribute": 1})])
        graphrecord.add_edges(("0", "1", {"attribute": 1}))

        schema = Schema(ungrouped=group_schema, schema_type=SchemaType.Provided)

        graphrecord.set_schema(schema)

        assert graphrecord.get_schema().ungrouped.nodes == {
            "attribute": (Int(), AttributeType.Continuous)
        }
        assert graphrecord.get_schema().ungrouped.edges == {
            "attribute": (Int(), AttributeType.Continuous)
        }

        graphrecord = GraphRecord()

        graphrecord.add_nodes(
            [("0", {"attribute": 1}), ("1", {"attribute": 1}), ("2", {"attribute": 1})]
        )
        graphrecord.add_edges(
            [
                ("0", "1", {"attribute": 1}),
                ("0", "1", {"attribute": 1}),
                ("0", "1", {"attribute": 1}),
            ]
        )

        schema = Schema(
            groups={"0": group_schema, "1": group_schema},
            ungrouped=group_schema,
            schema_type=SchemaType.Inferred,
        )

        graphrecord.add_group("0", ["0", "1"], [0, 1])
        graphrecord.add_group("1", ["0", "1"], [0, 1])

        inferred_schema = Schema(schema_type=SchemaType.Inferred)

        graphrecord.set_schema(inferred_schema)

        schema = graphrecord.get_schema()

        assert schema.group("0").nodes == {
            "attribute": (Int(), AttributeType.Continuous)
        }
        assert schema.group("0").edges == {
            "attribute": (Int(), AttributeType.Continuous)
        }
        assert schema.group("1").nodes == {
            "attribute": (Int(), AttributeType.Continuous)
        }
        assert schema.group("1").edges == {
            "attribute": (Int(), AttributeType.Continuous)
        }
        assert schema.ungrouped.nodes == {
            "attribute": (Int(), AttributeType.Continuous)
        }
        assert schema.ungrouped.edges == {
            "attribute": (Int(), AttributeType.Continuous)
        }
        assert schema.schema_type == inferred_schema.schema_type

    def test_invalid_schema(self) -> None:
        graphrecord = GraphRecord()

        graphrecord.add_nodes(("0", {"attribute2": 1}))

        schema = Schema(
            ungrouped=GroupSchema(
                nodes={"attribute": Int()}, edges={"attribute": Int()}
            ),
            schema_type=SchemaType.Provided,
        )

        with pytest.raises(
            ValueError,
            match=r"Attribute [^\s]+ of type [^\s]+ not found on node with index [^\s]+",
        ):
            graphrecord.set_schema(schema)

        assert graphrecord.get_schema().ungrouped.nodes == {
            "attribute2": (Int(), AttributeType.Continuous)
        }
        assert graphrecord.get_schema().ungrouped.edges == {}
        assert len(graphrecord.get_schema().groups) == 0
        assert graphrecord.get_schema().schema_type == SchemaType.Inferred

        graphrecord = GraphRecord()

        graphrecord.add_nodes([("0", {"attribute": 1}), ("1", {"attribute": 1})])
        graphrecord.add_edges(("0", "1", {"attribute2": 1}))

        with pytest.raises(
            ValueError,
            match=r"Attribute [^\s]+ of type [^\s]+ not found on edge with index [^\s]+",
        ):
            graphrecord.set_schema(schema)

        schema = graphrecord.get_schema()

        assert schema.ungrouped.nodes == {
            "attribute": (Int(), AttributeType.Continuous)
        }
        assert schema.ungrouped.edges == {
            "attribute2": (Int(), AttributeType.Continuous)
        }
        assert len(schema.groups) == 0
        assert schema.schema_type == SchemaType.Inferred

    def test_freeze_schema(self) -> None:
        graphrecord = GraphRecord()

        assert graphrecord.get_schema().schema_type == SchemaType.Inferred

        graphrecord.freeze_schema()

        assert graphrecord.get_schema().schema_type == SchemaType.Provided

    def test_unfreeze_schema(self) -> None:
        graphrecord = GraphRecord.with_schema(Schema(schema_type=SchemaType.Provided))

        assert graphrecord.get_schema().schema_type == SchemaType.Provided

        graphrecord.unfreeze_schema()

        assert graphrecord.get_schema().schema_type == SchemaType.Inferred

    def test_nodes(self) -> None:
        graphrecord = create_graphrecord()

        nodes = [x[0] for x in create_nodes()]

        for node in graphrecord.nodes:
            assert node in nodes

    def test_edges(self) -> None:
        graphrecord = create_graphrecord()

        edges = list(range(len(create_edges())))

        for edge in graphrecord.edges:
            assert edge in edges

    def test_groups(self) -> None:
        graphrecord = create_graphrecord()

        graphrecord.add_group("0")

        assert graphrecord.groups == ["0"]

    def test_group(self) -> None:
        graphrecord = create_graphrecord()

        graphrecord.add_group("0")

        assert graphrecord.group("0") == {"nodes": [], "edges": []}

        graphrecord.add_group("1", ["0"], [0])

        assert graphrecord.group("1") == {"nodes": ["0"], "edges": [0]}

        assert graphrecord.group(["0", "1"]) == {
            "0": {"nodes": [], "edges": []},
            "1": {"nodes": ["0"], "edges": [0]},
        }

    def test_invalid_group(self) -> None:
        graphrecord = create_graphrecord()

        # Querying a non-existing group should fail
        with pytest.raises(IndexError):
            graphrecord.group("0")

        graphrecord.add_group("1", ["0"])

        # Querying a non-existing group should fail
        with pytest.raises(IndexError):
            graphrecord.group(["0", "50"])

    def test_outgoing_edges(self) -> None:
        graphrecord = create_graphrecord()

        edges = graphrecord.outgoing_edges("0")

        assert sorted([0, 3]) == sorted(edges)

        edges = graphrecord.outgoing_edges(["0", "1"])

        assert {key: sorted(value) for key, value in edges.items()} == {
            "0": sorted([0, 3]),
            "1": [1, 2],
        }

        def query(node: NodeOperand) -> NodeIndicesOperand:
            node.index().is_in(["0", "1"])

            return node.index()

        edges = graphrecord.outgoing_edges(query)

        assert {key: sorted(value) for key, value in edges.items()} == {
            "0": sorted([0, 3]),
            "1": [1, 2],
        }

        def query2(node: NodeOperand) -> NodeIndexOperand:
            node.index().equal_to("0")

            return node.index().max()

        edges = graphrecord.outgoing_edges(query2)
        assert sorted(edges) == [0, 3]

        def query3(node: NodeOperand) -> NodeIndexOperand:
            max_index = node.index().max()
            max_index.equal_to("non-found")

            return max_index

        edges = graphrecord.outgoing_edges(query3)

        assert edges == []

    def test_invalid_outgoing_edges(self) -> None:
        graphrecord = create_graphrecord()

        # Querying outgoing edges of a non-existing node should fail
        with pytest.raises(IndexError):
            graphrecord.outgoing_edges("50")

        # Querying outgoing edges of a non-existing node should fail
        with pytest.raises(IndexError):
            graphrecord.outgoing_edges(["0", "50"])

    def test_incoming_edges(self) -> None:
        graphrecord = create_graphrecord()

        edges = graphrecord.incoming_edges("1")

        assert edges == [0]

        edges = graphrecord.incoming_edges(["1", "2"])

        assert edges == {"1": [0], "2": [2]}

        def query(node: NodeOperand) -> NodeIndicesOperand:
            node.index().is_in(["1", "2"])

            return node.index()

        edges = graphrecord.incoming_edges(query)

        assert edges == {"1": [0], "2": [2]}

        def query2(node: NodeOperand) -> NodeIndexOperand:
            node.index().equal_to("0")

            return node.index().max()

        edges = graphrecord.incoming_edges(query2)
        assert edges == [1]

        def query3(node: NodeOperand) -> NodeIndexOperand:
            max_index = node.index().max()
            max_index.equal_to("non-found")

            return max_index

        edges = graphrecord.incoming_edges(query3)

        assert edges == []

    def test_invalid_incoming_edges(self) -> None:
        graphrecord = create_graphrecord()

        # Querying incoming edges of a non-existing node should fail
        with pytest.raises(IndexError):
            graphrecord.incoming_edges("50")

        # Querying incoming edges of a non-existing node should fail
        with pytest.raises(IndexError):
            graphrecord.incoming_edges(["0", "50"])

    def test_edge_endpoints(self) -> None:
        graphrecord = create_graphrecord()

        endpoints = graphrecord.edge_endpoints(0)

        assert endpoints == ("0", "1")

        endpoints = graphrecord.edge_endpoints([0, 1])

        assert endpoints == {0: ("0", "1"), 1: ("1", "0")}

        def query(edge: EdgeOperand) -> EdgeIndicesOperand:
            edge.index().is_in([0, 1])

            return edge.index()

        endpoints = graphrecord.edge_endpoints(query)

        assert endpoints == {0: ("0", "1"), 1: ("1", "0")}

        def query2(edge: EdgeOperand) -> EdgeIndexOperand:
            edge.index().equal_to(0)

            return edge.index().max()

        endpoints = graphrecord.edge_endpoints(query2)

        assert endpoints == ("0", "1")

        def query3(edge: EdgeOperand) -> EdgeIndexOperand:
            max_index = edge.index().max()
            max_index.greater_than(10)

            return max_index

        with pytest.raises(IndexError, match="The query returned no results"):
            graphrecord.edge_endpoints(query3)

    def test_invalid_edge_endpoints(self) -> None:
        graphrecord = create_graphrecord()

        # Querying endpoints of a non-existing edge should fail
        with pytest.raises(IndexError):
            graphrecord.edge_endpoints(50)

        # Querying endpoints of a non-existing edge should fail
        with pytest.raises(IndexError):
            graphrecord.edge_endpoints([0, 50])

    def test_edges_connecting(self) -> None:
        graphrecord = create_graphrecord()

        edges = graphrecord.edges_connecting("0", "1")

        assert edges == [0]

        edges = graphrecord.edges_connecting(["0", "1"], "1")

        assert edges == [0]

        def query1(node: NodeOperand) -> NodeIndicesOperand:
            node.index().is_in(["0", "1"])

            return node.index()

        edges = graphrecord.edges_connecting(query1, "1")

        assert edges == [0]

        def query1_single(node: NodeOperand) -> NodeIndexOperand:
            node.index().equal_to("0")

            return node.index().max()

        edges = graphrecord.edges_connecting(query1_single, "1")

        assert edges == [0]

        def query1_not_found(node: NodeOperand) -> NodeIndexOperand:
            max_index = node.index().max()
            max_index.equal_to("non-found")

            return max_index

        edges = graphrecord.edges_connecting(query1_not_found, "1")

        assert edges == []

        edges = graphrecord.edges_connecting("0", ["1", "3"])

        assert sorted([0, 3]) == sorted(edges)

        def query2(node: NodeOperand) -> NodeIndicesOperand:
            node.index().is_in(["1", "3"])

            return node.index()

        edges = graphrecord.edges_connecting("0", query2)

        assert sorted([0, 3]) == sorted(edges)

        def query2_single(node: NodeOperand) -> NodeIndexOperand:
            node.index().equal_to("1")

            return node.index().max()

        edges = graphrecord.edges_connecting("0", query2_single)

        assert edges == [0]

        def query2_not_found(node: NodeOperand) -> NodeIndexOperand:
            max_index = node.index().max()
            max_index.equal_to("non-found")

            return max_index

        edges = graphrecord.edges_connecting("0", query2_not_found)

        assert edges == []

        edges = graphrecord.edges_connecting(["0", "1"], ["1", "2", "3"])

        assert sorted([0, 2, 3]) == sorted(edges)

        def query3(node: NodeOperand) -> NodeIndicesOperand:
            node.index().is_in(["0", "1"])

            return node.index()

        def query4(node: NodeOperand) -> NodeIndicesOperand:
            node.index().is_in(["1", "2", "3"])

            return node.index()

        edges = graphrecord.edges_connecting(query3, query4)

        assert sorted([0, 2, 3]) == sorted(edges)

        edges = graphrecord.edges_connecting(
            "0", "1", directed=EdgesDirected.UNDIRECTED
        )

        assert sorted(edges) == [0, 1]

    def test_remove_nodes(self) -> None:
        graphrecord = create_graphrecord()

        assert graphrecord.node_count() == 4

        attributes = graphrecord.remove_nodes("0")

        assert graphrecord.node_count() == 3
        assert create_nodes()[0][1] == attributes

        attributes = graphrecord.remove_nodes(["1", "2"])

        assert graphrecord.node_count() == 1
        assert attributes == {"1": create_nodes()[1][1], "2": create_nodes()[2][1]}

        graphrecord = create_graphrecord()

        assert graphrecord.node_count() == 4

        def query(node: NodeOperand) -> NodeIndicesOperand:
            node.index().is_in(["0", "1"])

            return node.index()

        attributes = graphrecord.remove_nodes(query)

        assert graphrecord.node_count() == 2
        assert attributes == {"0": create_nodes()[0][1], "1": create_nodes()[1][1]}

        graphrecord = create_graphrecord()

        assert graphrecord.node_count() == 4

        def query2(node: NodeOperand) -> NodeIndexOperand:
            node.index().equal_to("0")

            return node.index().max()

        attributes = graphrecord.remove_nodes(query2)

        assert graphrecord.node_count() == 3
        assert attributes == create_nodes()[0][1]

        def query3(node: NodeOperand) -> NodeIndexOperand:
            max_index = node.index().max()
            max_index.equal_to("non-found")

            return max_index

        attributes = graphrecord.remove_nodes(query3)

        assert graphrecord.node_count() == 3
        assert attributes == {}

        graphrecord = GraphRecord.from_tuples(nodes=[(0, {})], edges=[(0, 0, {})])

        assert graphrecord.node_count() == 1
        assert graphrecord.edge_count() == 1

        graphrecord.remove_nodes(0)

        assert graphrecord.node_count() == 0
        assert graphrecord.edge_count() == 0

    def test_invalid_remove_nodes(self) -> None:
        graphrecord = create_graphrecord()

        # Removing a non-existing node should fail
        with pytest.raises(IndexError):
            graphrecord.remove_nodes("50")

        # Removing a non-existing node should fail
        with pytest.raises(IndexError):
            graphrecord.remove_nodes(["0", "50"])

    def test_add_nodes(self) -> None:
        graphrecord = GraphRecord()

        assert graphrecord.node_count() == 0

        graphrecord.add_nodes(create_nodes())

        assert graphrecord.node_count() == 4

        # Adding node tuple
        graphrecord = GraphRecord()

        assert graphrecord.node_count() == 0

        graphrecord.add_nodes(("0", {}))

        assert graphrecord.node_count() == 1
        assert len(graphrecord.groups) == 0

        graphrecord = GraphRecord()

        graphrecord.add_nodes(("0", {}), "0")

        assert "0" in graphrecord.nodes_in_group("0")
        assert len(graphrecord.groups) == 1

        # Adding tuple to a group
        graphrecord = GraphRecord()

        graphrecord.add_nodes(create_nodes(), "0")

        assert "0" in graphrecord.nodes_in_group("0")
        assert "1" in graphrecord.nodes_in_group("0")
        assert "2" in graphrecord.nodes_in_group("0")
        assert "3" in graphrecord.nodes_in_group("0")
        assert "0" in graphrecord.groups

        # Adding group without nodes
        graphrecord = GraphRecord()

        graphrecord.add_nodes([], "0")

        assert graphrecord.node_count() == 0
        assert "0" in graphrecord.groups

        # Adding pandas dataframe
        graphrecord = GraphRecord()

        assert graphrecord.node_count() == 0

        graphrecord.add_nodes((create_pandas_nodes_dataframe(), "index"))

        assert graphrecord.node_count() == 2

        # Adding pandas dataframe to a group
        graphrecord = GraphRecord()

        graphrecord.add_nodes((create_pandas_nodes_dataframe(), "index"), "0")

        assert "0" in graphrecord.nodes_in_group("0")
        assert "1" in graphrecord.nodes_in_group("0")

        # Adding polars dataframe
        graphrecord = GraphRecord()

        assert graphrecord.node_count() == 0

        nodes = pl.from_pandas(create_pandas_nodes_dataframe())

        graphrecord.add_nodes((nodes, "index"))

        assert graphrecord.node_count() == 2

        # Adding polars dataframe to a group
        graphrecord = GraphRecord()

        graphrecord.add_nodes((nodes, "index"), "0")

        assert "0" in graphrecord.nodes_in_group("0")
        assert "1" in graphrecord.nodes_in_group("0")

        # Adding multiple pandas dataframes
        graphrecord = GraphRecord()

        assert graphrecord.node_count() == 0

        graphrecord.add_nodes(
            [
                (create_pandas_nodes_dataframe(), "index"),
                (create_second_pandas_nodes_dataframe(), "index"),
            ]
        )

        assert graphrecord.node_count() == 4

        # Adding multiple pandas dataframes to a group
        graphrecord = GraphRecord()

        graphrecord.add_nodes(
            [
                (create_pandas_nodes_dataframe(), "index"),
                (create_second_pandas_nodes_dataframe(), "index"),
            ],
            group="0",
        )

        assert "0" in graphrecord.nodes_in_group("0")
        assert "1" in graphrecord.nodes_in_group("0")
        assert "2" in graphrecord.nodes_in_group("0")
        assert "3" in graphrecord.nodes_in_group("0")

        # Checking if nodes can be added to a group that already exists
        graphrecord = GraphRecord()

        graphrecord.add_nodes((create_pandas_nodes_dataframe(), "index"), group="0")

        assert "0" in graphrecord.nodes_in_group("0")
        assert "1" in graphrecord.nodes_in_group("0")
        assert "2" not in graphrecord.nodes_in_group("0")
        assert "3" not in graphrecord.nodes_in_group("0")

        graphrecord.add_nodes(
            (create_second_pandas_nodes_dataframe(), "index"), group="0"
        )

        assert "2" in graphrecord.nodes_in_group("0")
        assert "3" in graphrecord.nodes_in_group("0")

        # Adding multiple polars dataframes
        graphrecord = GraphRecord()

        second_nodes = pl.from_pandas(create_second_pandas_nodes_dataframe())

        assert graphrecord.node_count() == 0

        graphrecord.add_nodes(
            [
                (nodes, "index"),
                (second_nodes, "index"),
            ]
        )

        assert graphrecord.node_count() == 4

        # Adding multiple polars dataframes to a group
        graphrecord = GraphRecord()

        graphrecord.add_nodes(
            [
                (nodes, "index"),
                (second_nodes, "index"),
            ],
            group="0",
        )

        assert "0" in graphrecord.nodes_in_group("0")
        assert "1" in graphrecord.nodes_in_group("0")
        assert "2" in graphrecord.nodes_in_group("0")
        assert "3" in graphrecord.nodes_in_group("0")

        graphrecord = GraphRecord()

        graphrecord.add_nodes(("0", {}))

        assert graphrecord.node_count() == 1

        graphrecord.freeze_schema()

        graphrecord.add_nodes(("1", {}))

        assert graphrecord.node_count() == 2

    def test_invalid_add_nodes(self) -> None:
        graphrecord = create_graphrecord()

        with pytest.raises(AssertionError):
            graphrecord.add_nodes(create_nodes())

        graphrecord.freeze_schema()

        with pytest.raises(
            ValueError,
            match=r"Attributes \[[^\]]+\] of node with index [^\s]+ do not exist in schema\.",
        ):
            graphrecord.add_nodes([("4", {"attribute": 1})])

    def test_add_nodes_pandas(self) -> None:
        graphrecord = GraphRecord()

        nodes = (create_pandas_nodes_dataframe(), "index")

        assert graphrecord.node_count() == 0

        graphrecord.add_nodes_pandas(nodes)

        assert graphrecord.node_count() == 2

        graphrecord = GraphRecord()

        second_nodes = (create_second_pandas_nodes_dataframe(), "index")

        assert graphrecord.node_count() == 0

        graphrecord.add_nodes_pandas([nodes, second_nodes])

        assert graphrecord.node_count() == 4

        # Trying with the group argument
        graphrecord = GraphRecord()

        graphrecord.add_nodes_pandas(nodes, group="0")

        assert "0" in graphrecord.nodes_in_group("0")
        assert "1" in graphrecord.nodes_in_group("0")

        graphrecord = GraphRecord()

        graphrecord.add_nodes_pandas([], group="0")

        assert graphrecord.node_count() == 0
        assert "0" in graphrecord.groups

        graphrecord = GraphRecord()

        graphrecord.add_nodes_pandas([nodes, second_nodes], group="0")

        assert "0" in graphrecord.nodes_in_group("0")
        assert "1" in graphrecord.nodes_in_group("0")
        assert "2" in graphrecord.nodes_in_group("0")
        assert "3" in graphrecord.nodes_in_group("0")

    def test_add_nodes_polars(self) -> None:
        graphrecord = GraphRecord()

        nodes = pl.from_pandas(create_pandas_nodes_dataframe())

        assert graphrecord.node_count() == 0

        graphrecord.add_nodes_polars((nodes, "index"))

        assert graphrecord.node_count() == 2

        graphrecord = GraphRecord()

        second_nodes = pl.from_pandas(create_second_pandas_nodes_dataframe())

        assert graphrecord.node_count() == 0

        graphrecord.add_nodes_polars([(nodes, "index"), (second_nodes, "index")])

        assert graphrecord.node_count() == 4

        # Trying with the group argument
        graphrecord = GraphRecord()

        graphrecord.add_nodes_polars((nodes, "index"), group="0")

        assert "0" in graphrecord.nodes_in_group("0")
        assert "1" in graphrecord.nodes_in_group("0")

        graphrecord = GraphRecord()

        graphrecord.add_nodes_polars([], group="0")

        assert graphrecord.node_count() == 0
        assert "0" in graphrecord.groups

        graphrecord = GraphRecord()

        graphrecord.add_nodes_polars(
            [(nodes, "index"), (second_nodes, "index")], group="0"
        )

        assert "0" in graphrecord.nodes_in_group("0")
        assert "1" in graphrecord.nodes_in_group("0")
        assert "2" in graphrecord.nodes_in_group("0")
        assert "3" in graphrecord.nodes_in_group("0")

    def test_invalid_add_nodes_polars(self) -> None:
        graphrecord = GraphRecord()

        nodes = pl.from_pandas(create_pandas_nodes_dataframe())
        second_nodes = pl.from_pandas(create_second_pandas_nodes_dataframe())

        # Adding a nodes dataframe with the wrong index column name should fail
        with pytest.raises(RuntimeError):
            graphrecord.add_nodes_polars((nodes, "invalid"))

        # Adding a nodes dataframe with the wrong index column name should fail
        with pytest.raises(RuntimeError):
            graphrecord.add_nodes_polars([(nodes, "index"), (second_nodes, "invalid")])

    def test_remove_edges(self) -> None:
        graphrecord = create_graphrecord()

        assert graphrecord.edge_count() == 4

        attributes = graphrecord.remove_edges(0)

        assert graphrecord.edge_count() == 3
        assert create_edges()[0][2] == attributes

        attributes = graphrecord.remove_edges([1, 2])

        assert graphrecord.edge_count() == 1
        assert attributes == {1: create_edges()[1][2], 2: create_edges()[2][2]}

        graphrecord = create_graphrecord()

        assert graphrecord.edge_count() == 4

        def query(edge: EdgeOperand) -> EdgeIndicesOperand:
            edge.index().is_in([0, 1])

            return edge.index()

        attributes = graphrecord.remove_edges(query)

        assert graphrecord.edge_count() == 2
        assert attributes == {0: create_edges()[0][2], 1: create_edges()[1][2]}

        def query2(edge: EdgeOperand) -> EdgeIndexOperand:
            edge.index().equal_to(2)

            return edge.index().max()

        attributes = graphrecord.remove_edges(query2)

        assert graphrecord.edge_count() == 1
        assert attributes == create_edges()[2][2]

        def query3(edge: EdgeOperand) -> EdgeIndexOperand:
            max_index = edge.index().max()
            max_index.equal_to(10)

            return max_index

        attributes = graphrecord.remove_edges(query3)
        assert graphrecord.edge_count() == 1
        assert attributes == {}

    def test_invalid_remove_edges(self) -> None:
        graphrecord = create_graphrecord()

        # Removing a non-existing edge should fail
        with pytest.raises(IndexError):
            graphrecord.remove_edges(50)

    def test_add_edges(self) -> None:
        graphrecord = GraphRecord()

        nodes = create_nodes()

        graphrecord.add_nodes(nodes)

        assert graphrecord.edge_count() == 0

        graphrecord.add_edges(create_edges())

        assert graphrecord.edge_count() == 4

        # Adding single edge tuple
        graphrecord = create_graphrecord()

        assert graphrecord.edge_count() == 4

        graphrecord.add_edges(("0", "3", {}))

        assert graphrecord.edge_count() == 5

        graphrecord.add_edges(("3", "0", {}), group="0")

        assert graphrecord.edge_count() == 6
        assert 5 in graphrecord.edges_in_group("0")

        # Adding tuple to a group
        graphrecord = GraphRecord()

        graphrecord.add_nodes(nodes)

        graphrecord.add_edges(create_edges(), "0")

        assert 0 in graphrecord.edges_in_group("0")
        assert 1 in graphrecord.edges_in_group("0")
        assert 2 in graphrecord.edges_in_group("0")
        assert 3 in graphrecord.edges_in_group("0")

        # Adding pandas dataframe
        graphrecord = GraphRecord()

        graphrecord.add_nodes(nodes)

        assert graphrecord.edge_count() == 0

        graphrecord.add_edges((create_pandas_edges_dataframe(), "source", "target"))

        assert graphrecord.edge_count() == 2

        # Adding pandas dataframe to a group
        graphrecord = GraphRecord()

        graphrecord.add_nodes(nodes)

        graphrecord.add_edges(
            (create_pandas_edges_dataframe(), "source", "target"), "0"
        )

        assert 0 in graphrecord.edges_in_group("0")
        assert 1 in graphrecord.edges_in_group("0")

        # Adding polars dataframe
        graphrecord = GraphRecord()

        graphrecord.add_nodes(nodes)

        assert graphrecord.edge_count() == 0

        edges = pl.from_pandas(create_pandas_edges_dataframe())

        graphrecord.add_edges((edges, "source", "target"))

        assert graphrecord.edge_count() == 2

        # Adding polars dataframe to a group
        graphrecord = GraphRecord()

        graphrecord.add_nodes(nodes)

        graphrecord.add_edges((edges, "source", "target"), "0")

        assert 0 in graphrecord.edges_in_group("0")
        assert 1 in graphrecord.edges_in_group("0")

        # Adding multiple pandas dataframe
        graphrecord = GraphRecord()

        graphrecord.add_nodes(nodes)

        assert graphrecord.edge_count() == 0

        graphrecord.add_edges(
            [
                (create_pandas_edges_dataframe(), "source", "target"),
                (create_second_pandas_edges_dataframe(), "source", "target"),
            ]
        )

        assert graphrecord.edge_count() == 4

        # Adding multiple pandas dataframe to a group
        graphrecord = GraphRecord()

        graphrecord.add_nodes(nodes)

        graphrecord.add_edges(
            [
                (create_pandas_edges_dataframe(), "source", "target"),
                (create_second_pandas_edges_dataframe(), "source", "target"),
            ],
            "0",
        )

        assert 0 in graphrecord.edges_in_group("0")
        assert 1 in graphrecord.edges_in_group("0")
        assert 2 in graphrecord.edges_in_group("0")
        assert 3 in graphrecord.edges_in_group("0")

        # Adding multiple polars dataframe
        graphrecord = GraphRecord()

        graphrecord.add_nodes(nodes)

        assert graphrecord.edge_count() == 0

        second_edges = pl.from_pandas(create_second_pandas_edges_dataframe())

        graphrecord.add_edges(
            [
                (edges, "source", "target"),
                (second_edges, "source", "target"),
            ]
        )

        assert graphrecord.edge_count() == 4

        # Adding multiple polars dataframe to a group
        graphrecord = GraphRecord()

        graphrecord.add_nodes(nodes)

        graphrecord.add_edges(
            [
                (edges, "source", "target"),
                (second_edges, "source", "target"),
            ],
            "0",
        )

        assert 0 in graphrecord.edges_in_group("0")
        assert 1 in graphrecord.edges_in_group("0")
        assert 2 in graphrecord.edges_in_group("0")
        assert 3 in graphrecord.edges_in_group("0")

        graphrecord = GraphRecord()

        graphrecord.add_nodes(nodes)

        graphrecord.add_edges([("0", "1", {"attribute": 1})])

        graphrecord.freeze_schema()

        graphrecord.add_edges([("1", "2", {"attribute": 1})])

        assert graphrecord.edge_count() == 2

    def test_invalid_add_edges(self) -> None:
        graphrecord = GraphRecord()

        nodes = create_nodes()

        graphrecord.add_nodes(nodes)

        # Adding an edge pointing to a non-existent node should fail
        with pytest.raises(IndexError):
            graphrecord.add_edges(("0", "50", {}))

        # Adding an edge from a non-existing node should fail
        with pytest.raises(IndexError):
            graphrecord.add_edges(("50", "0", {}))

        graphrecord.freeze_schema()

        with pytest.raises(
            ValueError,
            match=r"Attributes \[[^\]]+\] of edge with index [^\s]+ do not exist in schema\.",
        ):
            graphrecord.add_edges([("0", "1", {"attribute": 1})])

    def test_add_edges_pandas(self) -> None:
        graphrecord = GraphRecord()

        nodes = create_nodes()

        graphrecord.add_nodes(nodes)

        edges = (create_pandas_edges_dataframe(), "source", "target")

        assert graphrecord.edge_count() == 0

        graphrecord.add_edges(edges)

        assert graphrecord.edge_count() == 2

        # Adding to a group
        graphrecord = GraphRecord()

        graphrecord.add_nodes(nodes)

        graphrecord.add_edges(edges, "0")

        assert 0 in graphrecord.edges_in_group("0")
        assert 1 in graphrecord.edges_in_group("0")

        graphrecord = GraphRecord()

        graphrecord.add_nodes(nodes)

        second_edges = (create_second_pandas_edges_dataframe(), "source", "target")

        graphrecord.add_edges([edges, second_edges], "0")

        assert 0 in graphrecord.edges_in_group("0")
        assert 1 in graphrecord.edges_in_group("0")
        assert 2 in graphrecord.edges_in_group("0")
        assert 3 in graphrecord.edges_in_group("0")

    def test_add_edges_polars(self) -> None:
        graphrecord = GraphRecord()

        nodes = create_nodes()

        graphrecord.add_nodes(nodes)

        edges = pl.from_pandas(create_pandas_edges_dataframe())

        assert graphrecord.edge_count() == 0

        graphrecord.add_edges_polars((edges, "source", "target"))

        assert graphrecord.edge_count() == 2

        # Adding to a group
        graphrecord = GraphRecord()

        graphrecord.add_nodes(nodes)

        graphrecord.add_edges_polars((edges, "source", "target"), "0")

        assert 0 in graphrecord.edges_in_group("0")
        assert 1 in graphrecord.edges_in_group("0")

        graphrecord = GraphRecord()

        graphrecord.add_nodes(nodes)

        second_edges = pl.from_pandas(create_second_pandas_edges_dataframe())

        graphrecord.add_edges_polars(
            [(edges, "source", "target"), (second_edges, "source", "target")], "0"
        )

        assert 0 in graphrecord.edges_in_group("0")
        assert 1 in graphrecord.edges_in_group("0")
        assert 2 in graphrecord.edges_in_group("0")
        assert 3 in graphrecord.edges_in_group("0")

    def test_invalid_add_edges_polars(self) -> None:
        graphrecord = GraphRecord()

        nodes = create_nodes()

        graphrecord.add_nodes(nodes)

        edges = pl.from_pandas(create_pandas_edges_dataframe())

        # Providing the wrong source index column name should fail
        with pytest.raises(RuntimeError):
            graphrecord.add_edges_polars((edges, "invalid", "target"))

        # Providing the wrong target index column name should fail
        with pytest.raises(RuntimeError):
            graphrecord.add_edges_polars((edges, "source", "invalid"))

    def test_add_group(self) -> None:
        graphrecord = create_graphrecord()

        assert graphrecord.group_count() == 0

        graphrecord.add_group("0")

        assert graphrecord.group_count() == 1

        graphrecord.add_group("1", "0", 0)

        assert graphrecord.group_count() == 2
        assert graphrecord.group("1") == {"nodes": ["0"], "edges": [0]}

        graphrecord.add_group("2", ["0", "1"], [0, 1])

        assert graphrecord.group_count() == 3
        nodes_and_edges = graphrecord.group("2")
        assert sorted(["0", "1"]) == sorted(nodes_and_edges["nodes"])
        assert sorted([0, 1]) == sorted(nodes_and_edges["edges"])

        def query1(node: NodeOperand) -> NodeIndicesOperand:
            node.index().is_in(["0", "1"])

            return node.index()

        def query2(edge: EdgeOperand) -> EdgeIndicesOperand:
            edge.index().is_in([0, 1])

            return edge.index()

        graphrecord.add_group(
            "3",
            query1,
            query2,
        )

        assert graphrecord.group_count() == 4
        nodes_and_edges = graphrecord.group("3")
        assert sorted(["0", "1"]) == sorted(nodes_and_edges["nodes"])
        assert sorted([0, 1]) == sorted(nodes_and_edges["edges"])

    def test_invalid_add_group(self) -> None:
        graphrecord = create_graphrecord()

        # Adding a group with a non-existing node should fail
        with pytest.raises(IndexError):
            graphrecord.add_group("0", "50")

        # Adding a group with a non-existing edge should fail
        with pytest.raises(IndexError):
            graphrecord.add_group("0", edges=[50])

        # Adding an already existing group should fail
        with pytest.raises(IndexError):
            graphrecord.add_group("0", ["0", "50"])

        graphrecord.add_group("0", "0")

        # Adding an already existing group should fail
        with pytest.raises(AssertionError):
            graphrecord.add_group("0")

        # Adding a node to a group that already is in the group should fail
        with pytest.raises(AssertionError):
            graphrecord.add_group("0", "0")

        # Adding a node to a group that already is in the group should fail
        with pytest.raises(AssertionError):
            graphrecord.add_group("0", ["1", "0"])

        def query(node: NodeOperand) -> NodeIndicesOperand:
            node.index().equal_to("0")

            return node.index()

        # Adding a node to a group that already is in the group should fail
        with pytest.raises(AssertionError):
            graphrecord.add_group("0", query)

        graphrecord.add_nodes(("4", {"test": "test"}))
        edge_index = graphrecord.add_edges(("4", "4", {"test": "test"}))[0]

        graphrecord.freeze_schema()

        with pytest.raises(ValueError, match="Group 2 is not defined in the schema"):
            graphrecord.add_group("2")

        graphrecord.remove_groups("0")

        with pytest.raises(
            ValueError,
            match=r"Attribute [^\s]+ of type [^\s]+ not found on node with index [^\s]+",
        ):
            graphrecord.add_group("0", "4")

        with pytest.raises(
            ValueError,
            match=r"Attributes \[[^\]]+\] of edge with index [^\s]+ do not exist in schema\.",
        ):
            graphrecord.add_group("0", edges=edge_index)

    def test_remove_groups(self) -> None:
        graphrecord = create_graphrecord()

        graphrecord.add_group("0")

        assert graphrecord.group_count() == 1

        graphrecord.remove_groups("0")

        assert graphrecord.group_count() == 0

    def test_invalid_remove_groups(self) -> None:
        graphrecord = create_graphrecord()

        # Removing a non-existing group should fail
        with pytest.raises(IndexError):
            graphrecord.remove_groups("0")

    def test_add_nodes_to_group(self) -> None:
        graphrecord = create_graphrecord()

        graphrecord.add_group("0")

        assert graphrecord.nodes_in_group("0") == []

        graphrecord.add_nodes_to_group("0", "0")

        assert graphrecord.nodes_in_group("0") == ["0"]

        graphrecord.add_nodes_to_group("0", ["1", "2"])

        assert sorted(["0", "1", "2"]) == sorted(graphrecord.nodes_in_group("0"))

        def query(node: NodeOperand) -> NodeIndicesOperand:
            node.index().equal_to("3")

            return node.index()

        graphrecord.add_nodes_to_group("0", query)

        assert sorted(["0", "1", "2", "3"]) == sorted(graphrecord.nodes_in_group("0"))

        def query2(node: NodeOperand) -> NodeIndexOperand:
            node.index().equal_to("0")

            return node.index().max()

        graphrecord.add_group("1")
        graphrecord.add_nodes_to_group("1", query2)

        assert graphrecord.nodes_in_group("1") == ["0"]

        def query3(node: NodeOperand) -> NodeIndexOperand:
            max_index = node.index().max()
            max_index.greater_than(10)

            return max_index

        graphrecord.add_nodes_to_group("1", query3)

        assert graphrecord.nodes_in_group("1") == ["0"]

        graphrecord.add_nodes(("4", {"test": "test"}), "1")

        graphrecord.freeze_schema()

        graphrecord.add_nodes(("5", {"test": "test"}), "1")

        assert len(graphrecord.nodes_in_group("1")) == 3

    def test_invalid_add_nodes_to_group(self) -> None:
        graphrecord = create_graphrecord()

        graphrecord.add_group("0", ["0"])

        # Adding a non-existing node to a group should fail
        with pytest.raises(IndexError):
            graphrecord.add_nodes_to_group("0", "50")

        # Adding a non-existing node to a group should fail
        with pytest.raises(IndexError):
            graphrecord.add_nodes_to_group("0", ["1", "50"])

        # Adding a node to a group that already is in the group should fail
        with pytest.raises(AssertionError):
            graphrecord.add_nodes_to_group("0", "0")

        # Adding a node to a group that already is in the group should fail
        with pytest.raises(AssertionError):
            graphrecord.add_nodes_to_group("0", ["1", "0"])

        def query(node: NodeOperand) -> NodeIndicesOperand:
            node.index().equal_to("0")

            return node.index()

        # Adding a node to a group that already is in the group should fail
        with pytest.raises(AssertionError):
            graphrecord.add_nodes_to_group("0", query)

        graphrecord = GraphRecord()

        graphrecord.add_nodes(("0", {"test": "test"}))
        graphrecord.add_group("0")

        graphrecord.freeze_schema()

        with pytest.raises(
            ValueError,
            match=r"Attributes \[[^\]]+\] of node with index [^\s]+ do not exist in schema\.",
        ):
            graphrecord.add_nodes_to_group("0", "0")

    def test_add_edges_to_group(self) -> None:
        graphrecord = create_graphrecord()

        graphrecord.add_group("0")

        assert graphrecord.edges_in_group("0") == []

        graphrecord.add_edges_to_group("0", 0)

        assert graphrecord.edges_in_group("0") == [0]

        graphrecord.add_edges_to_group("0", [1, 2])

        assert sorted([0, 1, 2]) == sorted(graphrecord.edges_in_group("0"))

        def query(edge: EdgeOperand) -> EdgeIndicesOperand:
            edge.index().equal_to(3)

            return edge.index()

        graphrecord.add_edges_to_group("0", query)

        assert sorted([0, 1, 2, 3]) == sorted(graphrecord.edges_in_group("0"))

        def query2(edge: EdgeOperand) -> EdgeIndexOperand:
            edge.index().equal_to(0)

            return edge.index().max()

        graphrecord.add_group("1")
        graphrecord.add_edges_to_group("1", query2)

        assert graphrecord.edges_in_group("1") == [0]

        def query3(edge: EdgeOperand) -> EdgeIndexOperand:
            max_index = edge.index().max()
            max_index.greater_than(10)

            return max_index

        graphrecord.add_edges_to_group("1", query3)
        assert graphrecord.edges_in_group("1") == [0]

        graphrecord = GraphRecord()
        graphrecord.add_nodes(create_nodes())

        graphrecord.add_edges(("0", "1", {"test": "test"}), group="0")

        graphrecord.freeze_schema()

        graphrecord.add_edges(("0", "1", {"test": "test"}), group="0")

        assert len(graphrecord.edges_in_group("0")) == 2

    def test_invalid_add_edges_to_group(self) -> None:
        graphrecord = create_graphrecord()

        graphrecord.add_group("0", edges=[0])

        # Adding a non-existing edge to a group should fail
        with pytest.raises(IndexError):
            graphrecord.add_edges_to_group("0", 50)

        # Adding a non-existing edge to a group should fail
        with pytest.raises(IndexError):
            graphrecord.add_edges_to_group("0", [1, 50])

        # Adding an edge to a group that already is in the group should fail
        with pytest.raises(AssertionError):
            graphrecord.add_edges_to_group("0", 0)

        # Adding an edge to a group that already is in the group should fail
        with pytest.raises(AssertionError):
            graphrecord.add_edges_to_group("0", [1, 0])

        def query(edge: EdgeOperand) -> EdgeIndicesOperand:
            edge.index().equal_to(0)

            return edge.index()

        # Adding an edge to a group that already is in the group should fail
        with pytest.raises(AssertionError):
            graphrecord.add_edges_to_group("0", query)

        graphrecord = GraphRecord()

        graphrecord.add_nodes(("0", {}))

        graphrecord.add_edges(("0", "0", {"test": "test"}), group="0")

        graphrecord.freeze_schema()

        with pytest.raises(AssertionError):
            graphrecord.add_edges_to_group("0", 0)

    def test_remove_nodes_from_group(self) -> None:
        graphrecord = create_graphrecord()

        graphrecord.add_group("0", ["0", "1"])

        assert sorted(["0", "1"]) == sorted(graphrecord.nodes_in_group("0"))

        graphrecord.remove_nodes_from_group("0", "1")

        assert graphrecord.nodes_in_group("0") == ["0"]

        graphrecord.add_nodes_to_group("0", "1")

        assert sorted(["0", "1"]) == sorted(graphrecord.nodes_in_group("0"))

        graphrecord.remove_nodes_from_group("0", ["0", "1"])

        assert graphrecord.nodes_in_group("0") == []

        graphrecord.add_nodes_to_group("0", ["0", "1"])

        assert sorted(["0", "1"]) == sorted(graphrecord.nodes_in_group("0"))

        def query(node: NodeOperand) -> NodeIndicesOperand:
            node.index().is_in(["0", "1"])

            return node.index()

        graphrecord.remove_nodes_from_group("0", query)

        assert graphrecord.nodes_in_group("0") == []

        graphrecord.add_nodes_to_group("0", ["0", "1"])

        def query2(node: NodeOperand) -> NodeIndexOperand:
            node.index().equal_to("0")

            return node.index().max()

        graphrecord.remove_nodes_from_group("0", query2)

        assert graphrecord.nodes_in_group("0") == ["1"]

        def query3(node: NodeOperand) -> NodeIndexOperand:
            max_index = node.index().max()
            max_index.greater_than(10)

            return max_index

        graphrecord.remove_nodes_from_group("0", query3)

        assert graphrecord.nodes_in_group("0") == ["1"]

    def test_invalid_remove_nodes_from_group(self) -> None:
        graphrecord = create_graphrecord()

        graphrecord.add_group("0", ["0", "1"])

        # Removing a node from a non-existing group should fail
        with pytest.raises(IndexError):
            graphrecord.remove_nodes_from_group("50", "0")

        # Removing a node from a non-existing group should fail
        with pytest.raises(IndexError):
            graphrecord.remove_nodes_from_group("50", ["0", "1"])

        def query(node: NodeOperand) -> NodeIndicesOperand:
            node.index().equal_to("0")

            return node.index()

        # Removing a node from a non-existing group should fail
        with pytest.raises(IndexError):
            graphrecord.remove_nodes_from_group("50", query)

        # Removing a non-existing node from a group should fail
        with pytest.raises(IndexError):
            graphrecord.remove_nodes_from_group("0", "50")

        # Removing a non-existing node from a group should fail
        with pytest.raises(IndexError):
            graphrecord.remove_nodes_from_group("0", ["0", "50"])

    def test_remove_edges_from_group(self) -> None:
        graphrecord = create_graphrecord()

        graphrecord.add_group("0", edges=[0, 1])

        assert sorted([0, 1]) == sorted(graphrecord.edges_in_group("0"))

        graphrecord.remove_edges_from_group("0", 1)

        assert graphrecord.edges_in_group("0") == [0]

        graphrecord.add_edges_to_group("0", 1)

        assert sorted([0, 1]) == sorted(graphrecord.edges_in_group("0"))

        graphrecord.remove_edges_from_group("0", [0, 1])

        assert graphrecord.edges_in_group("0") == []

        graphrecord.add_edges_to_group("0", [0, 1])

        assert sorted([0, 1]) == sorted(graphrecord.edges_in_group("0"))

        def query(edge: EdgeOperand) -> EdgeIndicesOperand:
            edge.index().is_in([0, 1])

            return edge.index()

        graphrecord.remove_edges_from_group("0", query)

        assert graphrecord.edges_in_group("0") == []

        def query2(edge: EdgeOperand) -> EdgeIndexOperand:
            edge.index().equal_to(0)

            return edge.index().max()

        graphrecord.add_edges_to_group("0", [0, 1])
        graphrecord.remove_edges_from_group("0", query2)

        assert graphrecord.edges_in_group("0") == [1]

        def query3(edge: EdgeOperand) -> EdgeIndexOperand:
            max_index = edge.index().max()
            max_index.greater_than(10)

            return max_index

        graphrecord.remove_edges_from_group("0", query3)

        assert graphrecord.edges_in_group("0") == [1]

    def test_invalid_remove_edges_from_group(self) -> None:
        graphrecord = create_graphrecord()

        graphrecord.add_group("0", edges=[0, 1])

        # Removing an edge from a non-existing group should fail
        with pytest.raises(IndexError):
            graphrecord.remove_edges_from_group("50", 0)

        # Removing an edge from a non-existing group should fail
        with pytest.raises(IndexError):
            graphrecord.remove_edges_from_group("50", [0, 1])

        def query(edge: EdgeOperand) -> EdgeIndicesOperand:
            edge.index().equal_to(0)

            return edge.index()

        # Removing an edge from a non-existing group should fail
        with pytest.raises(IndexError):
            graphrecord.remove_edges_from_group("50", query)

        # Removing a non-existing edge from a group should fail
        with pytest.raises(IndexError):
            graphrecord.remove_edges_from_group("0", 50)

        # Removing a non-existing edge from a group should fail
        with pytest.raises(IndexError):
            graphrecord.remove_edges_from_group("0", [0, 50])

    def test_nodes_in_group(self) -> None:
        graphrecord = create_graphrecord()

        graphrecord.add_group("0", ["0", "1"])

        assert sorted(["0", "1"]) == sorted(graphrecord.nodes_in_group("0"))

        graphrecord.add_group("1", ["2", "3"])

        actual = {
            k: sorted(v) for k, v in graphrecord.nodes_in_group(["0", "1"]).items()
        }

        assert {"0": sorted(["1", "0"]), "1": sorted(["2", "3"])} == actual

    def test_invalid_nodes_in_group(self) -> None:
        graphrecord = create_graphrecord()

        # Querying nodes in a non-existing group should fail
        with pytest.raises(IndexError):
            graphrecord.nodes_in_group("50")

    def test_ungrouped_nodes(self) -> None:
        graphrecord = create_graphrecord()

        graphrecord.add_group("0", ["0", "1"])

        assert sorted(["2", "3"]) == sorted(graphrecord.ungrouped_nodes())

    def test_edges_in_group(self) -> None:
        graphrecord = create_graphrecord()

        graphrecord.add_group("0", edges=[0, 1])

        assert sorted([0, 1]) == sorted(graphrecord.edges_in_group("0"))

        graphrecord.add_group("1", edges=[2, 3])

        actual = {
            k: sorted(v) for k, v in graphrecord.edges_in_group(["0", "1"]).items()
        }

        assert {"0": sorted([0, 1]), "1": sorted([2, 3])} == actual

    def test_invalid_edges_in_group(self) -> None:
        graphrecord = create_graphrecord()

        # Querying edges in a non-existing group should fail
        with pytest.raises(IndexError):
            graphrecord.edges_in_group("50")

    def test_ungrouped_edges(self) -> None:
        graphrecord = create_graphrecord()

        graphrecord.add_group("0", edges=[0, 1])

        assert sorted([2, 3]) == sorted(graphrecord.ungrouped_edges())

    def test_groups_of_node(self) -> None:
        graphrecord = create_graphrecord()

        graphrecord.add_group("0", ["0", "1"])

        assert graphrecord.groups_of_node("0") == ["0"]

        assert graphrecord.groups_of_node(["0", "1"]) == {"0": ["0"], "1": ["0"]}

        def query(node: NodeOperand) -> NodeIndicesOperand:
            node.index().is_in(["0", "1"])

            return node.index()

        assert graphrecord.groups_of_node(query) == {"0": ["0"], "1": ["0"]}

        def query2(node: NodeOperand) -> NodeIndexOperand:
            node.index().equal_to("0")

            return node.index().max()

        assert graphrecord.groups_of_node(query2) == ["0"]

        def query3(node: NodeOperand) -> NodeIndexOperand:
            max_index = node.index().max()
            max_index.greater_than(10)

            return max_index

        assert graphrecord.groups_of_node(query3) == []

    def test_invalid_groups_of_node(self) -> None:
        graphrecord = create_graphrecord()

        # Querying groups of a non-existing node should fail
        with pytest.raises(IndexError):
            graphrecord.groups_of_node("50")

        # Querying groups of a non-existing node should fail
        with pytest.raises(IndexError):
            graphrecord.groups_of_node(["0", "50"])

    def test_groups_of_edge(self) -> None:
        graphrecord = create_graphrecord()

        graphrecord.add_group("0", edges=[0, 1])

        assert graphrecord.groups_of_edge(0) == ["0"]

        assert graphrecord.groups_of_edge([0, 1]) == {0: ["0"], 1: ["0"]}

        def query(edge: EdgeOperand) -> EdgeIndicesOperand:
            edge.index().is_in([0, 1])

            return edge.index()

        assert graphrecord.groups_of_edge(query) == {0: ["0"], 1: ["0"]}

        def query2(edge: EdgeOperand) -> EdgeIndexOperand:
            edge.index().equal_to(0)

            return edge.index().max()

        assert graphrecord.groups_of_edge(query2) == ["0"]

        def query3(edge: EdgeOperand) -> EdgeIndexOperand:
            max_index = edge.index().max()
            max_index.greater_than(10)

            return max_index

        assert graphrecord.groups_of_edge(query3) == []

    def test_invalid_groups_of_edge(self) -> None:
        graphrecord = create_graphrecord()

        # Querying groups of a non-existing edge should fail
        with pytest.raises(IndexError):
            graphrecord.groups_of_edge(50)

        # Querying groups of a non-existing edge should fail
        with pytest.raises(IndexError):
            graphrecord.groups_of_edge([0, 50])

    def test_node_count(self) -> None:
        graphrecord = GraphRecord()

        assert graphrecord.node_count() == 0

        graphrecord.add_nodes([("0", {})])

        assert graphrecord.node_count() == 1

    def test_edge_count(self) -> None:
        graphrecord = GraphRecord()

        graphrecord.add_nodes(("0", {}))
        graphrecord.add_nodes(("1", {}))

        assert graphrecord.edge_count() == 0

        graphrecord.add_edges(("0", "1", {}))

        assert graphrecord.edge_count() == 1

    def test_group_count(self) -> None:
        graphrecord = create_graphrecord()

        assert graphrecord.group_count() == 0

        graphrecord.add_group("0")

        assert graphrecord.group_count() == 1

    def test_contains_node(self) -> None:
        graphrecord = create_graphrecord()

        assert graphrecord.contains_node("0")

        assert not graphrecord.contains_node("50")

    def test_contains_edge(self) -> None:
        graphrecord = create_graphrecord()

        assert graphrecord.contains_edge(0)

        assert not graphrecord.contains_edge(50)

    def test_contains_group(self) -> None:
        graphrecord = create_graphrecord()

        assert not graphrecord.contains_group("0")

        graphrecord.add_group("0")

        assert graphrecord.contains_group("0")

    def test_neighbors(self) -> None:
        graphrecord = create_graphrecord()

        neighbors = graphrecord.neighbors("0")

        assert sorted(["1", "3"]) == sorted(neighbors)

        neighbors = graphrecord.neighbors(["0", "1"])

        assert {key: sorted(value) for key, value in neighbors.items()} == {
            "0": sorted(["1", "3"]),
            "1": ["0", "2"],
        }

        def query1(node: NodeOperand) -> NodeIndicesOperand:
            node.index().is_in(["0", "1"])

            return node.index()

        neighbors = graphrecord.neighbors(query1)

        assert {key: sorted(value) for key, value in neighbors.items()} == {
            "0": sorted(["1", "3"]),
            "1": ["0", "2"],
        }

        def query1_single(node: NodeOperand) -> NodeIndexOperand:
            node.index().equal_to("0")

            return node.index().max()

        neighbors = graphrecord.neighbors(query1_single)

        assert sorted(neighbors) == ["1", "3"]

        def query1_not_found(node: NodeOperand) -> NodeIndexOperand:
            max_index = node.index().max()
            max_index.equal_to("non-found")

            return max_index

        neighbors = graphrecord.neighbors(query1_not_found)

        assert neighbors == []

        neighbors = graphrecord.neighbors("0", directed=EdgesDirected.UNDIRECTED)

        assert sorted(["1", "3"]) == sorted(neighbors)

        neighbors = graphrecord.neighbors(["0", "1"], directed=EdgesDirected.UNDIRECTED)

        assert {key: sorted(value) for key, value in neighbors.items()} == {
            "0": sorted(["1", "3"]),
            "1": ["0", "2"],
        }

        def query2(node: NodeOperand) -> NodeIndicesOperand:
            node.index().is_in(["0", "1"])

            return node.index()

        neighbors = graphrecord.neighbors(query2, directed=EdgesDirected.UNDIRECTED)

        assert {key: sorted(value) for key, value in neighbors.items()} == {
            "0": sorted(["1", "3"]),
            "1": ["0", "2"],
        }

    def test_invalid_neighbors(self) -> None:
        graphrecord = create_graphrecord()

        # Querying neighbors of a non-existing node should fail
        with pytest.raises(IndexError):
            graphrecord.neighbors("50")

        # Querying neighbors of a non-existing node should fail
        with pytest.raises(IndexError):
            graphrecord.neighbors(["0", "50"])

        # Querying undirected neighbors of a non-existing node should fail
        with pytest.raises(IndexError):
            graphrecord.neighbors("50", directed=EdgesDirected.UNDIRECTED)

        # Querying undirected neighbors of a non-existing node should fail
        with pytest.raises(IndexError):
            graphrecord.neighbors(["0", "50"], directed=EdgesDirected.UNDIRECTED)

    def test_clear(self) -> None:
        graphrecord = create_graphrecord()

        assert graphrecord.node_count() == 4
        assert graphrecord.edge_count() == 4
        assert graphrecord.group_count() == 0

        graphrecord.clear()

        assert graphrecord.node_count() == 0
        assert graphrecord.edge_count() == 0
        assert graphrecord.group_count() == 0

    def test_clone(self) -> None:
        graphrecord = create_graphrecord()

        cloned_graphrecord = graphrecord.clone()

        assert graphrecord.node_count() == cloned_graphrecord.node_count()
        assert graphrecord.edge_count() == cloned_graphrecord.edge_count()
        assert graphrecord.group_count() == cloned_graphrecord.group_count()

        cloned_graphrecord.add_nodes(("new_node", {"attribute": "value"}))
        cloned_graphrecord.add_edges(("0", "new_node", {"attribute": "value"}))
        cloned_graphrecord.add_group("new_group", ["new_node"])

        assert graphrecord.node_count() != cloned_graphrecord.node_count()
        assert graphrecord.edge_count() != cloned_graphrecord.edge_count()
        assert graphrecord.group_count() != cloned_graphrecord.group_count()

    def test_query_nodes(self) -> None:
        graphrecord = create_graphrecord()

        def query1(node: NodeOperand) -> NodeIndicesOperand:
            node.index().is_in(["0", "1"])

            return node.index()

        assert sorted(["0", "1"]) == sorted(graphrecord.query_nodes(query1))

        def query2(node: NodeOperand) -> NodeIndexOperand:
            node.index().equal_to("0")

            return node.index().max()

        assert graphrecord.query_nodes(query2) == "0"

        def query3(node: NodeOperand) -> NodeMultipleValuesWithIndexOperand:
            return node.attribute("lorem")

        assert graphrecord.query_nodes(query3) == {"0": "ipsum"}

        def query4(node: NodeOperand) -> NodeSingleValueWithIndexOperand:
            return node.attribute("lorem").max()

        assert graphrecord.query_nodes(query4) == ("0", "ipsum")

        def query5(node: NodeOperand) -> NodeAttributesTreeOperand:
            node.index().equal_to("0")
            return node.attributes()

        actual = {k: sorted(v) for k, v in graphrecord.query_nodes(query5).items()}

        assert actual == {"0": ["dolor", "lorem"]}

        def query6(node: NodeOperand) -> NodeMultipleAttributesWithIndexOperand:
            attributes_tree = query5(node)
            return attributes_tree.max()

        assert graphrecord.query_nodes(query6) == {"0": "lorem"}

        def query7(node: NodeOperand) -> NodeSingleAttributeWithIndexOperand:
            multiple_attributes = query6(node)
            return multiple_attributes.max()

        assert graphrecord.query_nodes(query7) == ("0", "lorem")

        def query8(node: NodeOperand) -> EdgeIndexOperand:
            node.index().equal_to("0")
            return node.edges().index().max()

        assert graphrecord.query_nodes(query8) == 3

        def query9(node: NodeOperand) -> EdgeIndicesOperand:
            node.index().equal_to("0")
            return node.edges().index()

        assert sorted(graphrecord.query_nodes(query9)) == [0, 1, 3]

        def query10(node: NodeOperand) -> List[QueryReturnOperand]:
            node.index().equal_to("0")
            return [node.index(), node.edges().index()]

        node_indices, edge_indices = graphrecord.query_nodes(query10)
        assert node_indices == ["0"]
        assert is_edge_index_list(edge_indices)
        assert sorted(edge_indices) == [0, 1, 3]

    def test_query_edges(self) -> None:
        graphrecord = create_graphrecord()

        def query1(edge: EdgeOperand) -> EdgeIndicesOperand:
            edge.index().is_in([0, 1])

            return edge.index()

        assert sorted(graphrecord.query_edges(query1)) == [0, 1]

        def query2(edge: EdgeOperand) -> EdgeIndexOperand:
            edge.index().equal_to(0)

            return edge.index().max()

        assert graphrecord.query_edges(query2) == 0

        def query3(edge: EdgeOperand) -> EdgeMultipleValuesWithIndexOperand:
            return edge.attribute("eiusmod")

        assert graphrecord.query_edges(query3) == {0: "tempor", 1: "tempor"}

        def query4(edge: EdgeOperand) -> EdgeSingleValueWithIndexOperand:
            edge.index().equal_to(0)
            return edge.attribute("eiusmod").max()

        assert graphrecord.query_edges(query4) == (0, "tempor")

        def query5(edge: EdgeOperand) -> EdgeAttributesTreeOperand:
            edge.index().equal_to(0)
            return edge.attributes()

        actual = {k: sorted(v) for k, v in graphrecord.query_edges(query5).items()}

        assert actual == {0: ["eiusmod", "sed"]}

        def query6(edge: EdgeOperand) -> EdgeMultipleAttributesWithIndexOperand:
            attributes_tree = query5(edge)
            return attributes_tree.max()

        assert graphrecord.query_edges(query6) == {0: "sed"}

        def query7(edge: EdgeOperand) -> EdgeSingleAttributeWithIndexOperand:
            multiple_attributes = query6(edge)
            return multiple_attributes.max()

        assert graphrecord.query_edges(query7) == (0, "sed")

        def query8(edge: EdgeOperand) -> NodeIndexOperand:
            edge.index().equal_to(0)
            return edge.source_node().index().max()

        assert graphrecord.query_edges(query8) == "0"

        def query9(edge: EdgeOperand) -> NodeIndicesOperand:
            return edge.source_node().index()

        assert sorted(graphrecord.query_edges(query9)) == ["0", "0", "1", "1"]

        def query10(edge: EdgeOperand) -> List[QueryReturnOperand]:
            edge.index().equal_to(0)
            return [edge.index(), edge.source_node().index()]

        edge_indices, node_indices = graphrecord.query_edges(query10)
        assert edge_indices == [0]
        assert is_node_index_list(node_indices)
        assert sorted(node_indices) == ["0"]


class TestGraphRecordPlugins(unittest.TestCase):
    def test_with_plugins_single(self) -> None:
        graphrecord = GraphRecord.with_plugins([Plugin()])

        graphrecord.add_nodes([("a", {})])

        assert "a" in graphrecord.nodes

    def test_with_plugins_list(self) -> None:
        graphrecord = GraphRecord.with_plugins([Plugin()])

        graphrecord.add_nodes([("a", {})])

        assert "a" in graphrecord.nodes

    def test_run_with_plugins_calls_pre_and_post(self) -> None:
        calls: List[str] = []

        class TrackingPlugin(Plugin):
            def pre_add_nodes(
                self, graphrecord: GraphRecord, context: PreAddNodesContext
            ) -> PreAddNodesContext:
                calls.append("pre")
                return context

            def post_add_nodes(
                self, graphrecord: GraphRecord, context: PostAddNodesContext
            ) -> None:
                calls.append("post")

        graphrecord = GraphRecord.with_plugins([TrackingPlugin()])
        graphrecord.add_nodes([("a", {})])

        assert calls == ["pre", "post"]
        assert "a" in graphrecord.nodes

    def test_run_with_plugins_simple_calls_pre_and_post(self) -> None:
        calls: List[str] = []

        class TrackingPlugin(Plugin):
            def pre_clear(self, graphrecord: GraphRecord) -> None:
                calls.append("pre_clear")

            def post_clear(self, graphrecord: GraphRecord) -> None:
                calls.append("post_clear")

        graphrecord = GraphRecord.with_plugins([TrackingPlugin()])
        graphrecord.add_nodes([("a", {})])
        graphrecord.clear()

        assert calls == ["pre_clear", "post_clear"]
        assert graphrecord.nodes == []

    def test_run_with_plugins_restores_flag_on_error(self) -> None:
        class FailOncePlugin(Plugin):
            def __init__(self) -> None:
                self.failed = False

            def pre_add_nodes(
                self, graphrecord: GraphRecord, context: PreAddNodesContext
            ) -> PreAddNodesContext:
                if not self.failed:
                    self.failed = True
                    msg = "boom"
                    raise RuntimeError(msg)
                return context

        graphrecord = GraphRecord.with_plugins([FailOncePlugin()])

        with pytest.raises(RuntimeError, match="boom"):
            graphrecord.add_nodes([("a", {})])

        graphrecord.add_nodes([("b", {})])

        assert "b" in graphrecord.nodes

    def test_run_with_plugins_simple_restores_flag_on_error(self) -> None:
        class FailOncePlugin(Plugin):
            def __init__(self) -> None:
                self.failed = False

            def pre_clear(self, graphrecord: GraphRecord) -> None:
                if not self.failed:
                    self.failed = True
                    msg = "boom"
                    raise RuntimeError(msg)

        graphrecord = GraphRecord.with_plugins([FailOncePlugin()])

        with pytest.raises(RuntimeError, match="boom"):
            graphrecord.clear()

        graphrecord.clear()


if __name__ == "__main__":
    suite = unittest.TestSuite()
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestGraphRecord))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestGraphRecordPlugins))
    unittest.TextTestRunner(verbosity=2).run(suite)
