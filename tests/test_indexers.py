import unittest

import pytest

from graphrecords import GraphRecord
from graphrecords.querying import (
    EdgeIndexOperand,
    EdgeIndicesOperand,
    EdgeOperand,
    NodeIndexOperand,
    NodeIndicesOperand,
    NodeOperand,
)


def create_graphrecord() -> GraphRecord:
    return GraphRecord.from_tuples(
        [
            (0, {"foo": "bar", "bar": "foo", "lorem": "ipsum"}),
            (1, {"foo": "bar", "bar": "foo"}),
            (2, {"foo": "bar", "bar": "foo"}),
            (3, {"foo": "bar", "bar": "test"}),
        ],
        [
            (0, 1, {"foo": "bar", "bar": "foo", "lorem": "ipsum"}),
            (1, 2, {"foo": "bar", "bar": "foo"}),
            (2, 3, {"foo": "bar", "bar": "foo"}),
            (3, 0, {"foo": "bar", "bar": "test"}),
        ],
    )


def node_greater_than_or_equal_two(node: NodeOperand) -> NodeIndicesOperand:
    node.index().greater_than_or_equal_to(2)

    return node.index()


def node_max(node: NodeOperand) -> NodeIndexOperand:
    node.index().greater_than(2)

    return node.index().max()


def node_max_greater_than_3(node: NodeOperand) -> NodeIndexOperand:
    max_index = node.index().max()
    max_index.greater_than(3)

    return max_index


def node_greater_than_three(node: NodeOperand) -> NodeIndicesOperand:
    node.index().greater_than(3)

    return node.index()


def node_less_than_two(node: NodeOperand) -> NodeIndicesOperand:
    node.index().less_than(2)

    return node.index()


def edge_greater_than_or_equal_two(edge: EdgeOperand) -> EdgeIndicesOperand:
    edge.index().greater_than_or_equal_to(2)

    return edge.index()


def edge_greater_than_three(edge: EdgeOperand) -> EdgeIndicesOperand:
    edge.index().greater_than(3)

    return edge.index()


def edge_less_than_two(edge: EdgeOperand) -> EdgeIndicesOperand:
    edge.index().less_than(2)

    return edge.index()


def edge_max(edge: EdgeOperand) -> EdgeIndexOperand:
    edge.index().greater_than(2)

    return edge.index().max()


def edge_max_greater_than_3(edge: EdgeOperand) -> EdgeIndexOperand:
    max_index = edge.index().max()
    max_index.greater_than(3)

    return max_index


class TestIndexers(unittest.TestCase):
    def test_node_getitem(self) -> None:
        graphrecord = create_graphrecord()

        assert graphrecord.node[0] == {"foo": "bar", "bar": "foo", "lorem": "ipsum"}

        # Accessing a non-existing node should fail
        with pytest.raises(IndexError):
            graphrecord.node[50]

        assert graphrecord.node[0, "foo"] == "bar"

        # Accessing a non-existing key should fail
        with pytest.raises(KeyError):
            graphrecord.node[0, "test"]

        assert graphrecord.node[0, ["foo", "bar"]] == {"foo": "bar", "bar": "foo"}

        # Accessing a non-existing key should fail
        with pytest.raises(KeyError):
            graphrecord.node[0, ["foo", "test"]]

        assert graphrecord.node[0, :] == {"foo": "bar", "bar": "foo", "lorem": "ipsum"}

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[0, 1:]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[0, :1]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[0, ::1]

        assert graphrecord.node[[0, 1]] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
        }

        with pytest.raises(IndexError):
            graphrecord.node[[0, 50]]

        assert graphrecord.node[[0, 1], "foo"] == {0: "bar", 1: "bar"}

        # Accessing a non-existing key should fail
        with pytest.raises(KeyError):
            graphrecord.node[[0, 1], "test"]

        # Accessing a key that doesn't exist in all nodes should fail
        with pytest.raises(KeyError):
            graphrecord.node[[0, 1], "lorem"]

        assert graphrecord.node[[0, 1], ["foo", "bar"]] == {
            0: {"foo": "bar", "bar": "foo"},
            1: {"foo": "bar", "bar": "foo"},
        }

        # Accessing a non-existing key should fail
        with pytest.raises(KeyError):
            graphrecord.node[[0, 1], ["foo", "test"]]

        # Accessing a key that doesn't exist in all nodes should fail
        with pytest.raises(KeyError):
            graphrecord.node[[0, 1], ["foo", "lorem"]]

        assert graphrecord.node[[0, 1], :] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
        }

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[[0, 1], 1:]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[[0, 1], :1]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[[0, 1], ::1]

        assert graphrecord.node[node_greater_than_or_equal_two] == {
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }
        assert graphrecord.node[node_max] == {"foo": "bar", "bar": "test"}

        # Empty query should not fail when using a NodeIndicesOperand
        assert graphrecord.node[node_greater_than_three] == {}

        # Query should fail when using a NodeIndexOperand with no return value
        with pytest.raises(IndexError, match="The query returned no results"):
            graphrecord.node[node_max_greater_than_3]

        assert graphrecord.node[node_greater_than_or_equal_two, "foo"] == {
            2: "bar",
            3: "bar",
        }

        assert graphrecord.node[node_max, "foo"] == "bar"

        with pytest.raises(IndexError, match="The query returned no results"):
            graphrecord.node[node_max_greater_than_3, "foo"]

        # Accessing a non-existing key should fail
        with pytest.raises(
            KeyError,
        ):
            graphrecord.node[node_greater_than_or_equal_two, "test"]

        assert graphrecord.node[node_greater_than_or_equal_two, ["foo", "bar"]] == {
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        assert graphrecord.node[node_max, ["foo", "bar"]] == {
            "foo": "bar",
            "bar": "test",
        }

        with pytest.raises(IndexError, match="The query returned no results"):
            graphrecord.node[node_max_greater_than_3, ["foo", "bar"]]

        # Accessing a non-existing key should fail
        with pytest.raises(KeyError):
            graphrecord.node[node_greater_than_or_equal_two, ["foo", "test"]]

        # Accessing a key that doesn't exist in all nodes should fail
        with pytest.raises(KeyError):
            graphrecord.node[node_less_than_two, ["foo", "lorem"]]

        assert graphrecord.node[node_greater_than_or_equal_two, :] == {
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }
        assert graphrecord.node[node_max, :] == {"foo": "bar", "bar": "test"}

        with pytest.raises(IndexError, match="The query returned no results"):
            graphrecord.node[node_max_greater_than_3, :]

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[node_greater_than_or_equal_two, 1:]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[node_greater_than_or_equal_two, :1]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[node_greater_than_or_equal_two, ::1]

        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[1:]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[:1]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[::1]

        assert graphrecord.node[:, "foo"] == {0: "bar", 1: "bar", 2: "bar", 3: "bar"}

        # Accessing a non-existing key should fail
        with pytest.raises(KeyError):
            graphrecord.node[:, "test"]

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[1:, "foo"]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[:1, "foo"]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[::1, "foo"]

        assert graphrecord.node[:, ["foo", "bar"]] == {
            0: {"foo": "bar", "bar": "foo"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        # Accessing a non-existing key should fail
        with pytest.raises(KeyError):
            graphrecord.node[:, ["foo", "test"]]

        # Accessing a key that doesn't exist in all nodes should fail
        with pytest.raises(KeyError):
            graphrecord.node[:, ["foo", "lorem"]]

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[1:, ["foo", "bar"]]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[:1, ["foo", "bar"]]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[::1, ["foo", "bar"]]

        assert graphrecord.node[:, :] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[1:, :]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[:1, :]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[::1, :]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[:, 1:]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[:, :1]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[:, ::1]

    def test_node_setitem(self) -> None:
        # Updating existing attributes

        graphrecord = create_graphrecord()
        graphrecord.node[0] = {"foo": "bar", "bar": "test"}
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "test"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.node[[0, 1]] = {"foo": "bar", "bar": "test"}
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "test"},
            1: {"foo": "bar", "bar": "test"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.node[node_greater_than_or_equal_two] = {
            "foo": "test",
            "bar": "test2",
        }
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "test", "bar": "test2"},
            3: {"foo": "test", "bar": "test2"},
        }

        graphrecord = create_graphrecord()
        graphrecord.node[node_max] = {"foo": "test", "bar": "test2"}
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "test", "bar": "test2"},
        }

        graphrecord = create_graphrecord()
        graphrecord.node[node_max_greater_than_3] = {"foo": "test", "bar": "test2"}
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[1:] = {"foo": "bar", "bar": "test"}

        graphrecord = create_graphrecord()
        graphrecord.node[:] = {"foo": "bar", "bar": "test"}
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "test"},
            1: {"foo": "bar", "bar": "test"},
            2: {"foo": "bar", "bar": "test"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        # Updating a non-existing node should fail
        with pytest.raises(IndexError):
            graphrecord.node[50] = {"foo": "bar", "test": "test"}

        graphrecord = create_graphrecord()
        graphrecord.node[0, "foo"] = "test"
        assert graphrecord.node[:] == {
            0: {"foo": "test", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.node[0, ["foo", "bar"]] = "test"
        assert graphrecord.node[:] == {
            0: {"foo": "test", "bar": "test", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.node[0, :] = "test"
        assert graphrecord.node[:] == {
            0: {"foo": "test", "bar": "test", "lorem": "test"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[0, 1:] = "test"
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[0, :1] = "test"
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[0, ::1] = "test"

        graphrecord = create_graphrecord()
        graphrecord.node[[0, 1], "foo"] = "test"
        assert graphrecord.node[:] == {
            0: {"foo": "test", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "test", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.node[[0, 1], ["foo", "bar"]] = "test"
        assert graphrecord.node[:] == {
            0: {"foo": "test", "bar": "test", "lorem": "ipsum"},
            1: {"foo": "test", "bar": "test"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.node[[0, 1], :] = "test"
        assert graphrecord.node[:] == {
            0: {"foo": "test", "bar": "test", "lorem": "test"},
            1: {"foo": "test", "bar": "test"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[[0, 1], 1:] = "test"
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[[0, 1], :1] = "test"
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[[0, 1], ::1] = "test"

        graphrecord = create_graphrecord()
        graphrecord.node[node_greater_than_or_equal_two] = {"foo": "bar", "bar": "test"}
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "test"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        # Empty query should not fail
        graphrecord.node[node_greater_than_three] = {"foo": "bar", "bar": "test"}

        graphrecord = create_graphrecord()
        graphrecord.node[node_greater_than_or_equal_two, "foo"] = "test"
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "test", "bar": "foo"},
            3: {"foo": "test", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.node[node_max, "foo"] = "test"
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "test", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.node[node_max_greater_than_3, "foo"] = "test"
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.node[node_greater_than_or_equal_two, ["foo", "bar"]] = "test"
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "test", "bar": "test"},
            3: {"foo": "test", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.node[node_max, ["foo", "bar"]] = "test"
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "test", "bar": "test"},
        }

        graphrecord.node[node_max_greater_than_3, ["foo", "bar"]] = "test"
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "test", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.node[node_greater_than_or_equal_two, :] = "test"
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "test", "bar": "test"},
            3: {"foo": "test", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.node[node_max, :] = "test"
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "test", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.node[node_max_greater_than_3, :] = "test"
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[node_greater_than_or_equal_two, 1:] = "test"
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[node_greater_than_or_equal_two, :1] = "test"
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[node_greater_than_or_equal_two, ::1] = "test"

        graphrecord = create_graphrecord()
        graphrecord.node[:, "foo"] = "test"
        assert graphrecord.node[:] == {
            0: {"foo": "test", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "test", "bar": "foo"},
            2: {"foo": "test", "bar": "foo"},
            3: {"foo": "test", "bar": "test"},
        }

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[1:, "foo"] = "test"
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[:1, "foo"] = "test"
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[::1, "foo"] = "test"

        graphrecord = create_graphrecord()
        graphrecord.node[:, ["foo", "bar"]] = "test"
        assert graphrecord.node[:] == {
            0: {"foo": "test", "bar": "test", "lorem": "ipsum"},
            1: {"foo": "test", "bar": "test"},
            2: {"foo": "test", "bar": "test"},
            3: {"foo": "test", "bar": "test"},
        }

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[1:, ["foo", "bar"]] = "test"
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[:1, ["foo", "bar"]] = "test"
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[::1, ["foo", "bar"]] = "test"

        graphrecord = create_graphrecord()
        graphrecord.node[:, :] = "test"
        assert graphrecord.node[:] == {
            0: {"foo": "test", "bar": "test", "lorem": "test"},
            1: {"foo": "test", "bar": "test"},
            2: {"foo": "test", "bar": "test"},
            3: {"foo": "test", "bar": "test"},
        }

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[1:, :] = "test"
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[:1, :] = "test"
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[::1, :] = "test"
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[:, 1:] = "test"
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[:, :1] = "test"
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.node[:, ::1] = "test"

        # Adding new attributes

        graphrecord = create_graphrecord()
        graphrecord.node[0, "test"] = "test"
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum", "test": "test"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.node[0, ["test", "test2"]] = "test"
        assert graphrecord.node[:] == {
            0: {
                "foo": "bar",
                "bar": "foo",
                "lorem": "ipsum",
                "test": "test",
                "test2": "test",
            },
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.node[[0, 1], "test"] = "test"
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum", "test": "test"},
            1: {"foo": "bar", "bar": "foo", "test": "test"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.node[[0, 1], ["test", "test2"]] = "test"
        assert graphrecord.node[:] == {
            0: {
                "foo": "bar",
                "bar": "foo",
                "lorem": "ipsum",
                "test": "test",
                "test2": "test",
            },
            1: {"foo": "bar", "bar": "foo", "test": "test", "test2": "test"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.node[node_greater_than_or_equal_two, "test"] = "test"
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo", "test": "test"},
            3: {"foo": "bar", "bar": "test", "test": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.node[node_greater_than_or_equal_two, ["test", "test2"]] = "test"
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo", "test": "test", "test2": "test"},
            3: {"foo": "bar", "bar": "test", "test": "test", "test2": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.node[:, "test"] = "test"
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum", "test": "test"},
            1: {"foo": "bar", "bar": "foo", "test": "test"},
            2: {"foo": "bar", "bar": "foo", "test": "test"},
            3: {"foo": "bar", "bar": "test", "test": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.node[:, ["test", "test2"]] = "test"
        assert graphrecord.node[:] == {
            0: {
                "foo": "bar",
                "bar": "foo",
                "lorem": "ipsum",
                "test": "test",
                "test2": "test",
            },
            1: {"foo": "bar", "bar": "foo", "test": "test", "test2": "test"},
            2: {"foo": "bar", "bar": "foo", "test": "test", "test2": "test"},
            3: {"foo": "bar", "bar": "test", "test": "test", "test2": "test"},
        }

        # Adding and updating attributes

        graphrecord = create_graphrecord()
        graphrecord.node[[0, 1], "lorem"] = "test"
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "test"},
            1: {"foo": "bar", "bar": "foo", "lorem": "test"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.node[[0, 1], ["lorem", "test"]] = "test"
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "test", "test": "test"},
            1: {"foo": "bar", "bar": "foo", "lorem": "test", "test": "test"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.node[node_less_than_two, "lorem"] = "test"
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "test"},
            1: {"foo": "bar", "bar": "foo", "lorem": "test"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.node[node_less_than_two, ["lorem", "test"]] = "test"
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "test", "test": "test"},
            1: {"foo": "bar", "bar": "foo", "lorem": "test", "test": "test"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.node[:, "lorem"] = "test"
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "test"},
            1: {"foo": "bar", "bar": "foo", "lorem": "test"},
            2: {"foo": "bar", "bar": "foo", "lorem": "test"},
            3: {"foo": "bar", "bar": "test", "lorem": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.node[:, ["lorem", "test"]] = "test"
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "test", "test": "test"},
            1: {"foo": "bar", "bar": "foo", "lorem": "test", "test": "test"},
            2: {"foo": "bar", "bar": "foo", "lorem": "test", "test": "test"},
            3: {"foo": "bar", "bar": "test", "lorem": "test", "test": "test"},
        }

    def test_node_delitem(self) -> None:
        graphrecord = create_graphrecord()
        del graphrecord.node[0, "foo"]
        assert graphrecord.node[:] == {
            0: {"bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        # Removing from a non-existing node should fail
        with pytest.raises(IndexError):
            del graphrecord.node[50, "foo"]

        graphrecord = create_graphrecord()
        # Removing a non-existing key should fail
        with pytest.raises(KeyError):
            del graphrecord.node[0, "test"]

        graphrecord = create_graphrecord()
        del graphrecord.node[0, ["foo", "bar"]]
        assert graphrecord.node[:] == {
            0: {"lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        # Removing a non-existing key should fail
        with pytest.raises(KeyError):
            del graphrecord.node[0, ["foo", "test"]]

        graphrecord = create_graphrecord()
        del graphrecord.node[0, :]
        assert graphrecord.node[:] == {
            0: {},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.node[0, 1:]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.node[0, :1]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.node[0, ::1]

        graphrecord = create_graphrecord()
        del graphrecord.node[[0, 1], "foo"]
        assert graphrecord.node[:] == {
            0: {"bar": "foo", "lorem": "ipsum"},
            1: {"bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        # Removing from a non-existing node should fail
        with pytest.raises(IndexError):
            del graphrecord.node[[0, 50], "foo"]

        graphrecord = create_graphrecord()
        # Removing a non-existing key should fail
        with pytest.raises(KeyError):
            del graphrecord.node[[0, 1], "test"]

        graphrecord = create_graphrecord()
        del graphrecord.node[[0, 1], ["foo", "bar"]]
        assert graphrecord.node[:] == {
            0: {"lorem": "ipsum"},
            1: {},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        # Removing a non-existing key should fail
        with pytest.raises(KeyError):
            del graphrecord.node[[0, 1], ["foo", "test"]]

        graphrecord = create_graphrecord()
        # Removing a key that doesn't exist in all nodes should fail
        with pytest.raises(KeyError):
            del graphrecord.node[[0, 1], ["foo", "lorem"]]

        graphrecord = create_graphrecord()
        del graphrecord.node[[0, 1], :]
        assert graphrecord.node[:] == {
            0: {},
            1: {},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.node[[0, 1], 1:]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.node[[0, 1], :1]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.node[[0, 1], ::1]

        graphrecord = create_graphrecord()
        del graphrecord.node[node_greater_than_or_equal_two, "foo"]
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"bar": "foo"},
            3: {"bar": "test"},
        }

        graphrecord = create_graphrecord()
        del graphrecord.node[node_max, "foo"]
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"bar": "test"},
        }

        graphrecord = create_graphrecord()
        del graphrecord.node[node_max_greater_than_3, "foo"]
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        # Empty query should not fail
        del graphrecord.node[node_greater_than_three, "foo"]
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        # Removing a non-existing key should fail
        with pytest.raises(KeyError):
            del graphrecord.node[node_greater_than_or_equal_two, "test"]

        graphrecord = create_graphrecord()
        del graphrecord.node[node_greater_than_or_equal_two, ["foo", "bar"]]
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {},
            3: {},
        }

        graphrecord = create_graphrecord()
        del graphrecord.node[node_max, ["foo", "bar"]]
        assert graphrecord.node[:] == {
            1: {"foo": "bar", "bar": "foo"},
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            2: {"foo": "bar", "bar": "foo"},
            3: {},
        }

        graphrecord = create_graphrecord()
        del graphrecord.node[node_max_greater_than_3, ["foo", "bar"]]
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        # Removing a non-existing key should fail
        with pytest.raises(KeyError):
            del graphrecord.node[node_greater_than_or_equal_two, ["foo", "test"]]

        graphrecord = create_graphrecord()
        # Removing a key that doesn't exist in all nodes should fail
        with pytest.raises(KeyError):
            del graphrecord.node[node_less_than_two, ["foo", "lorem"]]

        graphrecord = create_graphrecord()
        del graphrecord.node[node_greater_than_or_equal_two, :]
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {},
            3: {},
        }

        graphrecord = create_graphrecord()
        del graphrecord.node[node_max, :]
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {},
        }

        graphrecord = create_graphrecord()
        del graphrecord.node[node_max_greater_than_3, :]
        assert graphrecord.node[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.node[node_greater_than_or_equal_two, 1:]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.node[node_greater_than_or_equal_two, :1]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.node[node_greater_than_or_equal_two, ::1]

        graphrecord = create_graphrecord()
        del graphrecord.node[:, "foo"]
        assert graphrecord.node[:] == {
            0: {"bar": "foo", "lorem": "ipsum"},
            1: {"bar": "foo"},
            2: {"bar": "foo"},
            3: {"bar": "test"},
        }

        graphrecord = create_graphrecord()
        # Removing a non-existing key should fail
        with pytest.raises(KeyError):
            del graphrecord.node[:, "test"]

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.node[1:, "foo"]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.node[:1, "foo"]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.node[::1, "foo"]

        graphrecord = create_graphrecord()
        del graphrecord.node[:, ["foo", "bar"]]
        assert graphrecord.node[:] == {0: {"lorem": "ipsum"}, 1: {}, 2: {}, 3: {}}

        graphrecord = create_graphrecord()
        # Removing a non-existing key should fail
        with pytest.raises(KeyError):
            del graphrecord.node[:, ["foo", "test"]]

        graphrecord = create_graphrecord()
        # Removing a key that doesn't exist in all nodes should fail
        with pytest.raises(KeyError):
            del graphrecord.node[:, ["foo", "lorem"]]

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.node[1:, ["foo", "bar"]]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.node[:1, ["foo", "bar"]]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.node[::1, ["foo", "bar"]]

        graphrecord = create_graphrecord()
        del graphrecord.node[:, :]
        assert graphrecord.node[:] == {0: {}, 1: {}, 2: {}, 3: {}}

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.node[1:, :]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.node[:1, :]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.node[::1, :]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.node[:, 1:]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.node[:, :1]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.node[:, ::1]

    def test_edge_getitem(self) -> None:
        graphrecord = create_graphrecord()

        assert graphrecord.edge[0] == {"foo": "bar", "bar": "foo", "lorem": "ipsum"}

        # Accessing a non-existing edge should fail
        with pytest.raises(IndexError):
            graphrecord.edge[50]

        assert graphrecord.edge[0, "foo"] == "bar"

        # Accessing a non-existing key should fail
        with pytest.raises(KeyError):
            graphrecord.edge[0, "test"]

        assert graphrecord.edge[0, ["foo", "bar"]] == {"foo": "bar", "bar": "foo"}

        # Accessing a non-existing key should fail
        with pytest.raises(KeyError):
            graphrecord.edge[0, ["foo", "test"]]

        assert graphrecord.edge[0, :] == {"foo": "bar", "bar": "foo", "lorem": "ipsum"}

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[0, 1:]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[0, :1]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[0, ::1]

        assert graphrecord.edge[[0, 1]] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
        }

        with pytest.raises(IndexError):
            graphrecord.edge[[0, 50]]

        assert graphrecord.edge[[0, 1], "foo"] == {0: "bar", 1: "bar"}

        # Accessing a non-existing key should fail
        with pytest.raises(KeyError):
            graphrecord.edge[[0, 1], "test"]

        # Accessing a key that doesn't exist in all edges should fail
        with pytest.raises(KeyError):
            graphrecord.edge[[0, 1], "lorem"]

        assert graphrecord.edge[[0, 1], ["foo", "bar"]] == {
            0: {"foo": "bar", "bar": "foo"},
            1: {"foo": "bar", "bar": "foo"},
        }

        # Accessing a non-existing key should fail
        with pytest.raises(KeyError):
            graphrecord.edge[[0, 1], ["foo", "test"]]

        # Accessing a key that doesn't exist in all edges should fail
        with pytest.raises(KeyError):
            graphrecord.edge[[0, 1], ["foo", "lorem"]]

        assert graphrecord.edge[[0, 1], :] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
        }

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[[0, 1], 1:]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[[0, 1], :1]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[[0, 1], ::1]

        assert graphrecord.edge[edge_greater_than_or_equal_two] == {
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        assert graphrecord.edge[edge_max] == {"foo": "bar", "bar": "test"}

        with pytest.raises(IndexError, match="The query returned no results"):
            graphrecord.edge[edge_max_greater_than_3]

        # Empty query should not fail
        assert graphrecord.edge[edge_greater_than_three] == {}

        assert graphrecord.edge[edge_greater_than_or_equal_two, "foo"] == {
            2: "bar",
            3: "bar",
        }

        assert graphrecord.edge[edge_max, "foo"] == "bar"

        with pytest.raises(IndexError, match="The query returned no results"):
            graphrecord.edge[edge_max_greater_than_3, "foo"]

        # Accessing a non-existing key should fail
        with pytest.raises(KeyError):
            graphrecord.edge[edge_greater_than_or_equal_two, "test"]

        assert graphrecord.edge[edge_greater_than_or_equal_two, ["foo", "bar"]] == {
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        assert graphrecord.edge[edge_max, ["foo", "bar"]] == {
            "foo": "bar",
            "bar": "test",
        }

        with pytest.raises(IndexError, match="The query returned no results"):
            graphrecord.edge[edge_max_greater_than_3, ["foo", "bar"]]

        # Accessing a non-existing key should fail
        with pytest.raises(KeyError):
            graphrecord.edge[edge_greater_than_or_equal_two, ["foo", "test"]]

        # Accessing a key that doesn't exist in all edges should fail
        with pytest.raises(KeyError):
            graphrecord.edge[edge_less_than_two, ["foo", "lorem"]]

        assert graphrecord.edge[edge_greater_than_or_equal_two, :] == {
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        assert graphrecord.edge[edge_max, :] == {"foo": "bar", "bar": "test"}

        with pytest.raises(IndexError, match="The query returned no results"):
            graphrecord.edge[edge_max_greater_than_3, :]

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[edge_greater_than_or_equal_two, 1:]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[edge_greater_than_or_equal_two, :1]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[edge_greater_than_or_equal_two, ::1]

        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[1:]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[:1]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[::1]

        assert graphrecord.edge[:, "foo"] == {0: "bar", 1: "bar", 2: "bar", 3: "bar"}

        # Accessing a non-existing key should fail
        with pytest.raises(KeyError):
            graphrecord.edge[:, "test"]

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[1:, "foo"]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[:1, "foo"]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[::1, "foo"]

        assert graphrecord.edge[:, ["foo", "bar"]] == {
            0: {"foo": "bar", "bar": "foo"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        # Accessing a non-existing key should fail
        with pytest.raises(KeyError):
            graphrecord.edge[:, ["foo", "test"]]

        # Accessing a key that doesn't exist in all edges should fail
        with pytest.raises(KeyError):
            graphrecord.edge[:, ["foo", "lorem"]]

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[1:, ["foo", "bar"]]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[:1, ["foo", "bar"]]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[::1, ["foo", "bar"]]

        assert graphrecord.edge[:, :] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[1:, :]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[:1, :]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[::1, :]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[:, 1:]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[:, :1]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[:, ::1]

    def test_edge_setitem(self) -> None:
        # Updating existing attributes

        graphrecord = create_graphrecord()
        graphrecord.edge[0] = {"foo": "bar", "bar": "test"}
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "test"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.edge[[0, 1]] = {"foo": "test", "bar": "test1"}
        assert graphrecord.edge[:] == {
            0: {"foo": "test", "bar": "test1"},
            1: {"foo": "test", "bar": "test1"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        # Updating a non-existing edge should fail
        with pytest.raises(IndexError):
            graphrecord.edge[50] = {"foo": "bar", "test": "test"}

        graphrecord = create_graphrecord()
        graphrecord.edge[0, "foo"] = "test"
        assert graphrecord.edge[:] == {
            0: {"foo": "test", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.edge[0, ["foo", "bar"]] = "test"
        assert graphrecord.edge[:] == {
            0: {"foo": "test", "bar": "test", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.edge[0, :] = "test"
        assert graphrecord.edge[:] == {
            0: {"foo": "test", "bar": "test", "lorem": "test"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[0, 1:] = "test"
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[0, :1] = "test"
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[0, ::1] = "test"

        graphrecord = create_graphrecord()
        graphrecord.edge[[0, 1], "foo"] = "test"
        assert graphrecord.edge[:] == {
            0: {"foo": "test", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "test", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.edge[[0, 1], ["foo", "bar"]] = "test"
        assert graphrecord.edge[:] == {
            0: {"foo": "test", "bar": "test", "lorem": "ipsum"},
            1: {"foo": "test", "bar": "test"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.edge[[0, 1], :] = "test"
        assert graphrecord.edge[:] == {
            0: {"foo": "test", "bar": "test", "lorem": "test"},
            1: {"foo": "test", "bar": "test"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[[0, 1], 1:] = "test"
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[[0, 1], :1] = "test"
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[[0, 1], ::1] = "test"

        graphrecord = create_graphrecord()
        graphrecord.edge[edge_greater_than_or_equal_two] = {"foo": "bar", "bar": "test"}
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "test"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.edge[edge_max] = {"foo": "test", "bar": "test1"}
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "test", "bar": "test1"},
        }

        graphrecord = create_graphrecord()
        graphrecord.edge[edge_max_greater_than_3] = {"foo": "test", "bar": "test1"}
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        # Empty query should not fail
        graphrecord.edge[edge_greater_than_three] = {"foo": "bar", "bar": "test"}

        graphrecord = create_graphrecord()
        graphrecord.edge[edge_greater_than_or_equal_two, "foo"] = "test"
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "test", "bar": "foo"},
            3: {"foo": "test", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.edge[edge_max, "foo"] = "test"
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "test", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.edge[edge_max_greater_than_3, "foo"] = "test"
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.edge[edge_greater_than_or_equal_two, ["foo", "bar"]] = "test"
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "test", "bar": "test"},
            3: {"foo": "test", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.edge[edge_max, ["foo", "bar"]] = "test"
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "test", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.edge[edge_max_greater_than_3, ["foo", "bar"]] = "test"
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.edge[edge_greater_than_or_equal_two, :] = "test"
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "test", "bar": "test"},
            3: {"foo": "test", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.edge[edge_max, :] = "test"
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "test", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.edge[edge_max_greater_than_3, :] = "test"
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[edge_greater_than_or_equal_two, 1:] = "test"
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[edge_greater_than_or_equal_two, :1] = "test"
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[edge_greater_than_or_equal_two, ::1] = "test"

        graphrecord = create_graphrecord()
        graphrecord.edge[:, "foo"] = "test"
        assert graphrecord.edge[:] == {
            0: {"foo": "test", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "test", "bar": "foo"},
            2: {"foo": "test", "bar": "foo"},
            3: {"foo": "test", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.edge[:] = {"foo": "bar", "bar": "test"}
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "test"},
            1: {"foo": "bar", "bar": "test"},
            2: {"foo": "bar", "bar": "test"},
            3: {"foo": "bar", "bar": "test"},
        }

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[1:] = {"foo": "bar", "bar": "test"}

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[1:, "foo"] = "test"
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[:1, "foo"] = "test"
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[::1, "foo"] = "test"

        graphrecord = create_graphrecord()
        graphrecord.edge[:, ["foo", "bar"]] = "test"
        assert graphrecord.edge[:] == {
            0: {"foo": "test", "bar": "test", "lorem": "ipsum"},
            1: {"foo": "test", "bar": "test"},
            2: {"foo": "test", "bar": "test"},
            3: {"foo": "test", "bar": "test"},
        }

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[1:, ["foo", "bar"]] = "test"
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[:1, ["foo", "bar"]] = "test"
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[::1, ["foo", "bar"]] = "test"

        graphrecord = create_graphrecord()
        graphrecord.edge[:, :] = "test"
        assert graphrecord.edge[:] == {
            0: {"foo": "test", "bar": "test", "lorem": "test"},
            1: {"foo": "test", "bar": "test"},
            2: {"foo": "test", "bar": "test"},
            3: {"foo": "test", "bar": "test"},
        }

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[1:, :] = "test"
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[:1, :] = "test"
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[::1, :] = "test"
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[:, 1:] = "test"
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[:, :1] = "test"
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            graphrecord.edge[:, ::1] = "test"

        # Adding new attributes

        graphrecord = create_graphrecord()
        graphrecord.edge[0, "test"] = "test"
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum", "test": "test"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.edge[0, ["test", "test2"]] = "test"
        assert graphrecord.edge[:] == {
            0: {
                "foo": "bar",
                "bar": "foo",
                "lorem": "ipsum",
                "test": "test",
                "test2": "test",
            },
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.edge[[0, 1], "test"] = "test"
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum", "test": "test"},
            1: {"foo": "bar", "bar": "foo", "test": "test"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.edge[[0, 1], ["test", "test2"]] = "test"
        assert graphrecord.edge[:] == {
            0: {
                "foo": "bar",
                "bar": "foo",
                "lorem": "ipsum",
                "test": "test",
                "test2": "test",
            },
            1: {"foo": "bar", "bar": "foo", "test": "test", "test2": "test"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.edge[edge_greater_than_or_equal_two, "test"] = "test"
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo", "test": "test"},
            3: {"foo": "bar", "bar": "test", "test": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.edge[edge_greater_than_or_equal_two, ["test", "test2"]] = "test"
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo", "test": "test", "test2": "test"},
            3: {"foo": "bar", "bar": "test", "test": "test", "test2": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.edge[:, "test"] = "test"
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum", "test": "test"},
            1: {"foo": "bar", "bar": "foo", "test": "test"},
            2: {"foo": "bar", "bar": "foo", "test": "test"},
            3: {"foo": "bar", "bar": "test", "test": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.edge[:, ["test", "test2"]] = "test"
        assert graphrecord.edge[:] == {
            0: {
                "foo": "bar",
                "bar": "foo",
                "lorem": "ipsum",
                "test": "test",
                "test2": "test",
            },
            1: {"foo": "bar", "bar": "foo", "test": "test", "test2": "test"},
            2: {"foo": "bar", "bar": "foo", "test": "test", "test2": "test"},
            3: {"foo": "bar", "bar": "test", "test": "test", "test2": "test"},
        }

        # Adding and updating attributes

        graphrecord = create_graphrecord()
        graphrecord.edge[[0, 1], "lorem"] = "test"
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "test"},
            1: {"foo": "bar", "bar": "foo", "lorem": "test"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.edge[[0, 1], ["lorem", "test"]] = "test"
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "test", "test": "test"},
            1: {"foo": "bar", "bar": "foo", "lorem": "test", "test": "test"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.edge[edge_less_than_two, "lorem"] = "test"
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "test"},
            1: {"foo": "bar", "bar": "foo", "lorem": "test"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.edge[edge_less_than_two, ["lorem", "test"]] = "test"
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "test", "test": "test"},
            1: {"foo": "bar", "bar": "foo", "lorem": "test", "test": "test"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.edge[:, "lorem"] = "test"
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "test"},
            1: {"foo": "bar", "bar": "foo", "lorem": "test"},
            2: {"foo": "bar", "bar": "foo", "lorem": "test"},
            3: {"foo": "bar", "bar": "test", "lorem": "test"},
        }

        graphrecord = create_graphrecord()
        graphrecord.edge[:, ["lorem", "test"]] = "test"
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "test", "test": "test"},
            1: {"foo": "bar", "bar": "foo", "lorem": "test", "test": "test"},
            2: {"foo": "bar", "bar": "foo", "lorem": "test", "test": "test"},
            3: {"foo": "bar", "bar": "test", "lorem": "test", "test": "test"},
        }

    def test_edge_delitem(self) -> None:
        graphrecord = create_graphrecord()
        del graphrecord.edge[0, "foo"]
        assert graphrecord.edge[:] == {
            0: {"bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        # Removing from a non-existing edge should fail
        with pytest.raises(IndexError):
            del graphrecord.edge[50, "foo"]

        graphrecord = create_graphrecord()
        # Removing a non-existing key should fail
        with pytest.raises(KeyError):
            del graphrecord.edge[0, "test"]

        graphrecord = create_graphrecord()
        del graphrecord.edge[0, ["foo", "bar"]]
        assert graphrecord.edge[:] == {
            0: {"lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        # Removing a non-existing key should fail
        with pytest.raises(KeyError):
            del graphrecord.edge[0, ["foo", "test"]]

        graphrecord = create_graphrecord()
        del graphrecord.edge[0, :]
        assert graphrecord.edge[:] == {
            0: {},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.edge[0, 1:]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.edge[0, :1]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.edge[0, ::1]

        graphrecord = create_graphrecord()
        del graphrecord.edge[[0, 1], "foo"]
        assert graphrecord.edge[:] == {
            0: {"bar": "foo", "lorem": "ipsum"},
            1: {"bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        # Removing from a non-existing edge should fail
        with pytest.raises(IndexError):
            del graphrecord.edge[[0, 50], "foo"]

        graphrecord = create_graphrecord()
        # Removing a non-existing key should fail
        with pytest.raises(KeyError):
            del graphrecord.edge[[0, 1], "test"]

        graphrecord = create_graphrecord()
        del graphrecord.edge[[0, 1], ["foo", "bar"]]
        assert graphrecord.edge[:] == {
            0: {"lorem": "ipsum"},
            1: {},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        # Removing a non-existing key should fail
        with pytest.raises(KeyError):
            del graphrecord.edge[[0, 1], ["foo", "test"]]

        graphrecord = create_graphrecord()
        # Removing a key that doesn't exist in all edges should fail
        with pytest.raises(KeyError):
            del graphrecord.edge[[0, 1], ["foo", "lorem"]]

        graphrecord = create_graphrecord()
        del graphrecord.edge[[0, 1], :]
        assert graphrecord.edge[:] == {
            0: {},
            1: {},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.edge[[0, 1], 1:]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.edge[[0, 1], :1]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.edge[[0, 1], ::1]

        graphrecord = create_graphrecord()
        del graphrecord.edge[edge_greater_than_or_equal_two, "foo"]
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"bar": "foo"},
            3: {"bar": "test"},
        }

        graphrecord = create_graphrecord()
        del graphrecord.edge[edge_max, "foo"]
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"bar": "test"},
        }

        graphrecord = create_graphrecord()
        with pytest.raises(IndexError, match="The query returned no results"):
            del graphrecord.edge[edge_max_greater_than_3, "foo"]

        graphrecord = create_graphrecord()
        # Empty query should not fail
        del graphrecord.edge[edge_greater_than_three, "foo"]
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        graphrecord = create_graphrecord()
        # Removing a non-existing key should fail
        with pytest.raises(KeyError):
            del graphrecord.edge[edge_greater_than_or_equal_two, "test"]

        graphrecord = create_graphrecord()
        del graphrecord.edge[edge_greater_than_or_equal_two, ["foo", "bar"]]
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {},
            3: {},
        }

        graphrecord = create_graphrecord()
        del graphrecord.edge[edge_max, ["foo", "bar"]]
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {},
        }

        graphrecord = create_graphrecord()
        # Removing a non-existing key should fail
        with pytest.raises(KeyError):
            del graphrecord.edge[edge_greater_than_or_equal_two, ["foo", "test"]]

        graphrecord = create_graphrecord()
        # Removing a key that doesn't exist in all edges should fail
        with pytest.raises(KeyError):
            del graphrecord.edge[edge_less_than_two, ["foo", "lorem"]]

        graphrecord = create_graphrecord()
        del graphrecord.edge[edge_greater_than_or_equal_two, :]
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {},
            3: {},
        }

        graphrecord = create_graphrecord()
        del graphrecord.edge[edge_max, :]
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {},
        }

        graphrecord = create_graphrecord()
        del graphrecord.edge[edge_max_greater_than_3, :]
        assert graphrecord.edge[:] == {
            0: {"foo": "bar", "bar": "foo", "lorem": "ipsum"},
            1: {"foo": "bar", "bar": "foo"},
            2: {"foo": "bar", "bar": "foo"},
            3: {"foo": "bar", "bar": "test"},
        }

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.edge[edge_greater_than_or_equal_two, 1:]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.edge[edge_greater_than_or_equal_two, :1]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.edge[edge_greater_than_or_equal_two, ::1]

        graphrecord = create_graphrecord()
        del graphrecord.edge[:, "foo"]
        assert graphrecord.edge[:] == {
            0: {"bar": "foo", "lorem": "ipsum"},
            1: {"bar": "foo"},
            2: {"bar": "foo"},
            3: {"bar": "test"},
        }

        graphrecord = create_graphrecord()
        # Removing a non-existing key should fail
        with pytest.raises(KeyError):
            del graphrecord.edge[:, "test"]

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.edge[1:, "foo"]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.edge[:1, "foo"]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.edge[::1, "foo"]

        graphrecord = create_graphrecord()
        del graphrecord.edge[:, ["foo", "bar"]]
        assert graphrecord.edge[:] == {0: {"lorem": "ipsum"}, 1: {}, 2: {}, 3: {}}

        graphrecord = create_graphrecord()
        # Removing a non-existing key should fail
        with pytest.raises(KeyError):
            del graphrecord.edge[:, ["foo", "test"]]

        graphrecord = create_graphrecord()
        # Removing a key that doesn't exist in all edges should fail
        with pytest.raises(KeyError):
            del graphrecord.edge[:, ["foo", "lorem"]]

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.edge[1:, ["foo", "bar"]]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.edge[:1, ["foo", "bar"]]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.edge[::1, ["foo", "bar"]]

        graphrecord = create_graphrecord()
        del graphrecord.edge[:, :]
        assert graphrecord.edge[:] == {0: {}, 1: {}, 2: {}, 3: {}}

        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.edge[1:, :]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.edge[:1, :]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.edge[::1, :]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.edge[:, 1:]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.edge[:, :1]
        with pytest.raises(ValueError, match="Invalid slice, only ':' is allowed"):
            del graphrecord.edge[:, ::1]


if __name__ == "__main__":
    run_test = unittest.TestLoader().loadTestsFromTestCase(TestIndexers)
    unittest.TextTestRunner(verbosity=2).run(run_test)
