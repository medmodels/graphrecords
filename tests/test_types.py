import pickle
import unittest

from graphrecords import EdgeDirection, GraphRecord
from graphrecords.types import EdgeIndex


def create_graphrecord() -> GraphRecord:
    record = GraphRecord()
    record = record.add_node("0", {"lorem": "ipsum"})
    record = record.add_node("1", {"dolor": "sit"})
    record = record.add_edge("0", "1", {"amet": "consectetur"})
    return record.add_edge("1", "0", {"adipiscing": "elit"})


def create_directed_graphrecord() -> GraphRecord:
    record = GraphRecord()
    record = record.add_node("0", {"lorem": "ipsum"})
    record = record.add_node("1", {"dolor": "sit"})
    return record.add_edge("0", "1", {"amet": "consectetur"})


class TestEdgeIndex(unittest.TestCase):
    def test_eq(self) -> None:
        record = create_graphrecord()
        first_edge_index, second_edge_index = record.edge_indices()

        assert isinstance(first_edge_index, EdgeIndex)
        assert first_edge_index == record.edge_indices()[0]
        assert first_edge_index != second_edge_index
        assert first_edge_index != "lorem"

    def test_hash(self) -> None:
        record = create_graphrecord()
        first_edge_index, second_edge_index = record.edge_indices()

        assert hash(first_edge_index) == hash(record.edge_indices()[0])
        assert {first_edge_index: "lorem", second_edge_index: "ipsum"} == {
            first_edge_index: "lorem",
            second_edge_index: "ipsum",
        }
        assert {first_edge_index, second_edge_index, record.edge_indices()[0]} == {
            first_edge_index,
            second_edge_index,
        }

    def test_repr(self) -> None:
        record = create_graphrecord()
        first_edge_index, second_edge_index = record.edge_indices()

        assert repr(first_edge_index) == f"EdgeIndex({first_edge_index})"
        assert repr(first_edge_index) == repr(record.edge_indices()[0])
        assert repr(first_edge_index) != repr(second_edge_index)

    def test_str(self) -> None:
        record = create_graphrecord()
        first_edge_index, second_edge_index = record.edge_indices()

        assert str(first_edge_index) == str(record.edge_indices()[0])
        assert str(first_edge_index) != str(second_edge_index)

    def test_reduce(self) -> None:
        record = create_graphrecord()
        first_edge_index = record.edge_indices()[0]

        restored_edge_index = pickle.loads(pickle.dumps(first_edge_index))

        assert restored_edge_index == first_edge_index
        assert record.contains_edge(restored_edge_index)


class TestEdgeDirection(unittest.TestCase):
    def test_into_py_edge_direction(self) -> None:
        record = create_directed_graphrecord()
        edge_index = record.edge_indices()[0]
        source_node = record.node("0")
        target_node = record.node("1")

        assert source_node.edges(EdgeDirection.Outgoing) == [edge_index]
        assert source_node.edges(EdgeDirection.Incoming) == []
        assert source_node.edges(EdgeDirection.Both) == [edge_index]
        assert target_node.edges(EdgeDirection.Incoming) == [edge_index]
        assert target_node.edges(EdgeDirection.Outgoing) == []

    def test_repr(self) -> None:
        assert repr(EdgeDirection.Incoming) == "EdgeDirection.Incoming"
        assert repr(EdgeDirection.Outgoing) == "EdgeDirection.Outgoing"
        assert repr(EdgeDirection.Both) == "EdgeDirection.Both"

    def test_str(self) -> None:
        assert str(EdgeDirection.Incoming) == "Incoming"
        assert str(EdgeDirection.Outgoing) == "Outgoing"
        assert str(EdgeDirection.Both) == "Both"


if __name__ == "__main__":
    suite = unittest.TestSuite()

    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestEdgeIndex))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestEdgeDirection))

    unittest.TextTestRunner(verbosity=2).run(suite)
