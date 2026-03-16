import unittest

import pytest

import graphrecords as gr


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


if __name__ == "__main__":
    run_test = unittest.TestLoader().loadTestsFromTestCase(TestGraphRecordBuilder)
    unittest.TextTestRunner(verbosity=2).run(run_test)
