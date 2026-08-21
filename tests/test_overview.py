import unittest
from datetime import datetime

import pytest

import graphrecords as gr
from graphrecords.overview import DEFAULT_TRUNCATE_DETAILS


def create_group_schema() -> gr.GroupSchema:
    return gr.GroupSchema(
        nodes={
            "category": gr.AttributeDataType(gr.String(), gr.AttributeType.Categorical),
            "amount": gr.AttributeDataType(gr.Float(), gr.AttributeType.Continuous),
            "created_at": gr.AttributeDataType(
                gr.DateTime(), gr.AttributeType.Temporal
            ),
            "notes": gr.AttributeDataType(gr.String(), gr.AttributeType.Unstructured),
        },
        edges={
            "label": gr.AttributeDataType(gr.String(), gr.AttributeType.Categorical),
            "weight": gr.AttributeDataType(gr.Float(), gr.AttributeType.Continuous),
            "occurred_at": gr.AttributeDataType(
                gr.DateTime(), gr.AttributeType.Temporal
            ),
            "description": gr.AttributeDataType(
                gr.String(), gr.AttributeType.Unstructured
            ),
        },
    )


def create_graphrecord() -> gr.GraphRecord:
    group_schema = create_group_schema()

    schema = gr.Schema(
        groups={"cohort_a": group_schema},
        ungrouped=group_schema,
        schema_type=gr.SchemaType.Provided,
    )

    graphrecord = gr.GraphRecord.with_schema(schema)

    graphrecord = graphrecord.add_nodes(
        [
            (
                "n1",
                {
                    "category": "red",
                    "amount": 10.0,
                    "created_at": datetime(2024, 1, 1),
                    "notes": "first note",
                },
            ),
            (
                "n2",
                {
                    "category": "green",
                    "amount": 20.0,
                    "created_at": datetime(2024, 1, 2),
                    "notes": "second note",
                },
            ),
            (
                "n3",
                {
                    "category": "blue",
                    "amount": 30.0,
                    "created_at": datetime(2024, 1, 3),
                    "notes": "third note",
                },
            ),
        ]
    )
    graphrecord = graphrecord.add_edges(
        [
            (
                "n1",
                "n2",
                {
                    "label": "typeA",
                    "weight": 2.0,
                    "occurred_at": datetime(2024, 3, 1),
                    "description": "desc1",
                },
            ),
            (
                "n2",
                "n3",
                {
                    "label": "typeB",
                    "weight": 4.0,
                    "occurred_at": datetime(2024, 3, 2),
                    "description": "desc2",
                },
            ),
        ]
    )

    graphrecord = graphrecord.add_group("cohort_a")
    graphrecord = graphrecord.add_nodes_in_group(
        [
            (
                "n4",
                {
                    "category": "alpha",
                    "amount": 100.0,
                    "created_at": datetime(2025, 6, 1),
                    "notes": "grp note a",
                },
            ),
            (
                "n5",
                {
                    "category": "beta",
                    "amount": 200.0,
                    "created_at": datetime(2025, 6, 2),
                    "notes": "grp note b",
                },
            ),
        ],
        "cohort_a",
    )
    return graphrecord.add_edges_in_group(
        [
            (
                "n4",
                "n5",
                {
                    "label": "grpLabel",
                    "weight": 50.0,
                    "occurred_at": datetime(2025, 8, 1),
                    "description": "grp desc",
                },
            ),
        ],
        "cohort_a",
    )


class TestAttributeOverview(unittest.TestCase):
    def setUp(self) -> None:
        self.graphrecord = create_graphrecord()
        self.attributes = (
            self.graphrecord.overview().ungrouped_overview.node_overview.attributes
        )

    def test_data_categorical(self) -> None:
        attribute_overview = self.attributes["category"]

        assert attribute_overview.data_type == gr.String()
        assert attribute_overview.data == {
            "attribute_type": gr.AttributeType.Categorical,
            "distinct_values": ["blue", "green", "red"],
        }

    def test_data_continuous(self) -> None:
        attribute_overview = self.attributes["amount"]

        assert attribute_overview.data_type == gr.Float()
        assert attribute_overview.data == {
            "attribute_type": gr.AttributeType.Continuous,
            "min": 10.0,
            "mean": 20.0,
            "max": 30.0,
        }

    def test_data_temporal(self) -> None:
        attribute_overview = self.attributes["created_at"]

        assert attribute_overview.data_type == gr.DateTime()
        assert attribute_overview.data == {
            "attribute_type": gr.AttributeType.Temporal,
            "min": datetime(2024, 1, 1),
            "max": datetime(2024, 1, 3),
        }

    def test_data_unstructured(self) -> None:
        attribute_overview = self.attributes["notes"]

        assert attribute_overview.data_type == gr.String()
        assert attribute_overview.data == {
            "attribute_type": gr.AttributeType.Unstructured,
            "distinct_count": 3,
        }

    def test_repr(self) -> None:
        attribute_overview_repr = repr(self.attributes["category"])

        assert isinstance(attribute_overview_repr, str)
        assert "Categorical" in attribute_overview_repr
        assert attribute_overview_repr != repr(self.attributes["amount"])


class TestNodeGroupOverview(unittest.TestCase):
    def setUp(self) -> None:
        self.graphrecord = create_graphrecord()
        self.overview = self.graphrecord.overview()

    def test_count(self) -> None:
        assert self.overview.ungrouped_overview.node_overview.count == 3
        assert self.overview.grouped_overviews["cohort_a"].node_overview.count == 2

    def test_attributes(self) -> None:
        node_overview = self.overview.ungrouped_overview.node_overview

        assert node_overview.attributes.keys() == {
            "category",
            "amount",
            "created_at",
            "notes",
        }
        assert all(
            isinstance(attribute_overview, gr.AttributeOverview)
            for attribute_overview in node_overview.attributes.values()
        )

    def test_repr(self) -> None:
        node_overview_repr = repr(self.overview.ungrouped_overview.node_overview)

        assert isinstance(node_overview_repr, str)
        assert "Node Overview" in node_overview_repr


class TestEdgeGroupOverview(unittest.TestCase):
    def setUp(self) -> None:
        self.graphrecord = create_graphrecord()
        self.overview = self.graphrecord.overview()

    def test_count(self) -> None:
        assert self.overview.ungrouped_overview.edge_overview.count == 2
        assert self.overview.grouped_overviews["cohort_a"].edge_overview.count == 1

    def test_attributes(self) -> None:
        edge_overview = self.overview.grouped_overviews["cohort_a"].edge_overview

        assert edge_overview.attributes.keys() == {
            "label",
            "weight",
            "occurred_at",
            "description",
        }
        assert edge_overview.attributes["label"].data == {
            "attribute_type": gr.AttributeType.Categorical,
            "distinct_values": ["grpLabel"],
        }
        assert edge_overview.attributes["weight"].data == {
            "attribute_type": gr.AttributeType.Continuous,
            "min": 50.0,
            "mean": 50.0,
            "max": 50.0,
        }
        assert edge_overview.attributes["occurred_at"].data == {
            "attribute_type": gr.AttributeType.Temporal,
            "min": datetime(2025, 8, 1),
            "max": datetime(2025, 8, 1),
        }
        assert edge_overview.attributes["description"].data == {
            "attribute_type": gr.AttributeType.Unstructured,
            "distinct_count": 1,
        }

    def test_repr(self) -> None:
        edge_overview_repr = repr(self.overview.ungrouped_overview.edge_overview)

        assert isinstance(edge_overview_repr, str)
        assert "Edge Overview" in edge_overview_repr


class TestGroupOverview(unittest.TestCase):
    def setUp(self) -> None:
        self.graphrecord = create_graphrecord()
        self.overview = self.graphrecord.overview()

    def test_node_overview(self) -> None:
        group_overview = self.overview.grouped_overviews["cohort_a"]

        assert isinstance(group_overview.node_overview, gr.NodeGroupOverview)
        assert group_overview.node_overview.count == 2

    def test_edge_overview(self) -> None:
        group_overview = self.overview.grouped_overviews["cohort_a"]

        assert isinstance(group_overview.edge_overview, gr.EdgeGroupOverview)
        assert group_overview.edge_overview.count == 1

    def test_repr(self) -> None:
        group_overview_repr = repr(self.overview.ungrouped_overview)

        assert isinstance(group_overview_repr, str)
        assert "Node Overview" in group_overview_repr
        assert "Edge Overview" in group_overview_repr


class TestOverview(unittest.TestCase):
    def setUp(self) -> None:
        self.graphrecord = create_graphrecord()
        self.overview = self.graphrecord.overview()

    def test_ungrouped_overview(self) -> None:
        ungrouped_overview = self.overview.ungrouped_overview

        assert isinstance(ungrouped_overview, gr.GroupOverview)
        assert ungrouped_overview.node_overview.count == 3
        assert ungrouped_overview.edge_overview.count == 2

    def test_grouped_overviews(self) -> None:
        grouped_overviews = self.overview.grouped_overviews

        assert grouped_overviews.keys() == {"cohort_a"}
        assert isinstance(grouped_overviews["cohort_a"], gr.GroupOverview)

    def test_repr(self) -> None:
        overview_repr = repr(self.overview)

        assert isinstance(overview_repr, str)
        assert "Node Overview" in overview_repr
        assert "Edge Overview" in overview_repr
        assert "Ungrouped" in overview_repr
        assert "cohort_a" in overview_repr

    def test_truncate_details(self) -> None:
        group_schema = gr.GroupSchema(
            nodes={
                "category": gr.AttributeDataType(
                    gr.String(), gr.AttributeType.Categorical
                ),
            },
        )
        schema = gr.Schema(ungrouped=group_schema, schema_type=gr.SchemaType.Provided)
        graphrecord = gr.GraphRecord.with_schema(schema)
        graphrecord = graphrecord.add_nodes(
            [
                ("n1", {"category": "alpha-value-one"}),
                ("n2", {"category": "beta-value-two"}),
            ]
        )

        default_repr = repr(graphrecord.overview())
        explicit_default_repr = repr(graphrecord.overview(DEFAULT_TRUNCATE_DETAILS))
        none_repr = repr(graphrecord.overview(None))
        truncated_repr = repr(graphrecord.overview(10))

        assert default_repr == explicit_default_repr
        assert default_repr == none_repr
        assert truncated_repr != default_repr
        assert "Distinct v" in truncated_repr
        assert "Distinct values:" not in truncated_repr


class TestGraphRecordGroupOverview(unittest.TestCase):
    def setUp(self) -> None:
        self.graphrecord = create_graphrecord()

    def test_group_overview(self) -> None:
        group_overview = self.graphrecord.group_overview("cohort_a")
        expected_group_overview = self.graphrecord.overview().grouped_overviews[
            "cohort_a"
        ]

        assert (
            group_overview.node_overview.count
            == expected_group_overview.node_overview.count
        )
        assert (
            group_overview.edge_overview.count
            == expected_group_overview.edge_overview.count
        )
        assert group_overview.node_overview.attributes.keys() == {
            "category",
            "amount",
            "created_at",
            "notes",
        }

    def test_invalid_group_overview(self) -> None:
        with pytest.raises(IndexError, match="Cannot find group with index"):
            self.graphrecord.group_overview("missing_group")


class TestDefaultTruncateDetails(unittest.TestCase):
    def test_value(self) -> None:
        assert DEFAULT_TRUNCATE_DETAILS == 80
        assert gr.DEFAULT_TRUNCATE_DETAILS is DEFAULT_TRUNCATE_DETAILS
