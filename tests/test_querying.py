import unittest
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Set, Tuple

import polars as pl
import pytest

from graphrecords import GraphRecord
from graphrecords.querying import (
    AttributeName,
    AttributeNameIndex,
    Bare,
    CastTarget,
    Definite,
    Drop,
    DuplicateIndexError,
    EdgeDirection,
    EdgeEndpointRole,
    EdgeIndex,
    EdgesOperand,
    EndpointRole,
    Expanded,
    FailureKindValue,
    FailureValue,
    Grouped,
    Indexed,
    IndexPayload,
    IndexValue,
    Mask,
    MissingAttributeError,
    Multiple,
    NodeIndex,
    NodesOperand,
    Operand,
    Ordered,
    Positional,
    QueryError,
    Raise,
    Replace,
    Scalar,
    ScalarValue,
    Single,
    Ungrouped,
    Unit,
    Unordered,
    ValueIndex,
    ValueTarget,
)


def simple_example_dataset() -> GraphRecord:
    diagnosis_nodes = pl.read_csv(
        Path(__file__).parent / "simple_dataset/diagnosis.csv",
        schema={"diagnosis_code": pl.String, "description": pl.String},
    )
    drug_nodes = pl.read_csv(
        Path(__file__).parent / "simple_dataset/drug.csv",
        schema={"drug_code": pl.String, "description": pl.String},
    )
    patient_nodes = pl.read_csv(
        Path(__file__).parent / "simple_dataset/patient.csv",
        schema={"patient_id": pl.String, "gender": pl.String, "age": pl.Int64},
    )
    procedure_nodes = pl.read_csv(
        Path(__file__).parent / "simple_dataset/procedure.csv",
        schema={"procedure_code": pl.String, "description": pl.String},
    )
    patient_diagnosis_edges = pl.read_csv(
        Path(__file__).parent / "simple_dataset/patient_diagnosis.csv",
        schema={
            "patient_id": pl.String,
            "diagnosis_code": pl.String,
            "time": pl.Datetime,
            "duration_days": pl.Float64,
        },
    )
    patient_drug_edges = pl.read_csv(
        Path(__file__).parent / "simple_dataset/patient_drug.csv",
        schema={
            "patient_id": pl.String,
            "drug_code": pl.String,
            "time": pl.Datetime,
            "quantity": pl.Int64,
            "cost": pl.Float64,
        },
    )
    patient_procedure_edges = pl.read_csv(
        Path(__file__).parent / "simple_dataset/patient_procedure.csv",
        schema={
            "patient_id": pl.String,
            "procedure_code": pl.String,
            "time": pl.Datetime,
            "duration_minutes": pl.Float64,
        },
    )

    graphrecord = GraphRecord()
    graphrecord.add_nodes((diagnosis_nodes, "diagnosis_code"), "diagnosis")
    graphrecord.add_nodes((drug_nodes, "drug_code"), "drug")
    graphrecord.add_nodes((patient_nodes, "patient_id"), "patient")
    graphrecord.add_nodes((procedure_nodes, "procedure_code"), "procedure")
    graphrecord.add_edges(
        (patient_diagnosis_edges, "patient_id", "diagnosis_code"),
        "patient_diagnosis",
    )
    graphrecord.add_edges(
        (patient_drug_edges, "patient_id", "drug_code"), "patient_drug"
    )
    graphrecord.add_edges(
        (patient_procedure_edges, "patient_id", "procedure_code"),
        "patient_procedure",
    )

    return graphrecord


class TestNodeOperand(unittest.TestCase):
    def setUp(self) -> None:
        self.graphrecord = simple_example_dataset()

    def test_node_operand_attribute(self) -> None:
        def query(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.filter(nodes.in_group("patient")).attribute("age")

        assert sorted(self.graphrecord.query_nodes(query)) == [
            ("pat_1", 42),
            ("pat_2", 22),
            ("pat_3", 96),
            ("pat_4", 19),
            ("pat_5", 37),
        ]

    def test_node_operand_has_attribute(self) -> None:
        def query(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Mask], Multiple[Unordered], Ungrouped]:
            return nodes.has_attribute("age")

        result = dict(self.graphrecord.query_nodes(query))

        assert result["pat_1"] is True
        assert result["drug_856987"] is False

    def test_node_operand_neighbors(self) -> None:
        def query(nodes: NodesOperand) -> NodesOperand:
            return nodes.filter(nodes.index().equal_to("pat_1")).neighbors(
                EdgeDirection.Outgoing
            )

        result = self.graphrecord.query_nodes(query)

        assert "diagnosis_314529007" in result
        assert "drug_856987" in result
        assert "procedure_171207006" in result

    def test_node_operand_cache(self) -> None:
        def query(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Mask], Multiple[Unordered], Ungrouped]:
            ages = nodes.filter(nodes.in_group("patient")).attribute("age").cache()

            return ages.equal_to(ages)

        assert sorted(self.graphrecord.query_nodes(query)) == [
            ("pat_1", True),
            ("pat_2", True),
            ("pat_3", True),
            ("pat_4", True),
            ("pat_5", True),
        ]

    def test_node_operand_attributes_index_sum(self) -> None:
        def query(
            nodes: NodesOperand,
        ) -> Operand[Bare[IndexValue[AttributeNameIndex]], Single, Ungrouped]:
            return (
                nodes.filter(nodes.index().equal_to("pat_1"))
                .attributes()
                .discard_value()
                .index()
                .child_index()
                .sum()
            )

        assert self.graphrecord.query_nodes(query) in {"agegender", "genderage"}


class TestNodeGroupOperand(unittest.TestCase):
    def setUp(self) -> None:
        self.graphrecord = GraphRecord.from_tuples(
            [
                ("pat_1", {"gender": "M", "age": 42}),
                ("pat_2", {"gender": "F", "age": 22}),
                ("pat_3", {"gender": "F", "age": 96}),
                ("pat_4", {"gender": "M", "age": 19}),
                ("pat_5", {"gender": "M", "age": 37}),
            ]
        )

    def test_group_operand_attribute(self) -> None:
        def query(
            nodes: NodesOperand,
        ) -> Operand[
            Bare[Scalar],
            Single,
            Grouped[NodeIndex, ValueIndex, Ungrouped],
        ]:
            return nodes.group_by(nodes.attribute("gender")).attribute("age").mean()

        buckets, key_failures = self.graphrecord.query_nodes(query)
        result = {key: (set(members), value) for key, members, value in buckets}

        assert key_failures == []
        assert result["F"][0] == {"pat_2", "pat_3"}
        assert result["F"][1] == 59
        assert result["M"][0] == {"pat_1", "pat_4", "pat_5"}
        assert result["M"][1] == pytest.approx(32.666666666666664)

    def test_group_operand_cache(self) -> None:
        def query(
            nodes: NodesOperand,
        ) -> Operand[
            Bare[Scalar],
            Single,
            Grouped[NodeIndex, ValueIndex, Ungrouped],
        ]:
            return (
                nodes.group_by(nodes.attribute("gender"))
                .attribute("age")
                .mean()
                .cache()
            )

        buckets, key_failures = self.graphrecord.query_nodes(query)
        result = {key: value for key, _, value in buckets}

        assert key_failures == []
        assert result["F"] == 59
        assert result["M"] == pytest.approx(32.666666666666664)

    def test_group_operand_nested_lifting(self) -> None:
        def element_in_nested_group(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return (
                nodes.group_by(nodes.attribute("gender"))
                .group_by(nodes.attribute("age"))
                .attribute("age")
                .add(1)
                .ungroup()
                .ungroup()
            )

        def lane_in_nested_group(
            nodes: NodesOperand,
        ) -> Operand[Indexed[ValueIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return (
                nodes.group_by(nodes.attribute("gender"))
                .group_by(nodes.attribute("age"))
                .attribute("age")
                .sum()
                .ungroup_keyed()
                .ungroup()
            )

        def inner_keys(
            nodes: NodesOperand,
        ) -> Operand[Indexed[ValueIndex, Unit], Multiple[Unordered], Ungrouped]:
            return (
                nodes.group_by(nodes.attribute("gender"))
                .group_by(nodes.attribute("age"))
                .keys()
                .ungroup()
            )

        assert dict(self.graphrecord.query_nodes(element_in_nested_group)) == {
            "pat_1": 43,
            "pat_2": 23,
            "pat_3": 97,
            "pat_4": 20,
            "pat_5": 38,
        }
        assert dict(self.graphrecord.query_nodes(lane_in_nested_group)) == {
            19: 19,
            22: 22,
            37: 37,
            42: 42,
            96: 96,
        }
        assert set(self.graphrecord.query_nodes(inner_keys)) == {19, 22, 37, 42, 96}

    def test_group_operand_argument(self) -> None:
        graphrecord = GraphRecord.from_tuples(
            [
                ("n1", {"name": "Alpha", "width": 8, "kind": True}),
                ("n2", {"name": "Beta", "width": 7, "kind": False}),
            ]
        )

        def query(
            nodes: NodesOperand,
        ) -> Operand[
            Indexed[NodeIndex, Scalar],
            Multiple[Unordered],
            Grouped[NodeIndex, ValueIndex, Ungrouped],
        ]:
            return (
                nodes.group_by(nodes.attribute("kind"))
                .attribute("name")
                .pad_start(nodes.attribute("width"), ".")
            )

        buckets, key_failures = graphrecord.query_nodes(query)
        result = {key: payload for key, _, payload in buckets}

        assert key_failures == []
        assert result == {False: [("n2", "...Beta")], True: [("n1", "...Alpha")]}


class TestStringOperand(unittest.TestCase):
    def setUp(self) -> None:
        self.graphrecord = GraphRecord.from_tuples(
            [("n1", {"name": "Alpha"}), ("n2", {"name": "Beta"})]
        )

    def test_split(self) -> None:
        def query(
            nodes: NodesOperand,
        ) -> Operand[
            Indexed[Expanded[NodeIndex, Positional], Scalar],
            Multiple[Ordered],
            Ungrouped,
        ]:
            return nodes.attribute("name").split("a")

        assert sorted(self.graphrecord.query_nodes(query)) == [
            (("n1", 0), "Alph"),
            (("n1", 1), ""),
            (("n2", 0), "Bet"),
            (("n2", 1), ""),
        ]


class TestScalarOperand(unittest.TestCase):
    def setUp(self) -> None:
        self.graphrecord = GraphRecord.from_tuples(
            [
                (
                    "n1",
                    {
                        "number": -1.5,
                        "integer": 1,
                        "text": " Alpha ",
                        "flag": True,
                        "moment": datetime(2024, 1, 1),
                        "duration": timedelta(hours=1),
                        "nothing": None,
                    },
                ),
                (
                    "n2",
                    {
                        "number": 2.25,
                        "integer": 2,
                        "text": "Beta",
                        "flag": False,
                        "moment": datetime(2024, 1, 2),
                        "duration": timedelta(hours=2),
                        "nothing": None,
                    },
                ),
                (
                    "n3",
                    {
                        "number": 2.25,
                        "integer": 3,
                        "text": "Gamma",
                        "flag": True,
                        "moment": datetime(2024, 1, 3),
                        "duration": timedelta(hours=3),
                        "nothing": None,
                    },
                ),
            ]
        )

    def test_string_operations(self) -> None:
        def trim(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("text").trim()

        def trim_start(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("text").trim_start()

        def trim_end(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("text").trim_end()

        def lowercase(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("text").lowercase()

        def uppercase(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("text").uppercase()

        def reverse(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("text").reverse()

        def length(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("text").length()

        def slice_(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("text").slice(1, 4)

        def replace(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("text").replace("a", "_")

        def replace_all(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("text").replace_all("a", "_")

        def pad_start(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("text").trim().pad_start(7, ".")

        def pad_end(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("text").trim().pad_end(7, ".")

        assert dict(self.graphrecord.query_nodes(trim))["n1"] == "Alpha"
        assert dict(self.graphrecord.query_nodes(trim_start))["n1"] == "Alpha "
        assert dict(self.graphrecord.query_nodes(trim_end))["n1"] == " Alpha"
        assert dict(self.graphrecord.query_nodes(lowercase))["n1"] == " alpha "
        assert dict(self.graphrecord.query_nodes(uppercase))["n1"] == " ALPHA "
        assert dict(self.graphrecord.query_nodes(reverse))["n2"] == "ateB"
        assert dict(self.graphrecord.query_nodes(length))["n2"] == 4
        assert dict(self.graphrecord.query_nodes(slice_))["n2"] == "eta"
        assert dict(self.graphrecord.query_nodes(replace))["n3"] == "G_mma"
        assert dict(self.graphrecord.query_nodes(replace_all))["n3"] == "G_mm_"
        assert dict(self.graphrecord.query_nodes(pad_start))["n2"] == "...Beta"
        assert dict(self.graphrecord.query_nodes(pad_end))["n2"] == "Beta..."

    def test_string_predicates(self) -> None:
        def starts_with(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Mask], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("text").starts_with("B")

        def ends_with(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Mask], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("text").ends_with("a")

        def contains(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Mask], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("text").contains("mm")

        def matches(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Mask], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("text").matches("^B.*")

        def strip_prefix(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("text").strip_prefix("B")

        def strip_suffix(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("text").strip_suffix("a")

        assert dict(self.graphrecord.query_nodes(starts_with)) == {
            "n1": False,
            "n2": True,
            "n3": False,
        }
        assert dict(self.graphrecord.query_nodes(ends_with)) == {
            "n1": False,
            "n2": True,
            "n3": True,
        }
        assert dict(self.graphrecord.query_nodes(contains))["n3"] is True
        assert dict(self.graphrecord.query_nodes(matches))["n2"] is True
        assert dict(self.graphrecord.query_nodes(strip_prefix))["n2"] == "eta"
        assert dict(self.graphrecord.query_nodes(strip_suffix))["n3"] == "Gamm"

    def test_arithmetic_operations(self) -> None:
        def add(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("number").add(2)

        def subtract(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("number").subtract(1)

        def multiply(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("number").multiply(2)

        def divide(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("number").divide(2)

        def power(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("number").power(2)

        def modulo(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("number").modulo(2)

        assert dict(self.graphrecord.query_nodes(add))["n1"] == 0.5
        assert dict(self.graphrecord.query_nodes(subtract))["n2"] == 1.25
        assert dict(self.graphrecord.query_nodes(multiply))["n2"] == 4.5
        assert dict(self.graphrecord.query_nodes(divide))["n2"] == 1.125
        assert dict(self.graphrecord.query_nodes(power))["n1"] == 2.25
        assert dict(self.graphrecord.query_nodes(modulo))["n2"] == 0.25

    def test_comparison_operations(self) -> None:
        def equal_to(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Mask], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("number").equal_to(2.25)

        def not_equal_to(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Mask], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("number").not_equal_to(2.25)

        def greater_than(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Mask], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("number").greater_than(0)

        def greater_than_or_equal_to(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Mask], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("number").greater_than_or_equal_to(2.25)

        def less_than(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Mask], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("number").less_than(0)

        def less_than_or_equal_to(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Mask], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("number").less_than_or_equal_to(-1.5)

        assert dict(self.graphrecord.query_nodes(equal_to)) == {
            "n1": False,
            "n2": True,
            "n3": True,
        }
        assert dict(self.graphrecord.query_nodes(not_equal_to))["n1"] is True
        assert dict(self.graphrecord.query_nodes(greater_than))["n2"] is True
        assert (
            dict(self.graphrecord.query_nodes(greater_than_or_equal_to))["n3"] is True
        )
        assert dict(self.graphrecord.query_nodes(less_than))["n1"] is True
        assert dict(self.graphrecord.query_nodes(less_than_or_equal_to))["n1"] is True

    def test_numeric_operations(self) -> None:
        def absolute(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("number").abs()

        def negate(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("number").neg()

        def sign(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("number").sign()

        def ceil(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("number").ceil()

        def cube_root(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("number").cbrt()

        def exponential(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("number").exp()

        def floor(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("number").floor()

        def logarithm(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("number").abs().log()

        def round_(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("number").round()

        def square_root(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("number").abs().sqrt()

        assert dict(self.graphrecord.query_nodes(absolute))["n1"] == 1.5
        assert dict(self.graphrecord.query_nodes(negate))["n2"] == -2.25
        assert dict(self.graphrecord.query_nodes(sign))["n1"] == -1
        assert dict(self.graphrecord.query_nodes(ceil))["n2"] == 3
        assert dict(self.graphrecord.query_nodes(cube_root))["n2"] == pytest.approx(
            1.3103706971
        )
        assert dict(self.graphrecord.query_nodes(exponential))["n1"] == pytest.approx(
            0.2231301601
        )
        assert dict(self.graphrecord.query_nodes(floor))["n2"] == 2
        assert dict(self.graphrecord.query_nodes(logarithm))["n1"] == pytest.approx(
            0.4054651081
        )
        assert dict(self.graphrecord.query_nodes(round_))["n2"] == 2
        assert dict(self.graphrecord.query_nodes(square_root))["n1"] == pytest.approx(
            1.2247448714
        )

    def test_type_inspection(self) -> None:
        def is_bool(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Mask], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("flag").is_bool()

        def is_datetime(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Mask], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("moment").is_datetime()

        def is_duration(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Mask], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("duration").is_duration()

        def is_float(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Mask], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("number").is_float()

        def is_null(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Mask], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("nothing").is_null()

        def is_int(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Mask], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("integer").is_int()

        def is_string(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Mask], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("text").is_string()

        assert dict(self.graphrecord.query_nodes(is_bool)) == {
            "n1": True,
            "n2": True,
            "n3": True,
        }
        assert dict(self.graphrecord.query_nodes(is_datetime)) == {
            "n1": True,
            "n2": True,
            "n3": True,
        }
        assert dict(self.graphrecord.query_nodes(is_duration)) == {
            "n1": True,
            "n2": True,
            "n3": True,
        }
        assert dict(self.graphrecord.query_nodes(is_float)) == {
            "n1": True,
            "n2": True,
            "n3": True,
        }
        assert dict(self.graphrecord.query_nodes(is_null)) == {
            "n1": True,
            "n2": True,
            "n3": True,
        }
        assert dict(self.graphrecord.query_nodes(is_int)) == {
            "n1": True,
            "n2": True,
            "n3": True,
        }
        assert dict(self.graphrecord.query_nodes(is_string)) == {
            "n1": True,
            "n2": True,
            "n3": True,
        }

    def test_aggregations(self) -> None:
        def maximum(
            nodes: NodesOperand,
        ) -> Operand[Bare[Scalar], Single, Ungrouped]:
            return nodes.attribute("number").max()

        def minimum(
            nodes: NodesOperand,
        ) -> Operand[Bare[Scalar], Single, Ungrouped]:
            return nodes.attribute("number").min()

        def median(
            nodes: NodesOperand,
        ) -> Operand[Bare[Scalar], Single, Ungrouped]:
            return nodes.attribute("number").median()

        def product(
            nodes: NodesOperand,
        ) -> Operand[Bare[Scalar], Single, Ungrouped]:
            return nodes.attribute("integer").product()

        def mode(
            nodes: NodesOperand,
        ) -> Operand[Bare[Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("number").mode()

        def unique_count(
            nodes: NodesOperand,
        ) -> Operand[Bare[Scalar], Definite, Ungrouped]:
            return nodes.attribute("number").n_unique()

        def random(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Single, Ungrouped]:
            return nodes.attribute("number").random()

        def count(
            nodes: NodesOperand,
        ) -> Operand[Bare[Scalar], Definite, Ungrouped]:
            return nodes.attribute("number").count()

        def sum_(
            nodes: NodesOperand,
        ) -> Operand[Bare[Scalar], Single, Ungrouped]:
            return nodes.attribute("number").sum()

        def mean(
            nodes: NodesOperand,
        ) -> Operand[Bare[Scalar], Single, Ungrouped]:
            return nodes.attribute("number").mean()

        def standard_deviation(
            nodes: NodesOperand,
        ) -> Operand[Bare[Scalar], Single, Ungrouped]:
            return nodes.attribute("number").std()

        def variance(
            nodes: NodesOperand,
        ) -> Operand[Bare[Scalar], Single, Ungrouped]:
            return nodes.attribute("number").var()

        assert self.graphrecord.query_nodes(maximum) == 2.25
        assert self.graphrecord.query_nodes(minimum) == -1.5
        assert self.graphrecord.query_nodes(median) == 2.25
        assert self.graphrecord.query_nodes(product) == 6
        assert self.graphrecord.query_nodes(mode) == [2.25]
        assert self.graphrecord.query_nodes(unique_count) == 2
        assert self.graphrecord.query_nodes(random) in {
            ("n1", -1.5),
            ("n2", 2.25),
            ("n3", 2.25),
        }
        assert self.graphrecord.query_nodes(count) == 3
        assert self.graphrecord.query_nodes(sum_) == 3
        assert self.graphrecord.query_nodes(mean) == 1
        assert self.graphrecord.query_nodes(standard_deviation) == pytest.approx(
            2.1650635095
        )
        assert self.graphrecord.query_nodes(variance) == 4.6875

    def test_ordering_and_uniqueness(self) -> None:
        def first(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Single, Ungrouped]:
            return nodes.attribute("integer").sort().first()

        def last(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Single, Ungrouped]:
            return nodes.attribute("integer").sort().last()

        def reverse_order(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Ordered], Ungrouped]:
            return nodes.attribute("integer").sort().reverse_order()

        def take(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Ordered], Ungrouped]:
            return nodes.attribute("integer").sort().take(2)

        def shuffle(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Ordered], Ungrouped]:
            return nodes.attribute("integer").shuffle()

        def unorder(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("integer").sort().unorder()

        def sort(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Ordered], Ungrouped]:
            return nodes.attribute("number").sort()

        def sort_by(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Ordered], Ungrouped]:
            return nodes.attribute("text").sort_by(nodes.attribute("integer").neg())

        def drop_duplicates(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Ordered], Ungrouped]:
            return nodes.attribute("number").sort().drop_duplicates()

        def is_duplicated(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Mask], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("number").is_duplicated()

        def unique(
            nodes: NodesOperand,
        ) -> Operand[Bare[Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("number").discard_index().unique()

        assert self.graphrecord.query_nodes(first) == ("n1", 1)
        assert self.graphrecord.query_nodes(last) == ("n3", 3)
        assert self.graphrecord.query_nodes(reverse_order) == [
            ("n3", 3),
            ("n2", 2),
            ("n1", 1),
        ]
        assert self.graphrecord.query_nodes(take) == [("n1", 1), ("n2", 2)]
        assert sorted(self.graphrecord.query_nodes(shuffle)) == [
            ("n1", 1),
            ("n2", 2),
            ("n3", 3),
        ]
        assert sorted(self.graphrecord.query_nodes(unorder)) == [
            ("n1", 1),
            ("n2", 2),
            ("n3", 3),
        ]
        assert self.graphrecord.query_nodes(sort) == [
            ("n1", -1.5),
            ("n2", 2.25),
            ("n3", 2.25),
        ]
        assert self.graphrecord.query_nodes(sort_by) == [
            ("n3", "Gamma"),
            ("n2", "Beta"),
            ("n1", " Alpha "),
        ]
        assert self.graphrecord.query_nodes(drop_duplicates) == [
            ("n1", -1.5),
            ("n2", 2.25),
        ]
        assert dict(self.graphrecord.query_nodes(is_duplicated)) == {
            "n1": False,
            "n2": True,
            "n3": True,
        }
        assert set(self.graphrecord.query_nodes(unique)) == {-1.5, 2.25}

    def test_membership_cast_and_clip(self) -> None:
        def is_in(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Mask], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("integer").is_in([1, 3])

        def clip(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("number").clip(0, 2)

        def is_in_operand(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Mask], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("integer").is_in(nodes.attribute("integer"))

        def cast(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("integer").cast(CastTarget.String)

        assert dict(self.graphrecord.query_nodes(is_in)) == {
            "n1": True,
            "n2": False,
            "n3": True,
        }
        assert dict(self.graphrecord.query_nodes(is_in_operand)) == {
            "n1": True,
            "n2": True,
            "n3": True,
        }
        assert dict(self.graphrecord.query_nodes(clip)) == {
            "n1": 0.0,
            "n2": 2.0,
            "n3": 2.0,
        }
        assert dict(self.graphrecord.query_nodes(cast)) == {
            "n1": "1",
            "n2": "2",
            "n3": "3",
        }

    def test_logic_and_filter(self) -> None:
        def and_(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Mask], Multiple[Unordered], Ungrouped]:
            return (
                nodes.attribute("number")
                .greater_than(0)
                .and_(nodes.attribute("integer").less_than(3))
            )

        def or_(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Mask], Multiple[Unordered], Ungrouped]:
            return (
                nodes.attribute("number")
                .less_than(0)
                .or_(nodes.attribute("integer").equal_to(3))
            )

        def xor(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Mask], Multiple[Unordered], Ungrouped]:
            return (
                nodes.attribute("flag")
                .transition(ValueTarget.Mask)
                .xor(nodes.attribute("integer").equal_to(3))
            )

        def not_(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Mask], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("flag").transition(ValueTarget.Mask).not_()

        def filter_(nodes: NodesOperand) -> NodesOperand:
            return nodes.filter(nodes.attribute("number").greater_than(0))

        def all_(
            nodes: NodesOperand,
        ) -> Operand[Bare[Mask], Definite, Ungrouped]:
            return nodes.attribute("integer").greater_than(0).all()

        def any_(
            nodes: NodesOperand,
        ) -> Operand[Bare[Mask], Definite, Ungrouped]:
            return nodes.attribute("number").less_than(0).any()

        assert dict(self.graphrecord.query_nodes(and_)) == {
            "n1": False,
            "n2": True,
            "n3": False,
        }
        assert dict(self.graphrecord.query_nodes(or_)) == {
            "n1": True,
            "n2": False,
            "n3": True,
        }
        assert dict(self.graphrecord.query_nodes(xor)) == {
            "n1": True,
            "n2": False,
            "n3": False,
        }
        assert dict(self.graphrecord.query_nodes(not_)) == {
            "n1": False,
            "n2": True,
            "n3": False,
        }
        assert set(self.graphrecord.query_nodes(filter_)) == {"n2", "n3"}
        assert self.graphrecord.query_nodes(all_) is True
        assert self.graphrecord.query_nodes(any_) is True

    def test_operator_forms(self) -> None:
        def add(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("number") + 2

        def negate(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return -nodes.attribute("number")

        def greater_than(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Mask], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("number") > 0

        def equal_to(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Mask], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("number") == 2.25

        def not_equal_to(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Mask], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("number") != 2.25

        def conjunction(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Mask], Multiple[Unordered], Ungrouped]:
            return (nodes.attribute("number") > 0) & (nodes.attribute("integer") < 3)

        assert dict(self.graphrecord.query_nodes(add))["n1"] == 0.5
        assert dict(self.graphrecord.query_nodes(negate))["n2"] == -2.25
        assert dict(self.graphrecord.query_nodes(greater_than))["n2"] is True
        assert dict(self.graphrecord.query_nodes(equal_to)) == {
            "n1": False,
            "n2": True,
            "n3": True,
        }
        assert dict(self.graphrecord.query_nodes(not_equal_to))["n1"] is True
        assert dict(self.graphrecord.query_nodes(conjunction)) == {
            "n1": False,
            "n2": True,
            "n3": False,
        }

    def test_missing_argument_policies(self) -> None:
        def dropping(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            width = nodes.filter(nodes.index().equal_to("n1")).attribute("integer")
            return (
                nodes.attribute("text").trim().pad_start(width.on_missing(Drop()), ".")
            )

        def replacing(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            width = nodes.filter(nodes.index().equal_to("n1")).attribute("integer")
            return (
                nodes.attribute("text")
                .trim()
                .pad_start(width.on_missing(Replace(6)), ".")
            )

        assert self.graphrecord.query_nodes(dropping) == [("n1", "Alpha")]
        assert dict(self.graphrecord.query_nodes(replacing)) == {
            "n1": "Alpha",
            "n2": "..Beta",
            "n3": ".Gamma",
        }

    def test_index_and_conversion_operations(self) -> None:
        def index(
            nodes: NodesOperand,
        ) -> Operand[
            Indexed[NodeIndex, IndexValue[NodeIndex]],
            Multiple[Unordered],
            Ungrouped,
        ]:
            return nodes.index()

        def discard_value(nodes: NodesOperand) -> NodesOperand:
            return nodes.attribute("number").discard_value()

        def resolve_and_select(nodes: NodesOperand) -> NodesOperand:
            return nodes.index().resolve().select()

        def enumerate_(
            nodes: NodesOperand,
        ) -> Operand[Indexed[Positional, Scalar], Multiple[Ordered], Ungrouped]:
            return nodes.attribute("integer").sort().discard_index().enumerate()

        def transition(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.index().transition(ValueTarget.Value)

        def parent_index(
            nodes: NodesOperand,
        ) -> Operand[
            Indexed[Expanded[NodeIndex, Positional], IndexValue[NodeIndex]],
            Multiple[Ordered],
            Ungrouped,
        ]:
            return (
                nodes.attribute("text")
                .split("a")
                .discard_value()
                .index()
                .parent_index()
            )

        def child_index(
            nodes: NodesOperand,
        ) -> Operand[
            Indexed[Expanded[NodeIndex, Positional], IndexValue[Positional]],
            Multiple[Ordered],
            Ungrouped,
        ]:
            return (
                nodes.attribute("text").split("a").discard_value().index().child_index()
            )

        def expand_to(
            nodes: NodesOperand,
        ) -> Operand[
            Indexed[Expanded[NodeIndex, Positional], Scalar],
            Multiple[Ordered],
            Ungrouped,
        ]:
            return (
                nodes.attribute("text").split("a").expand_to(nodes.attribute("integer"))
            )

        assert dict(self.graphrecord.query_nodes(index)) == {
            "n1": "n1",
            "n2": "n2",
            "n3": "n3",
        }
        assert set(self.graphrecord.query_nodes(discard_value)) == {"n1", "n2", "n3"}
        assert set(self.graphrecord.query_nodes(resolve_and_select)) == {
            "n1",
            "n2",
            "n3",
        }
        assert self.graphrecord.query_nodes(enumerate_) == [(0, 1), (1, 2), (2, 3)]
        assert dict(self.graphrecord.query_nodes(transition)) == {
            "n1": "n1",
            "n2": "n2",
            "n3": "n3",
        }
        assert sorted(self.graphrecord.query_nodes(parent_index)) == [
            (("n1", 0), "n1"),
            (("n1", 1), "n1"),
            (("n2", 0), "n2"),
            (("n2", 1), "n2"),
            (("n3", 0), "n3"),
            (("n3", 1), "n3"),
            (("n3", 2), "n3"),
        ]
        assert sorted(self.graphrecord.query_nodes(child_index)) == [
            (("n1", 0), 0),
            (("n1", 1), 1),
            (("n2", 0), 0),
            (("n2", 1), 1),
            (("n3", 0), 0),
            (("n3", 1), 1),
            (("n3", 2), 2),
        ]
        assert sorted(self.graphrecord.query_nodes(expand_to)) == [
            (("n1", 0), 1),
            (("n1", 1), 1),
            (("n2", 0), 2),
            (("n2", 1), 2),
            (("n3", 0), 3),
            (("n3", 1), 3),
            (("n3", 2), 3),
        ]


class TestConversionOperand(unittest.TestCase):
    def setUp(self) -> None:
        self.graphrecord = GraphRecord.from_tuples(
            [
                (
                    "n1",
                    {
                        "number": -1.5,
                        "integer": 1,
                        "text": "Alpha",
                        "flag": True,
                        "moment": datetime(2024, 1, 1),
                        "duration": timedelta(hours=1),
                        "nothing": None,
                    },
                ),
                (
                    "n2",
                    {
                        "number": 2.25,
                        "integer": 2,
                        "text": "Beta",
                        "flag": False,
                        "moment": datetime(2024, 1, 2),
                        "duration": timedelta(hours=2),
                        "nothing": None,
                    },
                ),
                (
                    "n3",
                    {
                        "number": 2.25,
                        "integer": 3,
                        "text": "Gamma",
                        "flag": True,
                        "moment": datetime(2024, 1, 3),
                        "duration": timedelta(hours=3),
                        "nothing": None,
                    },
                ),
            ]
        )

    def test_cast_scalar_targets(self) -> None:
        def to_bool(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("integer").cast(CastTarget.Bool)

        def to_bool_from_bool(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("flag").cast(CastTarget.Bool)

        def to_int(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("number").cast(CastTarget.Int)

        def to_int_from_bool(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("flag").cast(CastTarget.Int)

        def to_float(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("integer").cast(CastTarget.Float)

        def to_string(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("flag").cast(CastTarget.String)

        def to_duration(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("integer").cast(CastTarget.Duration)

        def to_datetime(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("integer").cast(CastTarget.DateTime)

        assert dict(self.graphrecord.query_nodes(to_bool)) == {
            "n1": True,
            "n2": True,
            "n3": True,
        }
        assert dict(self.graphrecord.query_nodes(to_bool_from_bool)) == {
            "n1": True,
            "n2": False,
            "n3": True,
        }
        assert dict(self.graphrecord.query_nodes(to_int)) == {
            "n1": -1,
            "n2": 2,
            "n3": 2,
        }
        assert dict(self.graphrecord.query_nodes(to_int_from_bool)) == {
            "n1": 1,
            "n2": 0,
            "n3": 1,
        }
        assert dict(self.graphrecord.query_nodes(to_float)) == {
            "n1": 1.0,
            "n2": 2.0,
            "n3": 3.0,
        }
        assert dict(self.graphrecord.query_nodes(to_string)) == {
            "n1": "true",
            "n2": "false",
            "n3": "true",
        }
        assert dict(self.graphrecord.query_nodes(to_duration)) == {
            "n1": timedelta(milliseconds=1),
            "n2": timedelta(milliseconds=2),
            "n3": timedelta(milliseconds=3),
        }
        assert dict(self.graphrecord.query_nodes(to_datetime)) == {
            "n1": datetime(1970, 1, 1) + timedelta(milliseconds=1),
            "n2": datetime(1970, 1, 1) + timedelta(milliseconds=2),
            "n3": datetime(1970, 1, 1) + timedelta(milliseconds=3),
        }

    def test_cast_bare_receiver(self) -> None:
        def bare_cast(
            nodes: NodesOperand,
        ) -> Operand[Bare[Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("integer").discard_index().cast(CastTarget.String)

        assert set(self.graphrecord.query_nodes(bare_cast)) == {"1", "2", "3"}

    def test_cast_failures(self) -> None:
        def unparsable_text(
            nodes: NodesOperand,
        ) -> Operand[Bare[Scalar], Definite, Ungrouped]:
            return nodes.attribute("text").cast(CastTarget.Int).errors().count()

        def datetime_to_bool(
            nodes: NodesOperand,
        ) -> Operand[Bare[Scalar], Definite, Ungrouped]:
            return nodes.attribute("moment").cast(CastTarget.Bool).errors().count()

        def null_to_int(
            nodes: NodesOperand,
        ) -> Operand[Bare[Scalar], Definite, Ungrouped]:
            return nodes.attribute("nothing").cast(CastTarget.Int).errors().count()

        def dropped(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("text").cast(CastTarget.Int).on_error(Drop())

        def raised(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("text").cast(CastTarget.Int).on_error(Raise())

        assert self.graphrecord.query_nodes(unparsable_text) == 3
        assert self.graphrecord.query_nodes(datetime_to_bool) == 3
        assert self.graphrecord.query_nodes(null_to_int) == 3
        assert self.graphrecord.query_nodes(dropped) == []
        with pytest.raises(QueryError, match="cast"):
            self.graphrecord.query_nodes(raised)

    def test_cast_attribute_name_targets(self) -> None:
        def to_string(
            nodes: NodesOperand,
        ) -> Operand[
            Indexed[Expanded[NodeIndex, AttributeNameIndex], AttributeName],
            Multiple[Unordered],
            Ungrouped,
        ]:
            return (
                nodes.filter(nodes.index().equal_to("n1"))
                .attributes()
                .cast(CastTarget.String)
            )

        def to_int_failures(
            nodes: NodesOperand,
        ) -> Operand[Bare[Scalar], Definite, Ungrouped]:
            return (
                nodes.filter(nodes.index().equal_to("n1"))
                .attributes()
                .cast(CastTarget.Int)
                .errors()
                .count()
            )

        assert {value for _, value in self.graphrecord.query_nodes(to_string)} == {
            "duration",
            "flag",
            "integer",
            "moment",
            "nothing",
            "number",
            "text",
        }
        assert self.graphrecord.query_nodes(to_int_failures) == 7

    def test_discard_index_and_discard_value(self) -> None:
        def discard_index(
            nodes: NodesOperand,
        ) -> Operand[Bare[Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("integer").discard_index()

        def discard_value(nodes: NodesOperand) -> NodesOperand:
            return nodes.attribute("integer").discard_value()

        assert set(self.graphrecord.query_nodes(discard_index)) == {1, 2, 3}
        assert set(self.graphrecord.query_nodes(discard_value)) == {"n1", "n2", "n3"}

    def test_enumerate_receivers(self) -> None:
        def indexed_receiver(
            nodes: NodesOperand,
        ) -> Operand[Indexed[Positional, Scalar], Multiple[Ordered], Ungrouped]:
            return nodes.attribute("integer").sort().enumerate()

        def bare_receiver(
            nodes: NodesOperand,
        ) -> Operand[Indexed[Positional, Scalar], Multiple[Ordered], Ungrouped]:
            return nodes.attribute("integer").sort().discard_index().enumerate()

        assert self.graphrecord.query_nodes(indexed_receiver) == [
            (0, 1),
            (1, 2),
            (2, 3),
        ]
        assert self.graphrecord.query_nodes(bare_receiver) == [(0, 1), (1, 2), (2, 3)]

    def test_expand_to_argument_derived_value(self) -> None:
        def scalar_template(
            nodes: NodesOperand,
        ) -> Operand[
            Indexed[Expanded[NodeIndex, Positional], Scalar],
            Multiple[Ordered],
            Ungrouped,
        ]:
            return (
                nodes.attribute("text").split("a").expand_to(nodes.attribute("integer"))
            )

        def mask_template(
            nodes: NodesOperand,
        ) -> Operand[
            Indexed[Expanded[NodeIndex, Positional], Mask],
            Multiple[Ordered],
            Ungrouped,
        ]:
            return (
                nodes.attribute("text")
                .split("a")
                .expand_to(nodes.has_attribute("integer"))
            )

        def literal_template(
            nodes: NodesOperand,
        ) -> Operand[
            Indexed[Expanded[NodeIndex, Positional], Scalar],
            Multiple[Ordered],
            Ungrouped,
        ]:
            return nodes.attribute("text").split("a").expand_to(0)

        assert sorted(self.graphrecord.query_nodes(scalar_template)) == [
            (("n1", 0), 1),
            (("n1", 1), 1),
            (("n2", 0), 2),
            (("n2", 1), 2),
            (("n3", 0), 3),
            (("n3", 1), 3),
            (("n3", 2), 3),
        ]
        assert sorted(self.graphrecord.query_nodes(mask_template)) == [
            (("n1", 0), True),
            (("n1", 1), True),
            (("n2", 0), True),
            (("n2", 1), True),
            (("n3", 0), True),
            (("n3", 1), True),
            (("n3", 2), True),
        ]
        assert sorted(self.graphrecord.query_nodes(literal_template)) == [
            (("n1", 0), 0),
            (("n1", 1), 0),
            (("n2", 0), 0),
            (("n2", 1), 0),
            (("n3", 0), 0),
            (("n3", 1), 0),
            (("n3", 2), 0),
        ]

    def test_expand_to_grouped_template(self) -> None:
        def grouped(
            nodes: NodesOperand,
        ) -> Operand[
            Indexed[Expanded[NodeIndex, Positional], Scalar],
            Multiple[Ordered],
            Grouped[NodeIndex, ValueIndex, Ungrouped],
        ]:
            return (
                nodes.group_by(nodes.attribute("flag"))
                .attribute("text")
                .split("a")
                .expand_to(nodes.attribute("integer"))
            )

        buckets, key_failures = self.graphrecord.query_nodes(grouped)
        result = {key: payload for key, _, payload in buckets}
        flagged = result[True]
        unflagged = result[False]

        assert key_failures == []
        assert not isinstance(flagged, QueryError)
        assert not isinstance(unflagged, QueryError)
        assert sorted(flagged) == [
            (("n1", 0), 1),
            (("n1", 1), 1),
            (("n3", 0), 3),
            (("n3", 1), 3),
            (("n3", 2), 3),
        ]
        assert sorted(unflagged) == [(("n2", 0), 2), (("n2", 1), 2)]

    def test_transition_targets(self) -> None:
        def scalar_to_mask(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Mask], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("flag").transition(ValueTarget.Mask)

        def scalar_to_attribute_name(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, AttributeName], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("text").transition(ValueTarget.AttributeName)

        def scalar_to_value_index(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return (
                nodes.attribute("integer")
                .transition(ValueTarget.ValueIndex)
                .transition(ValueTarget.Value)
            )

        def node_index_to_scalar(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.index().transition(ValueTarget.Value)

        def mask_to_bool_index(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Mask], Multiple[Unordered], Ungrouped]:
            return (
                nodes.has_attribute("integer")
                .transition(ValueTarget.BoolIndex)
                .transition(ValueTarget.Mask)
            )

        assert dict(self.graphrecord.query_nodes(scalar_to_mask)) == {
            "n1": True,
            "n2": False,
            "n3": True,
        }
        assert dict(self.graphrecord.query_nodes(scalar_to_attribute_name)) == {
            "n1": "Alpha",
            "n2": "Beta",
            "n3": "Gamma",
        }
        assert dict(self.graphrecord.query_nodes(scalar_to_value_index)) == {
            "n1": 1,
            "n2": 2,
            "n3": 3,
        }
        assert dict(self.graphrecord.query_nodes(node_index_to_scalar)) == {
            "n1": "n1",
            "n2": "n2",
            "n3": "n3",
        }
        assert dict(self.graphrecord.query_nodes(mask_to_bool_index)) == {
            "n1": True,
            "n2": True,
            "n3": True,
        }


class TestComplexQuery(unittest.TestCase):
    def setUp(self) -> None:
        self.graphrecord = GraphRecord.from_tuples(
            [
                ("n1", {"tier": "a", "flag": True, "size": 1, "text": "x-y"}),
                ("n2", {"tier": "a", "flag": False, "size": 2, "text": "p-q"}),
                ("n3", {"tier": "b", "flag": True, "size": 3, "text": "m-n"}),
                ("n4", {"tier": "b", "flag": True, "size": 4, "text": "u-v"}),
            ],
            [("n1", "n2", {}), ("n2", "n3", {}), ("n3", "n4", {})],
        )

    def test_three_hop_expanded_carrier(self) -> None:
        def three_hops(
            nodes: NodesOperand,
        ) -> Operand[
            Indexed[
                Expanded[
                    Expanded[Expanded[NodeIndex, NodeIndex], NodeIndex], NodeIndex
                ],
                IndexValue[NodeIndex],
            ],
            Multiple[Unordered],
            Ungrouped,
        ]:
            return (
                nodes.filter(nodes.index().equal_to("n1"))
                .via_neighbors(EdgeDirection.Outgoing)
                .via_neighbors(EdgeDirection.Outgoing)
                .via_neighbors(EdgeDirection.Outgoing)
                .index()
            )

        def third_child(
            nodes: NodesOperand,
        ) -> Operand[
            Indexed[
                Expanded[
                    Expanded[Expanded[NodeIndex, NodeIndex], NodeIndex], NodeIndex
                ],
                IndexValue[NodeIndex],
            ],
            Multiple[Unordered],
            Ungrouped,
        ]:
            return (
                nodes.filter(nodes.index().equal_to("n1"))
                .via_neighbors(EdgeDirection.Outgoing)
                .via_neighbors(EdgeDirection.Outgoing)
                .via_neighbors(EdgeDirection.Outgoing)
                .discard_value()
                .index()
                .child_index()
            )

        def second_child(
            nodes: NodesOperand,
        ) -> Operand[
            Indexed[
                Expanded[
                    Expanded[Expanded[NodeIndex, NodeIndex], NodeIndex], NodeIndex
                ],
                IndexValue[NodeIndex],
            ],
            Multiple[Unordered],
            Ungrouped,
        ]:
            return (
                nodes.filter(nodes.index().equal_to("n1"))
                .via_neighbors(EdgeDirection.Outgoing)
                .via_neighbors(EdgeDirection.Outgoing)
                .via_neighbors(EdgeDirection.Outgoing)
                .discard_value()
                .index()
                .parent_index()
                .child_index()
            )

        def first_child(
            nodes: NodesOperand,
        ) -> Operand[
            Indexed[
                Expanded[
                    Expanded[Expanded[NodeIndex, NodeIndex], NodeIndex], NodeIndex
                ],
                IndexValue[NodeIndex],
            ],
            Multiple[Unordered],
            Ungrouped,
        ]:
            return (
                nodes.filter(nodes.index().equal_to("n1"))
                .via_neighbors(EdgeDirection.Outgoing)
                .via_neighbors(EdgeDirection.Outgoing)
                .via_neighbors(EdgeDirection.Outgoing)
                .discard_value()
                .index()
                .parent_index()
                .parent_index()
                .child_index()
            )

        def root(
            nodes: NodesOperand,
        ) -> Operand[
            Indexed[
                Expanded[
                    Expanded[Expanded[NodeIndex, NodeIndex], NodeIndex], NodeIndex
                ],
                IndexValue[NodeIndex],
            ],
            Multiple[Unordered],
            Ungrouped,
        ]:
            return (
                nodes.filter(nodes.index().equal_to("n1"))
                .via_neighbors(EdgeDirection.Outgoing)
                .via_neighbors(EdgeDirection.Outgoing)
                .via_neighbors(EdgeDirection.Outgoing)
                .discard_value()
                .index()
                .parent_index()
                .parent_index()
                .parent_index()
            )

        deepest = ((("n1", "n2"), "n3"), "n4")

        assert self.graphrecord.query_nodes(three_hops) == [(deepest, "n4")]
        assert self.graphrecord.query_nodes(third_child) == [(deepest, "n4")]
        assert self.graphrecord.query_nodes(second_child) == [(deepest, "n3")]
        assert self.graphrecord.query_nodes(first_child) == [(deepest, "n2")]
        assert self.graphrecord.query_nodes(root) == [(deepest, "n1")]

    def test_via_edges_then_via_nodes(self) -> None:
        def endpoints(
            nodes: NodesOperand,
        ) -> Operand[
            Indexed[
                Expanded[Expanded[NodeIndex, EdgeIndex], EndpointRole],
                IndexValue[NodeIndex],
            ],
            Multiple[Unordered],
            Ungrouped,
        ]:
            return (
                nodes.filter(nodes.index().equal_to("n1"))
                .via_edges(EdgeDirection.Outgoing)
                .via_nodes()
                .index()
            )

        assert set(self.graphrecord.query_nodes(endpoints)) == {
            ((("n1", 0), EdgeEndpointRole.Source), "n1"),
            ((("n1", 0), EdgeEndpointRole.Target), "n2"),
        }

    def test_depth_three_grouping(self) -> None:
        def innermost_work_fully_unwound(
            nodes: NodesOperand,
        ) -> Operand[Indexed[ValueIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return (
                nodes.group_by(nodes.attribute("tier"))
                .group_by(nodes.attribute("flag"))
                .group_by(nodes.attribute("size"))
                .attribute("size")
                .sum()
                .ungroup_keyed()
                .ungroup()
                .ungroup()
            )

        def innermost_keys(
            nodes: NodesOperand,
        ) -> Operand[Indexed[ValueIndex, Unit], Multiple[Unordered], Ungrouped]:
            return (
                nodes.group_by(nodes.attribute("tier"))
                .group_by(nodes.attribute("flag"))
                .group_by(nodes.attribute("size"))
                .keys()
                .ungroup()
                .ungroup()
            )

        def middle_keys(
            nodes: NodesOperand,
        ) -> Operand[Indexed[ValueIndex, Unit], Multiple[Unordered], Ungrouped]:
            return (
                nodes.group_by(nodes.attribute("tier"))
                .group_by(nodes.attribute("flag"))
                .group_by(nodes.attribute("size"))
                .ungroup()
                .keys()
                .ungroup()
            )

        def outer_keys(
            nodes: NodesOperand,
        ) -> Operand[Indexed[ValueIndex, Unit], Multiple[Unordered], Ungrouped]:
            return (
                nodes.group_by(nodes.attribute("tier"))
                .group_by(nodes.attribute("flag"))
                .group_by(nodes.attribute("size"))
                .ungroup()
                .ungroup()
                .keys()
            )

        def members_after_two_unwinds(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return (
                nodes.group_by(nodes.attribute("tier"))
                .group_by(nodes.attribute("flag"))
                .group_by(nodes.attribute("size"))
                .attribute("size")
                .ungroup()
                .ungroup()
                .ungroup()
            )

        assert sorted(self.graphrecord.query_nodes(innermost_work_fully_unwound)) == [
            (1, 1),
            (2, 2),
            (3, 3),
            (4, 4),
        ]
        assert set(self.graphrecord.query_nodes(innermost_keys)) == {1, 2, 3, 4}
        with pytest.raises(DuplicateIndexError, match="occurs more than once"):
            self.graphrecord.query_nodes(middle_keys)
        assert set(self.graphrecord.query_nodes(outer_keys)) == {"a", "b"}
        assert sorted(self.graphrecord.query_nodes(members_after_two_unwinds)) == [
            ("n1", 1),
            ("n2", 2),
            ("n3", 3),
            ("n4", 4),
        ]

    def test_depth_three_grouped_result(self) -> None:
        def per_level_totals(
            nodes: NodesOperand,
        ) -> Operand[
            Bare[Scalar],
            Single,
            Grouped[
                NodeIndex,
                ValueIndex,
                Grouped[
                    NodeIndex, ValueIndex, Grouped[NodeIndex, ValueIndex, Ungrouped]
                ],
            ],
        ]:
            return (
                nodes.group_by(nodes.attribute("tier"))
                .group_by(nodes.attribute("flag"))
                .group_by(nodes.attribute("size"))
                .attribute("size")
                .sum()
            )

        tier_buckets, tier_failures = self.graphrecord.query_nodes(per_level_totals)
        totals: Dict[Tuple[IndexPayload, IndexPayload, IndexPayload], ScalarValue] = {}
        flag_keys: Dict[IndexPayload, Set[IndexPayload]] = {}

        assert tier_failures == []

        for tier_key, _, tier_payload in tier_buckets:
            assert not isinstance(tier_payload, QueryError)
            flag_buckets, flag_failures = tier_payload
            assert flag_failures == []
            flag_keys[tier_key] = {key for key, _, _ in flag_buckets}

            for flag_key, _, flag_payload in flag_buckets:
                assert not isinstance(flag_payload, QueryError)
                size_buckets, size_failures = flag_payload
                assert size_failures == []

                for size_key, _, size_payload in size_buckets:
                    assert not isinstance(size_payload, QueryError)
                    totals[tier_key, flag_key, size_key] = size_payload

        assert {key for key, _, _ in tier_buckets} == {"a", "b"}
        assert flag_keys == {"a": {False, True}, "b": {True}}
        assert totals == {
            ("a", True, 1): 1,
            ("a", False, 2): 2,
            ("b", True, 3): 3,
            ("b", True, 4): 4,
        }

    def test_mixed_axis_expansion_inside_group(self) -> None:
        def split_at_depth(
            nodes: NodesOperand,
        ) -> Operand[
            Indexed[Expanded[NodeIndex, Positional], Scalar],
            Multiple[Unordered],
            Ungrouped,
        ]:
            return (
                nodes.group_by(nodes.attribute("tier"))
                .attribute("text")
                .split("-")
                .length()
                .ungroup()
            )

        assert sorted(self.graphrecord.query_nodes(split_at_depth)) == [
            (("n1", 0), 1),
            (("n1", 1), 1),
            (("n2", 0), 1),
            (("n2", 1), 1),
            (("n3", 0), 1),
            (("n3", 1), 1),
            (("n4", 0), 1),
            (("n4", 1), 1),
        ]

    def test_mixed_axis_group_over_expanded_lane(self) -> None:
        def group_expanded(
            nodes: NodesOperand,
        ) -> Operand[
            Indexed[ValueIndex, Scalar],
            Multiple[Unordered],
            Ungrouped,
        ]:
            fragments = nodes.attribute("text").split("-")
            return fragments.group_by(fragments.length()).length().sum().ungroup_keyed()

        assert self.graphrecord.query_nodes(group_expanded) == [(1, 8)]

    def test_multi_branch_filter(self) -> None:
        def branches(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            small = nodes.attribute("size").less_than(3)
            tier_b = nodes.attribute("tier").equal_to("b")
            unflagged = nodes.attribute("flag").transition(ValueTarget.Mask)
            return nodes.filter((small & tier_b) | ~unflagged).attribute("size")

        assert sorted(self.graphrecord.query_nodes(branches)) == [("n2", 2)]

    def test_cross_lane_aggregate_argument(self) -> None:
        def above_mean(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            sizes = nodes.attribute("size")
            return nodes.filter(sizes.greater_than(sizes.mean())).attribute("size")

        assert sorted(self.graphrecord.query_nodes(above_mean)) == [
            ("n3", 3),
            ("n4", 4),
        ]

    def test_worst_case_combination(self) -> None:
        def combined(
            nodes: NodesOperand,
        ) -> Operand[
            Bare[Scalar],
            Single,
            Grouped[NodeIndex, ValueIndex, Grouped[NodeIndex, ValueIndex, Ungrouped]],
        ]:
            sizes = nodes.attribute("size")
            reached = nodes.filter(sizes.greater_than(sizes.mean())).via_neighbors(
                EdgeDirection.Outgoing
            )
            return (
                reached.select()
                .group_by(nodes.attribute("tier"))
                .group_by(nodes.attribute("flag"))
                .attribute("size")
                .sum()
            )

        def failure_path(
            nodes: NodesOperand,
        ) -> Operand[Indexed[ValueIndex, FailureValue], Multiple[Unordered], Ungrouped]:
            return (
                nodes.group_by(nodes.attribute("tier"))
                .group_by(nodes.attribute("flag"))
                .attribute("missing")
                .sum()
                .bucket_errors()
                .ungroup()
            )

        tier_buckets, tier_failures = self.graphrecord.query_nodes(combined)
        tier_key, _, tier_payload = tier_buckets[0]

        assert tier_failures == []
        assert len(tier_buckets) == 1
        assert tier_key == "b"
        assert not isinstance(tier_payload, QueryError)

        flag_buckets, flag_failures = tier_payload

        assert flag_failures == []
        assert [(key, payload) for key, _, payload in flag_buckets] == [(True, 4)]
        with pytest.raises(DuplicateIndexError, match="occurs more than once"):
            self.graphrecord.query_nodes(failure_path)


class TestTraversalOperand(unittest.TestCase):
    def setUp(self) -> None:
        self.graphrecord = GraphRecord.from_tuples(
            [("n1", {}), ("n2", {}), ("n3", {})],
            [("n1", "n2", {}), ("n1", "n3", {}), ("n2", "n3", {})],
        )

    def test_direct_traversal(self) -> None:
        def edges(nodes: NodesOperand) -> EdgesOperand:
            return nodes.filter(nodes.index().equal_to("n1")).edges(
                EdgeDirection.Outgoing
            )

        def neighbors(nodes: NodesOperand) -> NodesOperand:
            return nodes.filter(nodes.index().equal_to("n1")).neighbors(
                EdgeDirection.Outgoing
            )

        def source_node(
            edges: EdgesOperand,
        ) -> Operand[Indexed[NodeIndex, Unit], Multiple[Unordered], Ungrouped]:
            return edges.source_node()

        def target_node(
            edges: EdgesOperand,
        ) -> Operand[Indexed[NodeIndex, Unit], Multiple[Unordered], Ungrouped]:
            return edges.target_node()

        def nodes(
            edges: EdgesOperand,
        ) -> Operand[Indexed[NodeIndex, Unit], Multiple[Unordered], Ungrouped]:
            return edges.nodes()

        assert set(self.graphrecord.query_nodes(edges)) == {0, 1}
        assert set(self.graphrecord.query_nodes(neighbors)) == {"n2", "n3"}
        assert set(self.graphrecord.query_edges(source_node)) == {"n1", "n2"}
        assert set(self.graphrecord.query_edges(target_node)) == {"n2", "n3"}
        assert set(self.graphrecord.query_edges(nodes)) == {"n1", "n2", "n3"}

    def test_reference_preserving_traversal(self) -> None:
        def via_edges(
            nodes: NodesOperand,
        ) -> Operand[
            Indexed[Expanded[NodeIndex, EdgeIndex], IndexValue[EdgeIndex]],
            Multiple[Unordered],
            Ungrouped,
        ]:
            return (
                nodes.filter(nodes.index().equal_to("n1"))
                .via_edges(EdgeDirection.Outgoing)
                .index()
            )

        def via_neighbors(
            nodes: NodesOperand,
        ) -> Operand[
            Indexed[Expanded[NodeIndex, NodeIndex], IndexValue[NodeIndex]],
            Multiple[Unordered],
            Ungrouped,
        ]:
            return (
                nodes.filter(nodes.index().equal_to("n1"))
                .via_neighbors(EdgeDirection.Outgoing)
                .index()
            )

        def via_nodes(
            edges: EdgesOperand,
        ) -> Operand[
            Indexed[Expanded[EdgeIndex, EndpointRole], IndexValue[NodeIndex]],
            Multiple[Unordered],
            Ungrouped,
        ]:
            return edges.via_nodes().index()

        def via_source_node(
            edges: EdgesOperand,
        ) -> Operand[
            Indexed[EdgeIndex, IndexValue[NodeIndex]],
            Multiple[Unordered],
            Ungrouped,
        ]:
            return edges.via_source_node().index()

        def via_target_node(
            edges: EdgesOperand,
        ) -> Operand[
            Indexed[EdgeIndex, IndexValue[NodeIndex]],
            Multiple[Unordered],
            Ungrouped,
        ]:
            return edges.via_target_node().index()

        def expanded_index_equality(
            nodes: NodesOperand,
        ) -> Operand[
            Indexed[Expanded[NodeIndex, EdgeIndex], Mask],
            Multiple[Unordered],
            Ungrouped,
        ]:
            indices = nodes.via_edges(EdgeDirection.Outgoing).discard_value().index()
            return indices.equal_to(indices)

        assert set(self.graphrecord.query_nodes(via_edges)) == {
            (("n1", 0), 0),
            (("n1", 1), 1),
        }
        assert set(self.graphrecord.query_nodes(via_neighbors)) == {
            (("n1", "n2"), "n2"),
            (("n1", "n3"), "n3"),
        }
        assert len(self.graphrecord.query_edges(via_nodes)) == 6
        assert dict(self.graphrecord.query_edges(via_source_node)) == {
            0: "n1",
            1: "n1",
            2: "n2",
        }
        assert dict(self.graphrecord.query_edges(via_target_node)) == {
            0: "n2",
            1: "n3",
            2: "n3",
        }
        assert set(self.graphrecord.query_nodes(expanded_index_equality)) == {
            (("n1", 0), True),
            (("n1", 1), True),
            (("n2", 2), True),
        }


class TestErrorOperand(unittest.TestCase):
    def setUp(self) -> None:
        self.graphrecord = GraphRecord.from_tuples(
            [("n1", {"value": 1}), ("n2", {"value": 2})]
        )

    def test_error_inspection(self) -> None:
        def errors(
            nodes: NodesOperand,
        ) -> Operand[
            Indexed[NodeIndex, FailureValue],
            Multiple[Unordered],
            Ungrouped,
        ]:
            return nodes.attribute("missing").errors()

        def kinds(
            nodes: NodesOperand,
        ) -> Operand[
            Indexed[NodeIndex, FailureKindValue],
            Multiple[Unordered],
            Ungrouped,
        ]:
            return nodes.attribute("missing").errors().kind()

        def names(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("missing").errors().kind().name()

        error_values = dict(self.graphrecord.query_nodes(errors))
        kind_values = dict(self.graphrecord.query_nodes(kinds))

        assert set(error_values) == {"n1", "n2"}
        assert all(
            isinstance(error, MissingAttributeError) for error in error_values.values()
        )
        assert set(kind_values) == {"n1", "n2"}
        assert all(
            not isinstance(kind, QueryError) and kind.name == "MissingAttribute"
            for kind in kind_values.values()
        )
        assert dict(self.graphrecord.query_nodes(names)) == {
            "n1": "MissingAttribute",
            "n2": "MissingAttribute",
        }

    def test_error_policies(self) -> None:
        def drop(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("missing").on_error(Drop())

        def replace(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("missing").on_error(Replace(5))

        def raise_(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.attribute("missing").on_error(Raise())

        def raise_when(
            nodes: NodesOperand,
        ) -> Operand[Bare[Scalar], Definite, Ungrouped]:
            return (
                nodes.attribute("missing")
                .on_error(Raise.when(condition=False))
                .errors()
                .count()
            )

        assert self.graphrecord.query_nodes(drop) == []
        assert dict(self.graphrecord.query_nodes(replace)) == {"n1": 5, "n2": 5}
        with pytest.raises(MissingAttributeError, match="no attribute"):
            self.graphrecord.query_nodes(raise_)
        assert self.graphrecord.query_nodes(raise_when) == 2


class TestGroupOperand(unittest.TestCase):
    def setUp(self) -> None:
        self.graphrecord = GraphRecord.from_tuples(
            [
                ("n1", {"kind": True, "value": 2}),
                ("n2", {"kind": False, "value": 4}),
                ("n3", {"kind": True, "value": 6}),
            ]
        )

    def test_group_structure(self) -> None:
        def keys(
            nodes: NodesOperand,
        ) -> Operand[Indexed[ValueIndex, Unit], Multiple[Unordered], Ungrouped]:
            return nodes.group_by(nodes.attribute("kind")).keys()

        def having(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Unit], Multiple[Unordered], Ungrouped]:
            grouped = nodes.group_by(nodes.attribute("kind"))
            retained_key = True
            return grouped.having(
                grouped.keys().index().equal_to(retained_key)
            ).ungroup()

        def ungroup(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return nodes.group_by(nodes.attribute("kind")).attribute("value").ungroup()

        def ungroup_keyed(
            nodes: NodesOperand,
        ) -> Operand[Indexed[ValueIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return (
                nodes.group_by(nodes.attribute("kind"))
                .attribute("value")
                .mean()
                .ungroup_keyed()
            )

        assert set(self.graphrecord.query_nodes(keys)) == {False, True}
        assert set(self.graphrecord.query_nodes(having)) == {"n1", "n3"}
        assert dict(self.graphrecord.query_nodes(ungroup)) == {
            "n1": 2,
            "n2": 4,
            "n3": 6,
        }
        assert dict(self.graphrecord.query_nodes(ungroup_keyed)) == {
            False: 4,
            True: 4,
        }

    def test_group_broadcast(self) -> None:
        def broadcast(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return (
                nodes.group_by(nodes.attribute("kind"))
                .attribute("value")
                .mean()
                .broadcast()
            )

        def broadcast_via(
            nodes: NodesOperand,
        ) -> Operand[Indexed[NodeIndex, Scalar], Multiple[Unordered], Ungrouped]:
            return (
                nodes.group_by(nodes.attribute("kind"))
                .attribute("value")
                .mean()
                .broadcast_via(nodes.attribute("kind"))
            )

        expected = {"n1": 4, "n2": 4, "n3": 4}
        assert dict(self.graphrecord.query_nodes(broadcast)) == expected
        assert dict(self.graphrecord.query_nodes(broadcast_via)) == expected

    def test_group_failures(self) -> None:
        def bucket_errors(
            nodes: NodesOperand,
        ) -> Operand[
            Indexed[ValueIndex, FailureValue],
            Multiple[Unordered],
            Ungrouped,
        ]:
            return (
                nodes.group_by(nodes.attribute("kind"))
                .attribute("missing")
                .sum()
                .bucket_errors()
            )

        def drop_bucket_errors(
            nodes: NodesOperand,
        ) -> Operand[Indexed[ValueIndex, Unit], Multiple[Unordered], Ungrouped]:
            return (
                nodes.group_by(nodes.attribute("kind"))
                .attribute("missing")
                .sum()
                .on_bucket_error(Drop())
                .keys()
            )

        def raise_bucket_errors(
            nodes: NodesOperand,
        ) -> Operand[Indexed[ValueIndex, Unit], Multiple[Unordered], Ungrouped]:
            return (
                nodes.group_by(nodes.attribute("kind"))
                .attribute("missing")
                .sum()
                .on_bucket_error(Raise())
                .keys()
            )

        def key_errors(
            nodes: NodesOperand,
        ) -> Operand[
            Indexed[NodeIndex, FailureValue],
            Multiple[Unordered],
            Ungrouped,
        ]:
            return nodes.group_by(nodes.attribute("missing")).key_errors()

        def drop_key_errors(
            nodes: NodesOperand,
        ) -> Operand[Indexed[ValueIndex, Unit], Multiple[Unordered], Ungrouped]:
            return (
                nodes.group_by(nodes.attribute("missing")).on_key_error(Drop()).keys()
            )

        def raise_key_errors(
            nodes: NodesOperand,
        ) -> Operand[Indexed[ValueIndex, Unit], Multiple[Unordered], Ungrouped]:
            return (
                nodes.group_by(nodes.attribute("missing")).on_key_error(Raise()).keys()
            )

        assert len(self.graphrecord.query_nodes(bucket_errors)) == 2
        assert self.graphrecord.query_nodes(drop_bucket_errors) == []
        with pytest.raises(MissingAttributeError, match="no attribute"):
            self.graphrecord.query_nodes(raise_bucket_errors)
        assert len(self.graphrecord.query_nodes(key_errors)) == 3
        assert self.graphrecord.query_nodes(drop_key_errors) == []
        with pytest.raises(MissingAttributeError, match="no attribute"):
            self.graphrecord.query_nodes(raise_key_errors)


class TestGraphRecordQuery(unittest.TestCase):
    def setUp(self) -> None:
        self.graphrecord = GraphRecord.from_tuples(
            [("n1", {}), ("n2", {})], [("n1", "n2", {})]
        )

    def test_query_as_node_argument(self) -> None:
        def query(nodes: NodesOperand) -> NodesOperand:
            return nodes.filter(nodes.index().equal_to("n1"))

        assert self.graphrecord.outgoing_edges(query) == {"n1": [0]}

    def test_query_failure_as_node_argument(self) -> None:
        def query(nodes: NodesOperand) -> NodesOperand:
            return nodes.attribute("missing").discard_value()

        with pytest.raises(MissingAttributeError, match="no attribute"):
            self.graphrecord.outgoing_edges(query)
