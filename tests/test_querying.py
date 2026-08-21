import unittest
from datetime import datetime
from typing import Any

import pytest

from graphrecords import EdgeDirection, GraphRecord
from graphrecords.querying import (
    ArgumentMissingError,
    CastTarget,
    DivisionByZeroError,
    Drop,
    DuplicateExpandedChildIndexError,
    DuplicateIndexError,
    EdgeEndpointRole,
    EmptySplitDelimiterError,
    EvaluationCacheGraphRecordMismatchError,
    Expression,
    ExternalError,
    FailureKind,
    GraphRecordError,
    GroupedResult,
    IncomparableIndicesError,
    IncomparableValuesAtError,
    IncomparableValuesError,
    IntegerOverflowError,
    InvalidCastError,
    InvalidClipBoundsError,
    InvalidMedianValueError,
    InvalidPaddingCharacterError,
    InvalidPartitionBucketArityError,
    InvalidRegexPatternError,
    InvalidStandardDeviationValueError,
    InvalidStringSliceError,
    InvalidTransitionError,
    InvalidVarianceValueError,
    MissingAttributeError,
    MissingGroupAggregateError,
    MissingGroupBucketError,
    MissingTraversedAttributeError,
    ModuloByZeroError,
    NegativeSquareRootError,
    NoChildIndexError,
    NonIntegerValueError,
    NonNumericValueError,
    NonPositiveLogarithmError,
    NonStringValueError,
    QueryError,
    Raise,
    RaisedFailuresError,
    Replace,
    ResultConsumedError,
    ResultView,
    Series,
    StringLengthOverflowError,
    StringPaddingOverflowError,
    UncoveredIndicesError,
    UnresolvedBucketFailuresError,
    UnresolvedGroupKeyFailuresError,
    UnresolvedIndexError,
    UnsupportedValueRoleError,
    ValueTarget,
    edges,
    groups,
    nodes,
)

ALWAYS = True
NEVER = False

EXCEPTION_ROSTER = [
    ArgumentMissingError,
    DivisionByZeroError,
    DuplicateExpandedChildIndexError,
    DuplicateIndexError,
    EmptySplitDelimiterError,
    EvaluationCacheGraphRecordMismatchError,
    ExternalError,
    GraphRecordError,
    IncomparableIndicesError,
    IncomparableValuesAtError,
    IncomparableValuesError,
    IntegerOverflowError,
    InvalidCastError,
    InvalidClipBoundsError,
    InvalidMedianValueError,
    InvalidPaddingCharacterError,
    InvalidPartitionBucketArityError,
    InvalidRegexPatternError,
    InvalidStandardDeviationValueError,
    InvalidStringSliceError,
    InvalidTransitionError,
    InvalidVarianceValueError,
    MissingAttributeError,
    MissingGroupAggregateError,
    MissingGroupBucketError,
    MissingTraversedAttributeError,
    ModuloByZeroError,
    NegativeSquareRootError,
    NoChildIndexError,
    NonIntegerValueError,
    NonNumericValueError,
    NonPositiveLogarithmError,
    NonStringValueError,
    RaisedFailuresError,
    StringLengthOverflowError,
    StringPaddingOverflowError,
    UncoveredIndicesError,
    UnresolvedBucketFailuresError,
    UnresolvedGroupKeyFailuresError,
    UnresolvedIndexError,
    UnsupportedValueRoleError,
]


def create_record() -> GraphRecord:
    record = GraphRecord()
    record = record.add_nodes(
        [
            ("lorem", {"amet": -2, "consectetur": "  Sit Amet  ", "adipiscing": True}),
            ("ipsum", {"amet": 5, "consectetur": "elit-elit", "adipiscing": None}),
            (
                "dolor",
                {
                    "amet": 8,
                    "consectetur": "consectetur",
                    "adipiscing": datetime(2024, 1, 2),
                },
            ),
            ("sit", {"amet": 5}),
        ]
    )
    record = record.add_edges(
        [
            ("lorem", "ipsum", {"tempor": 10}),
            ("ipsum", "dolor", {"tempor": 20}),
            ("lorem", "dolor", {"tempor": 30}),
        ]
    )
    record = record.add_group("elit")
    record = record.add_nodes_to_group(["lorem", "ipsum"], "elit")
    record = record.add_edges_to_group(record.edge_indices()[:1], "elit")

    return record.add_group("incididunt")


def create_bucket_record() -> GraphRecord:
    return GraphRecord().add_nodes(
        [
            ("lorem", {"amet": 1, "consectetur": "sit"}),
            ("ipsum", {"amet": 4, "consectetur": "sit"}),
            ("dolor", {"amet": 9, "consectetur": "elit"}),
            ("sit", {"amet": 16, "consectetur": "elit"}),
        ]
    )


def create_key_failure_record() -> GraphRecord:
    return GraphRecord().add_nodes(
        [
            ("lorem", {"amet": 1, "consectetur": "sit"}),
            ("ipsum", {"amet": 4}),
        ]
    )


def create_aligned_record() -> GraphRecord:
    return GraphRecord().add_nodes(
        [
            ("dolor", {"amet": -3}),
            ("lorem", {"amet": 8}),
            ("ipsum", {"amet": 5}),
        ]
    )


def create_population_record() -> GraphRecord:
    return GraphRecord().add_nodes(
        [
            ("lorem", {"consectetur": "sit"}),
            ("dolor", {"consectetur": "elit"}),
            ("amet", {"consectetur": "elit"}),
        ]
    )


def create_failure_kind() -> FailureKind:
    record = create_record()
    kinds = record.nodes().attribute("consectetur").errors().kind().evaluate()
    kind = next(iter(kinds))[1]
    assert isinstance(kind, FailureKind)

    return kind


class TestExpressionSurface(unittest.TestCase):
    def setUp(self) -> None:
        self.record = create_record()

    def test_free_roots(self) -> None:
        assert isinstance(nodes(), Expression)
        assert isinstance(edges(), Expression)
        assert isinstance(groups(), Expression)

    def test_bound_roots(self) -> None:
        assert isinstance(self.record.nodes(), Series)
        assert isinstance(self.record.edges(), Series)
        assert isinstance(self.record.groups(), Series)
        assert list(self.record.nodes().evaluate()) == [
            "lorem",
            "ipsum",
            "dolor",
            "sit",
        ]
        assert list(self.record.groups().evaluate()) == ["elit", "incididunt"]

    def test_query(self) -> None:
        bound = self.record.query(nodes().attribute("amet"))

        assert isinstance(bound, Series)
        assert list(bound.evaluate()) == [
            ("lorem", -2),
            ("ipsum", 5),
            ("dolor", 8),
            ("sit", 5),
        ]

    def test_repr(self) -> None:
        assert repr(nodes()) == "Expression(Expression [AllNodes])"
        assert repr(self.record.nodes()) == "Series(Series [AllNodes])"

    def test_explain(self) -> None:
        assert "AllNodes" in nodes().explain()
        assert "Attribute" in self.record.nodes().attribute("amet").explain()
        assert (
            "Attribute" in self.record.nodes().attribute("amet").explain_unoptimized()
        )

    def test_invalid_binding(self) -> None:
        unbound: Expression[Any, Any, Any] = nodes()
        bound: Any = self.record.nodes()

        with pytest.raises(TypeError, match="must be bound to a record"):
            unbound.evaluate()

        with pytest.raises(TypeError, match="must be bound to a record"):
            unbound.explain_unoptimized()

        with pytest.raises(TypeError, match="must be free"):
            self.record.query(bound)

    def test_invalid_argument(self) -> None:
        modes: Any = self.record.nodes().attribute("amet").mode()

        with pytest.raises(TypeError, match="series argument must hold"):
            self.record.nodes().attribute("amet").greater_than(modes)

    def test_invalid_missing_policy(self) -> None:
        aggregate: Any = nodes().attribute("amet").count()

        with pytest.raises(TypeError, match="`on_missing` policy needs"):
            aggregate.on_missing(Drop())

        with pytest.raises(TypeError, match="`on_missing` policy needs"):
            aggregate.on_missing(Replace(0))


class TestSeriesArguments(unittest.TestCase):
    def setUp(self) -> None:
        self.record = create_record()
        self.aligned = create_aligned_record()

    def test_same_record_argument(self) -> None:
        mask = self.record.nodes().attribute("amet").greater_than(0)

        assert list(self.record.nodes().filter(mask).evaluate()) == [
            "ipsum",
            "dolor",
            "sit",
        ]

    def test_cross_record_argument(self) -> None:
        elements = list(
            self.record.nodes()
            .attribute("amet")
            .greater_than(self.aligned.nodes().attribute("amet"))
            .evaluate()
        )

        assert [index for index, _ in elements[:3]] == ["lorem", "ipsum", "dolor"]
        assert [value for _, value in elements[:3]] == [False, False, True]
        assert elements[3][0] == "sit"
        assert isinstance(elements[3][1], ArgumentMissingError)

    def test_set_argument(self) -> None:
        assert [
            value
            for _, value in self.record.nodes()
            .attribute("amet")
            .is_in(self.aligned.nodes().attribute("amet"))
            .evaluate()
        ] == [False, True, True, True]

    def test_single_series_set_argument(self) -> None:
        assert [
            value
            for _, value in self.record.nodes()
            .attribute("amet")
            .is_in(self.record.nodes().attribute("amet").sort().first())
            .evaluate()
        ] == [True, False, False, False]

    def test_bare_series_set_argument(self) -> None:
        assert [
            value
            for _, value in self.record.nodes()
            .attribute("amet")
            .is_in(self.record.nodes().attribute("amet").discard_index())
            .evaluate()
        ] == [True, True, True, True]

    def test_reference_series_argument(self) -> None:
        sources = self.record.edges().via_source_node()
        counts = sources.group_by(self.record.edges().via_source_node()).count()

        result = counts.evaluate()

        assert result["lorem"] == 2
        assert result["ipsum"] == 1

    def test_bare_set_argument(self) -> None:
        assert [
            value
            for _, value in self.record.nodes()
            .attribute("amet")
            .is_in(self.aligned.nodes().attribute("amet").max())
            .evaluate()
        ] == [False, False, True, False]

    def test_dropping_argument(self) -> None:
        dropped = self.aligned.nodes().attribute("amet").on_missing(Drop())

        assert list(
            self.record.nodes().attribute("amet").greater_than(dropped).evaluate()
        ) == [("lorem", False), ("ipsum", False), ("dolor", True)]

    def test_bare_definite_argument(self) -> None:
        record = create_bucket_record()

        assert list(
            record.nodes()
            .attribute("amet")
            .greater_than(record.nodes().count())
            .evaluate()
        ) == [("lorem", False), ("ipsum", False), ("dolor", True), ("sit", True)]

    def test_cross_record_bare_definite_argument(self) -> None:
        record = create_bucket_record()

        assert list(
            record.nodes()
            .attribute("amet")
            .greater_than(self.aligned.nodes().count())
            .evaluate()
        ) == [("lorem", False), ("ipsum", True), ("dolor", True), ("sit", True)]

    def test_broadcast_population(self) -> None:
        record = create_bucket_record()
        grouped = record.nodes().group_by(nodes().attribute("consectetur"))

        assert list(
            grouped.attribute("amet")
            .sum()
            .broadcast_via(record.nodes().attribute("consectetur"))
            .evaluate()
        ) == [("lorem", 5), ("ipsum", 5), ("dolor", 25), ("sit", 25)]

    def test_cross_record_broadcast_population(self) -> None:
        record = create_bucket_record()
        population = create_population_record()
        grouped = record.nodes().group_by(nodes().attribute("consectetur"))

        assert list(
            grouped.attribute("amet")
            .sum()
            .broadcast_via(population.nodes().attribute("consectetur"))
            .evaluate()
        ) == [("lorem", 5), ("dolor", 25)]

    def test_replacing_argument(self) -> None:
        replaced = self.aligned.nodes().attribute("amet").on_missing(Replace(0))

        assert list(
            self.record.nodes().attribute("amet").greater_than(replaced).evaluate()
        ) == [
            ("lorem", False),
            ("ipsum", False),
            ("dolor", True),
            ("sit", True),
        ]


class TestFilters(unittest.TestCase):
    def setUp(self) -> None:
        self.record = create_record()

    def test_filter(self) -> None:
        assert list(
            self.record.nodes()
            .filter(nodes().attribute("amet").greater_than(0))
            .evaluate()
        ) == ["ipsum", "dolor", "sit"]
        assert list(self.record.nodes().filter(NEVER).evaluate()) == []

    def test_masks(self) -> None:
        positive = self.record.nodes().attribute("amet").greater_than(0)

        assert list(
            positive.and_(nodes().attribute("amet").less_than(8)).evaluate()
        ) == [
            ("lorem", False),
            ("ipsum", True),
            ("dolor", False),
            ("sit", True),
        ]
        assert list(
            self.record.nodes()
            .attribute("amet")
            .greater_than(7)
            .or_(nodes().attribute("amet").less_than(0))
            .evaluate()
        ) == [
            ("lorem", True),
            ("ipsum", False),
            ("dolor", True),
            ("sit", False),
        ]
        assert list(
            positive.xor(nodes().attribute("amet").less_than(8)).evaluate()
        ) == [
            ("lorem", True),
            ("ipsum", False),
            ("dolor", True),
            ("sit", False),
        ]
        assert list(positive.not_().evaluate()) == [
            ("lorem", True),
            ("ipsum", False),
            ("dolor", False),
            ("sit", False),
        ]
        assert list(positive.and_(ALWAYS).evaluate()) == [
            ("lorem", False),
            ("ipsum", True),
            ("dolor", True),
            ("sit", True),
        ]

    def test_has_attribute(self) -> None:
        assert list(self.record.nodes().has_attribute("consectetur").evaluate()) == [
            ("lorem", True),
            ("ipsum", True),
            ("dolor", True),
            ("sit", False),
        ]

    def test_in_group(self) -> None:
        assert list(self.record.nodes().in_group("elit").evaluate()) == [
            ("lorem", True),
            ("ipsum", True),
            ("dolor", False),
            ("sit", False),
        ]

    def test_invalid_in_group(self) -> None:
        with pytest.raises(GraphRecordError, match="Cannot find group"):
            self.record.nodes().in_group("tempor").evaluate()


class TestOrdering(unittest.TestCase):
    def setUp(self) -> None:
        self.record = create_record()

    def test_sort(self) -> None:
        assert list(self.record.nodes().attribute("amet").sort().evaluate()) == [
            ("lorem", -2),
            ("ipsum", 5),
            ("sit", 5),
            ("dolor", 8),
        ]

    def test_sort_by(self) -> None:
        assert list(
            self.record.nodes().sort_by(nodes().attribute("amet")).evaluate()
        ) == ["lorem", "ipsum", "sit", "dolor"]

    def test_reverse_order(self) -> None:
        assert list(
            self.record.nodes().attribute("amet").sort().reverse_order().evaluate()
        ) == [("dolor", 8), ("sit", 5), ("ipsum", 5), ("lorem", -2)]

    def test_shuffle(self) -> None:
        shuffled = list(self.record.nodes().attribute("amet").shuffle().evaluate())

        assert len(shuffled) == 4
        assert {value for _, value in shuffled} == {-2, 5, 8}

    def test_unorder(self) -> None:
        unordered = self.record.nodes().attribute("amet").sort().unorder()

        assert len(list(unordered.evaluate())) == 4

    def test_take(self) -> None:
        assert list(
            self.record.nodes().attribute("amet").sort().take(2).evaluate()
        ) == [("lorem", -2), ("ipsum", 5)]

    def test_first(self) -> None:
        assert self.record.nodes().attribute("amet").sort().first().evaluate() == (
            "lorem",
            -2,
        )

    def test_last(self) -> None:
        assert self.record.nodes().attribute("amet").sort().last().evaluate() == (
            "dolor",
            8,
        )

    def test_enumerate(self) -> None:
        assert list(
            self.record.nodes().attribute("amet").sort().enumerate().evaluate()
        ) == [(0, -2), (1, 5), (2, 5), (3, 8)]


class TestDeduplication(unittest.TestCase):
    def setUp(self) -> None:
        self.record = create_record()

    def test_drop_duplicates(self) -> None:
        assert list(
            self.record.nodes().attribute("amet").sort().drop_duplicates().evaluate()
        ) == [("lorem", -2), ("ipsum", 5), ("dolor", 8)]

    def test_is_duplicated(self) -> None:
        assert list(
            self.record.nodes().attribute("amet").is_duplicated().evaluate()
        ) == [
            ("lorem", False),
            ("ipsum", True),
            ("dolor", False),
            ("sit", True),
        ]

    def test_unique(self) -> None:
        assert list(
            self.record.nodes().attribute("amet").discard_index().unique().evaluate()
        ) == [-2, 5, 8]


class TestTypePredicates(unittest.TestCase):
    def setUp(self) -> None:
        self.record = create_record()

    def test_type_predicates(self) -> None:
        values = self.record.nodes().attribute("adipiscing").on_error(Drop())

        assert list(values.is_bool().evaluate()) == [
            ("lorem", True),
            ("ipsum", False),
            ("dolor", False),
        ]
        assert list(values.is_datetime().evaluate()) == [
            ("lorem", False),
            ("ipsum", False),
            ("dolor", True),
        ]
        assert list(values.is_duration().evaluate()) == [
            ("lorem", False),
            ("ipsum", False),
            ("dolor", False),
        ]
        assert list(values.is_float().evaluate()) == [
            ("lorem", False),
            ("ipsum", False),
            ("dolor", False),
        ]
        assert list(values.is_null().evaluate()) == [
            ("lorem", False),
            ("ipsum", True),
            ("dolor", False),
        ]
        assert list(values.is_int().evaluate()) == [
            ("lorem", False),
            ("ipsum", False),
            ("dolor", False),
        ]
        assert list(values.is_string().evaluate()) == [
            ("lorem", False),
            ("ipsum", False),
            ("dolor", False),
        ]


class TestNumerics(unittest.TestCase):
    def setUp(self) -> None:
        self.record = create_record()
        self.values = self.record.nodes().attribute("amet")

    def test_unary_operations(self) -> None:
        assert [value for _, value in self.values.abs().evaluate()] == [2, 5, 8, 5]
        assert [value for _, value in self.values.neg().evaluate()] == [2, -5, -8, -5]
        assert [value for _, value in self.values.sign().evaluate()] == [
            -1.0,
            1.0,
            1,
            1,
        ]
        assert [value for _, value in self.values.ceil().evaluate()] == [-2.0, 5, 8, 5]
        assert [value for _, value in self.values.floor().evaluate()] == [-2.0, 5, 8, 5]
        assert [value for _, value in self.values.round().evaluate()] == [-2.0, 5, 8, 5]
        assert [value for _, value in self.values.cbrt().evaluate()][2] == 2.0
        assert len(list(self.values.exp().evaluate())) == 4

    def test_invalid_unary_operations(self) -> None:
        logarithms = [value for _, value in self.values.log().evaluate()]
        roots = [value for _, value in self.values.sqrt().evaluate()]

        assert isinstance(logarithms[0], NonPositiveLogarithmError)
        assert isinstance(roots[0], NegativeSquareRootError)

    def test_arithmetic(self) -> None:
        assert [value for _, value in self.values.add(2).evaluate()] == [0, 7, 10, 7]
        assert [value for _, value in self.values.subtract(2).evaluate()] == [
            -4,
            3,
            6,
            3,
        ]
        assert [value for _, value in self.values.multiply(2).evaluate()] == [
            -4,
            10,
            16,
            10,
        ]
        assert [value for _, value in self.values.power(2).evaluate()] == [
            4,
            25,
            64,
            25,
        ]
        assert [value for _, value in self.values.modulo(2).evaluate()] == [0, 1, 0, 1]
        assert [value for _, value in self.values.divide(2).evaluate()] == [
            -1.0,
            2.5,
            4.0,
            2.5,
        ]

    def test_clip(self) -> None:
        assert [value for _, value in self.values.clip(0, 6).evaluate()] == [0, 5, 6, 5]

    def test_comparisons(self) -> None:
        assert [value for _, value in self.values.greater_than(0).evaluate()] == [
            False,
            True,
            True,
            True,
        ]
        assert [
            value for _, value in self.values.greater_than_or_equal_to(5).evaluate()
        ] == [False, True, True, True]
        assert [value for _, value in self.values.less_than(5).evaluate()] == [
            True,
            False,
            False,
            False,
        ]
        assert [
            value for _, value in self.values.less_than_or_equal_to(5).evaluate()
        ] == [True, True, False, True]
        assert [value for _, value in (self.values == 5).evaluate()] == [
            False,
            True,
            False,
            True,
        ]
        assert [value for _, value in (self.values != 5).evaluate()] == [
            True,
            False,
            True,
            False,
        ]

    def test_is_in(self) -> None:
        assert [value for _, value in self.values.is_in([5, 8]).evaluate()] == [
            False,
            True,
            True,
            True,
        ]
        assert [
            value
            for _, value in self.values.is_in(nodes().attribute("amet")).evaluate()
        ] == [True, True, True, True]

    def test_cast(self) -> None:
        assert [
            value for _, value in self.values.cast(CastTarget.String).evaluate()
        ] == ["-2", "5", "8", "5"]
        assert [
            value for _, value in self.values.cast(CastTarget.Float).evaluate()
        ] == [-2.0, 5.0, 8.0, 5.0]
        assert [value for _, value in self.values.cast(CastTarget.Int).evaluate()] == [
            -2,
            5,
            8,
            5,
        ]
        assert [value for _, value in self.values.cast(CastTarget.Bool).evaluate()] == [
            True,
            True,
            True,
            True,
        ]
        assert len(list(self.values.cast(CastTarget.DateTime).evaluate())) == 4
        assert len(list(self.values.cast(CastTarget.Duration).evaluate())) == 4

    def test_invalid_cast(self) -> None:
        casts = [
            value
            for _, value in self.record.nodes()
            .attribute("consectetur")
            .on_error(Drop())
            .cast(CastTarget.Int)
            .evaluate()
        ]

        assert isinstance(casts[0], InvalidCastError)


class TestStrings(unittest.TestCase):
    def setUp(self) -> None:
        self.record = create_record()
        self.strings = self.record.nodes().attribute("consectetur").on_error(Drop())

    def test_trimming(self) -> None:
        assert [value for _, value in self.strings.trim().evaluate()] == [
            "Sit Amet",
            "elit-elit",
            "consectetur",
        ]
        assert [value for _, value in self.strings.trim_start().evaluate()] == [
            "Sit Amet  ",
            "elit-elit",
            "consectetur",
        ]
        assert [value for _, value in self.strings.trim_end().evaluate()] == [
            "  Sit Amet",
            "elit-elit",
            "consectetur",
        ]

    def test_case_conversions(self) -> None:
        assert [value for _, value in self.strings.lowercase().evaluate()] == [
            "  sit amet  ",
            "elit-elit",
            "consectetur",
        ]
        assert [value for _, value in self.strings.uppercase().evaluate()] == [
            "  SIT AMET  ",
            "ELIT-ELIT",
            "CONSECTETUR",
        ]

    def test_reverse(self) -> None:
        assert [value for _, value in self.strings.reverse().evaluate()] == [
            "  temA tiS  ",
            "tile-tile",
            "rutetcesnoc",
        ]

    def test_length(self) -> None:
        assert [value for _, value in self.strings.length().evaluate()] == [12, 9, 11]

    def test_slice(self) -> None:
        assert [value for _, value in self.strings.slice(0, 3).evaluate()] == [
            "  S",
            "eli",
            "con",
        ]

    def test_invalid_slice(self) -> None:
        slices = [value for _, value in self.strings.slice(0, 200).evaluate()]

        assert isinstance(slices[0], InvalidStringSliceError)

    def test_predicates(self) -> None:
        assert [value for _, value in self.strings.starts_with("elit").evaluate()] == [
            False,
            True,
            False,
        ]
        assert [value for _, value in self.strings.ends_with("elit").evaluate()] == [
            False,
            True,
            False,
        ]
        assert [value for _, value in self.strings.contains("t-e").evaluate()] == [
            False,
            True,
            False,
        ]
        assert [value for _, value in self.strings.matches("^con.*ur$").evaluate()] == [
            False,
            False,
            True,
        ]

    def test_stripping(self) -> None:
        assert [value for _, value in self.strings.strip_prefix("elit").evaluate()] == [
            "  Sit Amet  ",
            "-elit",
            "consectetur",
        ]
        assert [value for _, value in self.strings.strip_suffix("elit").evaluate()] == [
            "  Sit Amet  ",
            "elit-",
            "consectetur",
        ]

    def test_replacing(self) -> None:
        assert [
            value for _, value in self.strings.replace("elit", "amet").evaluate()
        ] == ["  Sit Amet  ", "amet-elit", "consectetur"]
        assert [
            value for _, value in self.strings.replace_all("elit", "amet").evaluate()
        ] == ["  Sit Amet  ", "amet-amet", "consectetur"]

    def test_padding(self) -> None:
        assert [value for _, value in self.strings.pad_start(15, "*").evaluate()] == [
            "***  Sit Amet  ",
            "******elit-elit",
            "****consectetur",
        ]
        assert [value for _, value in self.strings.pad_end(15, "*").evaluate()] == [
            "  Sit Amet  ***",
            "elit-elit******",
            "consectetur****",
        ]

    def test_invalid_padding(self) -> None:
        padded = [value for _, value in self.strings.pad_start(15, "**").evaluate()]

        assert isinstance(padded[0], InvalidPaddingCharacterError)

    def test_split(self) -> None:
        assert list(self.strings.split("-").evaluate()) == [
            (("lorem", 0), "  Sit Amet  "),
            (("ipsum", 0), "elit"),
            (("ipsum", 1), "elit"),
            (("dolor", 0), "consectetur"),
        ]

    def test_invalid_split(self) -> None:
        parts = [value for _, value in self.strings.split("").evaluate()]

        assert isinstance(parts[0], EmptySplitDelimiterError)


class TestAttributes(unittest.TestCase):
    def setUp(self) -> None:
        self.record = create_record()

    def test_attribute(self) -> None:
        assert list(self.record.nodes().attribute("amet").evaluate()) == [
            ("lorem", -2),
            ("ipsum", 5),
            ("dolor", 8),
            ("sit", 5),
        ]

    def test_attributes(self) -> None:
        assert list(self.record.nodes().attributes().evaluate())[:3] == [
            (("lorem", "adipiscing"), "adipiscing"),
            (("lorem", "amet"), "amet"),
            (("lorem", "consectetur"), "consectetur"),
        ]

    def test_index(self) -> None:
        assert list(self.record.nodes().index().evaluate()) == [
            ("lorem", "lorem"),
            ("ipsum", "ipsum"),
            ("dolor", "dolor"),
            ("sit", "sit"),
        ]

    def test_discard_index(self) -> None:
        assert list(
            self.record.nodes().attribute("amet").discard_index().evaluate()
        ) == [-2, 5, 8, 5]

    def test_discard_value(self) -> None:
        assert list(
            self.record.nodes().attribute("amet").discard_value().evaluate()
        ) == ["lorem", "ipsum", "dolor", "sit"]

    def test_parent_index(self) -> None:
        assert list(
            self.record.nodes()
            .attributes()
            .discard_value()
            .index()
            .parent_index()
            .evaluate()
        )[:3] == [
            (("lorem", "adipiscing"), "lorem"),
            (("lorem", "amet"), "lorem"),
            (("lorem", "consectetur"), "lorem"),
        ]

    def test_child_index(self) -> None:
        assert list(
            self.record.nodes()
            .attributes()
            .discard_value()
            .index()
            .child_index()
            .evaluate()
        )[:3] == [
            (("lorem", "adipiscing"), "adipiscing"),
            (("lorem", "amet"), "amet"),
            (("lorem", "consectetur"), "consectetur"),
        ]

    def test_inherit(self) -> None:
        assert list(
            self.record.nodes()
            .attributes()
            .inherit(nodes().attribute("amet"))
            .evaluate()
        )[:3] == [
            (("lorem", "adipiscing"), -2),
            (("lorem", "amet"), -2),
            (("lorem", "consectetur"), -2),
        ]

    def test_transition(self) -> None:
        assert list(
            self.record.nodes()
            .attribute("amet")
            .transition(ValueTarget.ValueIndex)
            .evaluate()
        ) == [("lorem", -2), ("ipsum", 5), ("dolor", 8), ("sit", 5)]
        assert list(
            self.record.nodes().index().transition(ValueTarget.Value).evaluate()
        ) == [
            ("lorem", "lorem"),
            ("ipsum", "ipsum"),
            ("dolor", "dolor"),
            ("sit", "sit"),
        ]

    def test_invalid_transition(self) -> None:
        transitions = [
            value
            for _, value in self.record.nodes()
            .attribute("amet")
            .transition(ValueTarget.Mask)
            .evaluate()
        ]

        assert isinstance(transitions[0], InvalidTransitionError)

    def test_cache(self) -> None:
        assert list(self.record.nodes().attribute("amet").cache().evaluate()) == [
            ("lorem", -2),
            ("ipsum", 5),
            ("dolor", 8),
            ("sit", 5),
        ]
        assert isinstance(nodes().attribute("amet").cache(), Expression)


class TestErrorPolicies(unittest.TestCase):
    def setUp(self) -> None:
        self.record = create_record()
        self.failing = self.record.nodes().attribute("consectetur")

    def test_failure_in_value_position(self) -> None:
        result = list(self.failing.evaluate())

        assert len(result) == 4
        assert result[3][0] == "sit"
        assert isinstance(result[3][1], MissingAttributeError)
        assert isinstance(result[3][1], QueryError)

    def test_on_error(self) -> None:
        assert [index for index, _ in self.failing.on_error(Drop()).evaluate()] == [
            "lorem",
            "ipsum",
            "dolor",
        ]
        assert list(self.failing.on_error(Replace("amet")).evaluate())[3][1] == "amet"

        replaced = self.failing.on_error(Replace(nodes().attribute("amet")))

        assert list(replaced.evaluate())[3][1] == 5

        quiet = self.failing.on_error(Raise.when(nodes().count().greater_than(100)))

        assert len(list(quiet.evaluate())) == 4

        with pytest.raises(RaisedFailuresError, match="failing element"):
            self.failing.on_error(Raise()).evaluate()

        with pytest.raises(RaisedFailuresError, match="failing element"):
            self.failing.on_error(Raise.when(ALWAYS)).evaluate()

    def test_errors(self) -> None:
        failures = list(self.failing.errors().evaluate())

        assert len(failures) == 1
        assert failures[0][0] == "sit"
        assert isinstance(failures[0][1], MissingAttributeError)

    def test_kind(self) -> None:
        kinds = list(self.failing.errors().kind().evaluate())

        assert len(kinds) == 1
        assert kinds[0][0] == "sit"
        assert isinstance(kinds[0][1], FailureKind)
        assert kinds[0][1].name == "MissingAttribute"

    def test_name(self) -> None:
        assert list(self.failing.errors().kind().name().evaluate()) == [
            ("sit", "MissingAttribute")
        ]


class TestFailureKind(unittest.TestCase):
    def setUp(self) -> None:
        self.record = create_record()
        self.kind = create_failure_kind()

    def test_name(self) -> None:
        assert self.kind.name == "MissingAttribute"

    def test_repr(self) -> None:
        assert repr(self.kind) == "FailureKind.MissingAttribute"

    def test_equality(self) -> None:
        assert self.kind == create_failure_kind()
        assert self.kind != "MissingAttribute"

    def test_hash(self) -> None:
        assert len({self.kind, self.kind}) == 1

    def test_as_argument(self) -> None:
        kinds = self.record.nodes().attribute("consectetur").errors().kind()

        assert list((kinds == self.kind).evaluate()) == [("sit", True)]
        assert list(kinds.is_in([self.kind]).evaluate()) == [("sit", True)]


class TestAggregations(unittest.TestCase):
    def setUp(self) -> None:
        self.record = create_record()
        self.values = self.record.nodes().attribute("amet")

    def test_aggregations(self) -> None:
        assert self.record.nodes().count().evaluate() == 4
        assert self.values.sum().evaluate() == 16
        assert self.values.mean().evaluate() == 4.0
        assert self.values.max().evaluate() == 8
        assert self.values.min().evaluate() == -2
        assert self.values.median().evaluate() == 5.0
        assert self.values.product().evaluate() == -400
        assert self.values.n_unique().evaluate() == 3
        assert list(self.values.mode().evaluate()) == [5]

        deviation = self.values.std().evaluate()
        variance = self.values.var().evaluate()
        sample = self.values.random().evaluate()

        assert isinstance(deviation, float)
        assert deviation > 0
        assert isinstance(variance, float)
        assert variance > 0
        assert sample is not None
        assert sample[1] in {-2, 5, 8}

    def test_boolean_aggregations(self) -> None:
        assert self.values.greater_than(-10).all().evaluate()
        assert self.values.greater_than(7).any().evaluate()

    def test_empty_aggregations(self) -> None:
        empty = self.record.nodes().filter(NEVER).attribute("amet")

        assert empty.max().evaluate() is None
        assert self.record.nodes().filter(NEVER).count().evaluate() == 0


class TestTraversals(unittest.TestCase):
    def setUp(self) -> None:
        self.record = create_record()

    def test_edges(self) -> None:
        assert len(list(self.record.nodes().edges().evaluate())) == 3
        assert (
            len(list(self.record.nodes().edges(EdgeDirection.Outgoing).evaluate())) == 3
        )
        assert (
            len(list(self.record.nodes().edges(EdgeDirection.Incoming).evaluate())) == 3
        )
        assert len(list(self.record.nodes().edges(EdgeDirection.Both).evaluate())) == 3

    def test_via_edges(self) -> None:
        assert len(list(self.record.nodes().via_edges().evaluate())) == 6
        assert (
            len(list(self.record.nodes().via_edges(EdgeDirection.Outgoing).evaluate()))
            == 3
        )

    def test_neighbors(self) -> None:
        assert list(
            self.record.nodes().neighbors(EdgeDirection.Outgoing).evaluate()
        ) == ["ipsum", "dolor"]
        assert list(self.record.nodes().neighbors().evaluate()) == [
            "ipsum",
            "dolor",
            "lorem",
        ]

    def test_via_neighbors(self) -> None:
        assert list(
            self.record.nodes().via_neighbors(EdgeDirection.Outgoing).evaluate()
        ) == [
            (("lorem", "ipsum"), "ipsum"),
            (("lorem", "dolor"), "dolor"),
            (("ipsum", "dolor"), "dolor"),
        ]

    def test_nodes(self) -> None:
        assert list(self.record.edges().nodes().evaluate()) == [
            "lorem",
            "ipsum",
            "dolor",
        ]

    def test_via_nodes(self) -> None:
        endpoints = list(self.record.edges().via_nodes().evaluate())

        assert len(endpoints) == 6
        assert endpoints[0][0][1] == EdgeEndpointRole.Source
        assert endpoints[1][0][1] == EdgeEndpointRole.Target

    def test_source_node(self) -> None:
        assert list(self.record.edges().source_node().evaluate()) == ["lorem", "ipsum"]

    def test_target_node(self) -> None:
        assert list(self.record.edges().target_node().evaluate()) == ["ipsum", "dolor"]

    def test_via_source_node(self) -> None:
        assert [
            value for _, value in self.record.edges().via_source_node().evaluate()
        ] == ["lorem", "ipsum", "lorem"]

    def test_via_target_node(self) -> None:
        assert [
            value for _, value in self.record.edges().via_target_node().evaluate()
        ] == ["ipsum", "dolor", "dolor"]

    def test_resolve(self) -> None:
        assert list(self.record.nodes().index().resolve().evaluate()) == [
            ("lorem", "lorem"),
            ("ipsum", "ipsum"),
            ("dolor", "dolor"),
            ("sit", "sit"),
        ]

    def test_select(self) -> None:
        assert len(list(self.record.nodes().via_edges().select().evaluate())) == 3


class TestGroupLanes(unittest.TestCase):
    def setUp(self) -> None:
        self.record = create_record()

    def test_groups(self) -> None:
        assert list(self.record.nodes().groups().evaluate()) == ["elit"]

    def test_via_groups(self) -> None:
        assert list(self.record.nodes().via_groups().evaluate()) == [
            (("lorem", "elit"), "elit"),
            (("ipsum", "elit"), "elit"),
        ]

    def test_group_nodes(self) -> None:
        assert list(self.record.groups().nodes().evaluate()) == ["lorem", "ipsum"]

    def test_group_edges(self) -> None:
        assert len(list(self.record.groups().edges().evaluate())) == 1

    def test_group_via_edges(self) -> None:
        assert len(list(self.record.groups().via_edges().evaluate())) == 1

    def test_invalid_group_edges(self) -> None:
        group_lane: Expression[Any, Any, Any] = self.record.groups()

        with pytest.raises(TypeError, match="carry no direction"):
            group_lane.edges(EdgeDirection.Both)

        with pytest.raises(TypeError, match="carry no direction"):
            group_lane.via_edges(EdgeDirection.Both)

    def test_node_count(self) -> None:
        assert list(self.record.groups().node_count().evaluate()) == [
            ("elit", 2),
            ("incididunt", 0),
        ]

    def test_edge_count(self) -> None:
        assert list(self.record.groups().edge_count().evaluate()) == [
            ("elit", 1),
            ("incididunt", 0),
        ]


class TestGrouping(unittest.TestCase):
    def setUp(self) -> None:
        self.record = create_bucket_record()
        self.grouped = self.record.nodes().group_by(nodes().attribute("consectetur"))

    def test_group_by(self) -> None:
        result = self.grouped.evaluate()

        assert isinstance(result, GroupedResult)
        assert result.keys() == ["sit", "elit"]

    def test_keys(self) -> None:
        assert list(self.grouped.keys().evaluate()) == ["sit", "elit"]

    def test_having(self) -> None:
        grouped_edges = (
            create_record().edges().group_by(edges().via_source_node().index())
        )
        kept = grouped_edges.having(nodes().attribute("amet").greater_than(0))

        assert kept.evaluate().keys() == ["ipsum"]
        assert self.grouped.having(ALWAYS).evaluate().keys() == ["sit", "elit"]

    def test_ungroup(self) -> None:
        assert list(self.grouped.ungroup().evaluate()) == [
            "lorem",
            "ipsum",
            "dolor",
            "sit",
        ]

    def test_ungroup_keyed(self) -> None:
        assert list(
            self.grouped.attribute("amet").sum().ungroup_keyed().evaluate()
        ) == [("sit", 5), ("elit", 25)]

    def test_broadcast(self) -> None:
        assert list(self.grouped.attribute("amet").sum().broadcast().evaluate()) == [
            ("lorem", 5),
            ("ipsum", 5),
            ("dolor", 25),
            ("sit", 25),
        ]

    def test_broadcast_via(self) -> None:
        assert list(
            self.grouped.attribute("amet")
            .sum()
            .broadcast_via(nodes().attribute("consectetur"))
            .evaluate()
        ) == [("lorem", 5), ("ipsum", 5), ("dolor", 25), ("sit", 25)]

    def test_key_errors(self) -> None:
        record = create_key_failure_record()
        grouped = record.nodes().group_by(nodes().attribute("consectetur"))
        failures = list(grouped.key_errors().evaluate())

        assert list(self.grouped.key_errors().evaluate()) == []
        assert len(failures) == 1
        assert failures[0][0] == "ipsum"

    def test_bucket_errors(self) -> None:
        assert (
            list(self.grouped.attribute("amet").sum().bucket_errors().evaluate()) == []
        )

    def test_on_key_error(self) -> None:
        record = create_key_failure_record()
        grouped = record.nodes().group_by(nodes().attribute("consectetur"))

        assert grouped.on_key_error(Drop()).evaluate().keys() == ["sit"]
        assert self.grouped.on_key_error(Raise()).evaluate().keys() == ["sit", "elit"]

        with pytest.raises(MissingAttributeError):
            grouped.on_key_error(Raise()).evaluate()

    def test_on_bucket_error(self) -> None:
        sums = self.grouped.attribute("amet").sum()

        assert sums.on_bucket_error(Drop()).evaluate().keys() == ["sit", "elit"]
        assert sums.on_bucket_error(Raise()).evaluate().keys() == ["sit", "elit"]

    def test_nested_grouping(self) -> None:
        nested = self.grouped.group_by(nodes().attribute("amet"))
        result = nested.evaluate()
        bucket = result["sit"]

        assert result.keys() == ["sit", "elit"]
        assert isinstance(bucket, GroupedResult)
        assert bucket.keys() == [1, 4]

        members = bucket[1]

        assert isinstance(members, ResultView)
        assert list(members) == ["lorem"]


class TestResults(unittest.TestCase):
    def setUp(self) -> None:
        self.record = create_record()

    def test_result_view(self) -> None:
        view = self.record.nodes().attribute("amet").evaluate()

        assert isinstance(view, ResultView)
        assert repr(view) == "ResultView()"
        assert list(view) == [("lorem", -2), ("ipsum", 5), ("dolor", 8), ("sit", 5)]

        cursor = iter(self.record.nodes().attribute("amet").evaluate())

        assert iter(cursor) is cursor
        assert next(cursor) == ("lorem", -2)
        assert list(cursor) == [("ipsum", 5), ("dolor", 8), ("sit", 5)]

    def test_invalid_result_view(self) -> None:
        view = self.record.nodes().attribute("amet").evaluate()
        list(view)

        with pytest.raises(ResultConsumedError, match="is consumed"):
            list(view)

        partial = self.record.nodes().attribute("amet").evaluate()
        next(iter(partial))

        with pytest.raises(ResultConsumedError, match="is consumed"):
            iter(partial)

    def test_single_result(self) -> None:
        assert self.record.nodes().attribute("amet").max().evaluate() == 8
        assert self.record.nodes().attribute("amet").sort().last().evaluate() == (
            "dolor",
            8,
        )
        assert (
            self.record.nodes().filter(NEVER).attribute("amet").max().evaluate() is None
        )

    def test_definite_result(self) -> None:
        assert self.record.nodes().count().evaluate() == 4

    def test_grouped_result(self) -> None:
        record = create_bucket_record()
        result = record.nodes().group_by(nodes().attribute("consectetur")).evaluate()

        members = result["sit"]

        assert len(result) == 2
        assert result.keys() == ["sit", "elit"]
        assert list(result) == ["sit", "elit"]
        assert "sit" in result
        assert "amet" not in result
        assert repr(result) == "GroupedResult(keys=['sit', 'elit'])"
        assert isinstance(members, ResultView)
        assert list(members) == ["lorem", "ipsum"]

    def test_grouped_result_scalar_payloads(self) -> None:
        record = create_bucket_record()
        result = (
            record.nodes()
            .group_by(nodes().attribute("consectetur"))
            .attribute("amet")
            .sum()
            .evaluate()
        )

        assert result["sit"] == 5
        assert result["elit"] == 25

    def test_grouped_result_key_failures(self) -> None:
        record = create_key_failure_record()
        result = record.nodes().group_by(nodes().attribute("consectetur")).evaluate()
        failures = result.key_failures

        assert len(failures) == 1
        assert failures[0][0] == "ipsum"
        assert isinstance(failures[0][1], MissingAttributeError)

    def test_grouped_result_edge_index_keys(self) -> None:
        result = self.record.edges().group_by(edges().index()).evaluate()
        edge_index = self.record.edge_indices()[0]
        members = result[edge_index]

        assert edge_index in result
        assert isinstance(members, ResultView)
        assert list(members) == [edge_index]

    def test_grouped_result_failure_kind_keys(self) -> None:
        result = (
            self.record.nodes()
            .group_by(
                nodes().attribute("consectetur").errors().kind().on_missing(Drop())
            )
            .evaluate()
        )
        kind = result.keys()[0]
        members = result[kind]

        assert kind.name == "MissingAttribute"
        assert kind in result
        assert isinstance(members, ResultView)
        assert list(members) == ["sit"]

    def test_grouped_result_foreign_keys(self) -> None:
        result = (
            self.record.edges().group_by(edges().via_source_node().index()).evaluate()
        )
        edge_index = self.record.edge_indices()[0]

        assert (edge_index, EdgeEndpointRole.Source) not in result
        assert (edge_index, EdgeEndpointRole.Target) not in result


class TestArguments(unittest.TestCase):
    def setUp(self) -> None:
        self.record = create_record()

    def test_expression_as_mask_argument(self) -> None:
        assert list(
            self.record.nodes()
            .filter(nodes().attribute("amet").greater_than(0))
            .evaluate()
        ) == ["ipsum", "dolor", "sit"]

    def test_expression_as_value_argument(self) -> None:
        assert [
            value
            for _, value in self.record.nodes()
            .attribute("amet")
            .add(nodes().attribute("amet"))
            .evaluate()
        ] == [-4, 10, 16, 10]

    def test_expression_as_set_argument(self) -> None:
        assert [
            value
            for _, value in self.record.nodes()
            .attribute("amet")
            .is_in(nodes().attribute("amet"))
            .evaluate()
        ] == [True, True, True, True]

    def test_on_missing(self) -> None:
        positive = nodes().filter(nodes().attribute("amet").greater_than(0))

        assert list(
            self.record.nodes()
            .attribute("amet")
            .add(positive.attribute("amet").on_missing(Drop()))
            .evaluate()
        ) == [("ipsum", 10), ("dolor", 16), ("sit", 10)]
        assert list(
            self.record.nodes()
            .attribute("amet")
            .add(positive.attribute("amet").on_missing(Replace(100)))
            .evaluate()
        ) == [("lorem", 98), ("ipsum", 10), ("dolor", 16), ("sit", 10)]
        assert list(
            self.record.nodes()
            .attribute("amet")
            .add(positive.attribute("amet").on_missing(Replace(nodes().count())))
            .evaluate()
        ) == [("lorem", 2), ("ipsum", 10), ("dolor", 16), ("sit", 10)]
        assert [
            value
            for _, value in self.record.nodes()
            .attribute("amet")
            .greater_than(positive.attribute("amet").max().on_missing(Replace(0)))
            .evaluate()
        ] == [False, False, False, False]
        assert (
            list(
                self.record.nodes()
                .attribute("amet")
                .greater_than(
                    positive.filter(NEVER).attribute("amet").max().on_missing(Drop())
                )
                .evaluate()
            )
            == []
        )
        assert list(
            self.record.nodes()
            .attribute("amet")
            .add(
                positive.attribute("amet").on_missing(
                    Replace(nodes().attribute("amet").max().on_missing(Drop()))
                )
            )
            .evaluate()
        ) == [("lorem", 6), ("ipsum", 10), ("dolor", 16), ("sit", 10)]

    def test_invalid_is_in(self) -> None:
        values = self.record.nodes().attribute("consectetur")

        with pytest.raises(TypeError, match="single values"):
            values.is_in("lorem")

    def test_edge_index_as_argument(self) -> None:
        edge_index = self.record.edge_indices()[0]

        assert [
            value for _, value in (self.record.edges().index() == edge_index).evaluate()
        ] == [True, False, False]
        assert [
            value
            for _, value in self.record.edges().index().is_in([edge_index]).evaluate()
        ] == [True, False, False]

    def test_endpoint_role_as_argument(self) -> None:
        roles = self.record.edges().via_nodes().discard_value().index().child_index()

        assert [
            value for _, value in (roles == EdgeEndpointRole.Source).evaluate()
        ] == [True, False, True, False, True, False]
        assert [
            value for _, value in (roles != EdgeEndpointRole.Target).evaluate()
        ] == [True, False, True, False, True, False]
        assert [
            value for _, value in roles.is_in([EdgeEndpointRole.Target]).evaluate()
        ] == [False, True, False, True, False, True]


class TestSelections(unittest.TestCase):
    def setUp(self) -> None:
        self.record = create_record()

    def test_expression_selection(self) -> None:
        positive = nodes().filter(nodes().attribute("amet").greater_than(0))

        assert self.record.remove_nodes(positive).node_count() == 1
        assert self.record.keep_nodes(positive).node_count() == 3

    def test_series_selection(self) -> None:
        positive = self.record.nodes().filter(nodes().attribute("amet").greater_than(0))

        assert self.record.keep_nodes(positive).node_count() == 3

    def test_dropping_expression_selection(self) -> None:
        positive = nodes().filter(nodes().attribute("amet").greater_than(0))

        assert self.record.keep_nodes(positive.on_missing(Drop())).node_count() == 3
        assert (
            self.record.keep_nodes(positive.index().on_missing(Drop())).node_count()
            == 3
        )

    def test_dropping_series_selection(self) -> None:
        positive = self.record.nodes().filter(nodes().attribute("amet").greater_than(0))
        reduced = self.record.remove_nodes("sit")

        with pytest.raises(IndexError, match="Cannot find nodes with indices"):
            reduced.keep_nodes(positive)

        assert reduced.keep_nodes(positive.on_missing(Drop())).node_count() == 2

    def test_dropping_mask_selection(self) -> None:
        positive = nodes().filter(nodes().attribute("amet").greater_than(0))
        covered = positive.has_attribute("consectetur")

        with pytest.raises(UncoveredIndicesError, match="uncovered element"):
            self.record.keep_nodes(covered)

        assert self.record.keep_nodes(covered.on_missing(Drop())).node_count() == 2

    def test_dropping_group_selection(self) -> None:
        populated = groups().filter(groups().node_count().greater_than(0))

        assert list(
            self.record.keep_groups(populated.on_missing(Drop())).groups().evaluate()
        ) == ["elit"]

    def test_single_selection(self) -> None:
        assert self.record.add_node_in_group("tempor", {}, "elit").node_count() == 5

    def test_edge_selection(self) -> None:
        heavy = edges().filter(edges().attribute("tempor").greater_than(15))

        assert self.record.remove_edges(heavy).edge_count() == 1
        assert self.record.remove_edges(self.record.edges()).edge_count() == 0
        assert self.record.remove_edges(self.record.edge_indices()).edge_count() == 0

    def test_invalid_selection_policy(self) -> None:
        positive = nodes().filter(nodes().attribute("amet").greater_than(0))
        replaced: Any = positive.has_attribute("consectetur").on_missing(
            Replace(ALWAYS)
        )

        with pytest.raises(TypeError, match="`on_missing\\(Drop\\(\\)\\)` argument"):
            self.record.keep_nodes(replaced)


class TestWrappers(unittest.TestCase):
    def test_edge_direction(self) -> None:
        assert repr(EdgeDirection.Both) == "EdgeDirection.Both"
        assert str(EdgeDirection.Incoming) == "Incoming"

    def test_edge_endpoint_role(self) -> None:
        assert EdgeEndpointRole.Source != EdgeEndpointRole.Target

    def test_value_targets(self) -> None:
        targets = [
            ValueTarget.Value,
            ValueTarget.ValueIndex,
            ValueTarget.AttributeName,
            ValueTarget.AttributeNameIndex,
            ValueTarget.NodeIndex,
            ValueTarget.GroupIndex,
            ValueTarget.PositionalIndex,
            ValueTarget.BoolIndex,
            ValueTarget.Mask,
            ValueTarget.FailureKind,
            ValueTarget.FailureKindIndex,
        ]

        assert len(targets) == 11
        assert all(isinstance(target, ValueTarget) for target in targets)

    def test_cast_targets(self) -> None:
        targets = [
            CastTarget.Bool,
            CastTarget.DateTime,
            CastTarget.Duration,
            CastTarget.Float,
            CastTarget.Int,
            CastTarget.String,
        ]

        assert len(targets) == 6
        assert all(isinstance(target, CastTarget) for target in targets)

    def test_exception_roster(self) -> None:
        assert issubclass(QueryError, Exception)
        assert issubclass(ResultConsumedError, RuntimeError)
        assert all(issubclass(error, QueryError) for error in EXCEPTION_ROSTER)
