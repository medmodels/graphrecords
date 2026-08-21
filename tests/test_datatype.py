import unittest

import graphrecords as gr


class TestDataType(unittest.TestCase):
    def test_string(self) -> None:
        string_type = gr.String()

        assert str(string_type) == "String"
        assert repr(string_type) == "DataType.String"

        assert string_type == gr.String()
        assert string_type != gr.Int()

    def test_int(self) -> None:
        int_type = gr.Int()

        assert str(int_type) == "Int"
        assert repr(int_type) == "DataType.Int"

        assert int_type == gr.Int()
        assert int_type != gr.String()

    def test_float(self) -> None:
        float_type = gr.Float()

        assert str(float_type) == "Float"
        assert repr(float_type) == "DataType.Float"

        assert float_type == gr.Float()
        assert float_type != gr.String()

    def test_bool(self) -> None:
        bool_type = gr.Bool()

        assert str(bool_type) == "Bool"
        assert repr(bool_type) == "DataType.Bool"

        assert bool_type == gr.Bool()
        assert bool_type != gr.String()

    def test_datetime(self) -> None:
        datetime_type = gr.DateTime()

        assert str(datetime_type) == "DateTime"
        assert repr(datetime_type) == "DataType.DateTime"

        assert datetime_type == gr.DateTime()
        assert datetime_type != gr.String()

    def test_duration(self) -> None:
        duration_type = gr.Duration()

        assert str(duration_type) == "Duration"
        assert repr(duration_type) == "DataType.Duration"

        assert duration_type == gr.Duration()
        assert duration_type != gr.String()

    def test_null(self) -> None:
        null_type = gr.Null()

        assert str(null_type) == "Null"
        assert repr(null_type) == "DataType.Null"

        assert null_type == gr.Null()
        assert null_type != gr.String()

    def test_any(self) -> None:
        any_type = gr.Any()

        assert str(any_type) == "Any"
        assert repr(any_type) == "DataType.Any"

        assert any_type == gr.Any()
        assert any_type != gr.String()

    def test_union(self) -> None:
        union_type = gr.Union(gr.String(), gr.Int())

        assert isinstance(union_type.left, gr.String)
        assert isinstance(union_type.right, gr.Int)
        assert union_type.left == gr.String()
        assert union_type.right == gr.Int()

        assert str(union_type) == "Union(String, Int)"
        assert repr(union_type) == "DataType.Union(DataType.String, DataType.Int)"

        assert union_type == gr.Union(gr.String(), gr.Int())
        assert union_type == gr.Union(gr.Int(), gr.String())
        assert union_type != gr.Union(gr.String(), gr.Bool())
        assert union_type != gr.String()

        nested_union_type = gr.Union(gr.String(), gr.Union(gr.Int(), gr.Bool()))

        assert isinstance(nested_union_type.right, gr.Union)
        assert nested_union_type.right == gr.Union(gr.Int(), gr.Bool())

        assert str(nested_union_type) == "Union(String, Union(Int, Bool))"
        assert (
            repr(nested_union_type) == "DataType.Union(DataType.String, "
            "DataType.Union(DataType.Int, DataType.Bool))"
        )

        assert nested_union_type == gr.Union(gr.String(), gr.Union(gr.Int(), gr.Bool()))
        assert nested_union_type == gr.Union(gr.Union(gr.Bool(), gr.Int()), gr.String())
        assert nested_union_type != gr.Union(
            gr.String(), gr.Union(gr.Bool(), gr.Float())
        )

        union_of_option_type = gr.Union(gr.Option(gr.Int()), gr.Bool())

        assert isinstance(union_of_option_type.left, gr.Option)
        assert union_of_option_type.left == gr.Option(gr.Int())
        assert str(union_of_option_type) == "Union(Option(Int), Bool)"

    def test_option(self) -> None:
        option_type = gr.Option(gr.String())

        assert isinstance(option_type.datatype, gr.String)
        assert option_type.datatype == gr.String()

        assert str(option_type) == "Option(String)"
        assert repr(option_type) == "DataType.Option(DataType.String)"

        assert option_type == gr.Option(gr.String())
        assert option_type != gr.Option(gr.Int())
        assert option_type != gr.String()

        nested_option_type = gr.Option(gr.Option(gr.Int()))

        assert isinstance(nested_option_type.datatype, gr.Option)
        assert nested_option_type.datatype == gr.Option(gr.Int())

        assert str(nested_option_type) == "Option(Option(Int))"
        assert (
            repr(nested_option_type) == "DataType.Option(DataType.Option(DataType.Int))"
        )

        assert nested_option_type == gr.Option(gr.Option(gr.Int()))
        assert nested_option_type != gr.Option(gr.Option(gr.Float()))

        option_of_union_type = gr.Option(gr.Union(gr.Int(), gr.Float()))

        assert isinstance(option_of_union_type.datatype, gr.Union)
        assert option_of_union_type.datatype == gr.Union(gr.Int(), gr.Float())
        assert str(option_of_union_type) == "Option(Union(Int, Float))"

    def test_attribute_type_infer(self) -> None:
        assert gr.AttributeType.infer(gr.String()) == gr.AttributeType.Unstructured
        assert gr.AttributeType.infer(gr.Int()) == gr.AttributeType.Continuous
        assert gr.AttributeType.infer(gr.Float()) == gr.AttributeType.Continuous
        assert gr.AttributeType.infer(gr.Bool()) == gr.AttributeType.Categorical
        assert gr.AttributeType.infer(gr.DateTime()) == gr.AttributeType.Temporal
        assert gr.AttributeType.infer(gr.Duration()) == gr.AttributeType.Temporal
        assert gr.AttributeType.infer(gr.Null()) == gr.AttributeType.Unstructured
        assert gr.AttributeType.infer(gr.Any()) == gr.AttributeType.Unstructured
        assert (
            gr.AttributeType.infer(gr.Union(gr.Int(), gr.Float()))
            == gr.AttributeType.Continuous
        )
        assert (
            gr.AttributeType.infer(gr.Option(gr.Bool())) == gr.AttributeType.Categorical
        )

    def test_round_trip_through_schema(self) -> None:
        datatypes = [
            gr.String(),
            gr.Int(),
            gr.Float(),
            gr.Bool(),
            gr.DateTime(),
            gr.Duration(),
            gr.Null(),
            gr.Any(),
            gr.Union(gr.String(), gr.Int()),
            gr.Option(gr.Bool()),
            gr.Option(gr.Union(gr.Int(), gr.Float())),
            gr.Union(gr.Option(gr.String()), gr.Bool()),
        ]

        schema = gr.Schema(
            ungrouped=gr.GroupSchema(
                nodes={
                    f"attribute_{index}": gr.AttributeDataType(
                        data_type, gr.AttributeType.infer(data_type)
                    )
                    for index, data_type in enumerate(datatypes)
                },
                edges={
                    f"attribute_{index}": gr.AttributeDataType(
                        data_type, gr.AttributeType.infer(data_type)
                    )
                    for index, data_type in enumerate(datatypes)
                },
            )
        )
        graphrecord = gr.GraphRecord.with_schema(schema)

        node_schema = graphrecord.schema.ungrouped.nodes
        edge_schema = graphrecord.schema.ungrouped.edges

        for index, data_type in enumerate(datatypes):
            attribute_name = f"attribute_{index}"

            assert node_schema[attribute_name].data_type == data_type
            assert edge_schema[attribute_name].data_type == data_type


if __name__ == "__main__":
    run_test = unittest.TestLoader().loadTestsFromTestCase(TestDataType)
    unittest.TextTestRunner(verbosity=2).run(run_test)
