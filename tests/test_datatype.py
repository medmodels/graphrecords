import unittest

import graphrecords as gr
from graphrecords._graphrecords import (
    PyAny,
    PyBool,
    PyDateTime,
    PyDuration,
    PyFloat,
    PyInt,
    PyNull,
    PyOption,
    PyString,
    PyUnion,
)
from graphrecords.datatype import DataType


class TestDataType(unittest.TestCase):
    def test_from_py_data_type(self) -> None:
        py_string = PyString()
        result = DataType._from_py_data_type(py_string)
        assert isinstance(result, gr.String)

        py_int = PyInt()
        result = DataType._from_py_data_type(py_int)
        assert isinstance(result, gr.Int)

        py_float = PyFloat()
        result = DataType._from_py_data_type(py_float)
        assert isinstance(result, gr.Float)

        py_bool = PyBool()
        result = DataType._from_py_data_type(py_bool)
        assert isinstance(result, gr.Bool)

        py_datetime = PyDateTime()
        result = DataType._from_py_data_type(py_datetime)
        assert isinstance(result, gr.DateTime)

        py_duration = PyDuration()
        result = DataType._from_py_data_type(py_duration)
        assert isinstance(result, gr.Duration)

        py_null = PyNull()
        result = DataType._from_py_data_type(py_null)
        assert isinstance(result, gr.Null)

        py_any = PyAny()
        result = DataType._from_py_data_type(py_any)
        assert isinstance(result, gr.Any)

        py_union = PyUnion(PyString(), PyInt())
        result = DataType._from_py_data_type(py_union)
        assert isinstance(result, gr.Union)
        assert result == gr.Union(gr.String(), gr.Int())

        nested_py_union = PyUnion(PyString(), PyUnion(PyInt(), PyBool()))
        result = DataType._from_py_data_type(nested_py_union)
        assert isinstance(result, gr.Union)
        assert result == gr.Union(gr.String(), gr.Union(gr.Int(), gr.Bool()))

        py_option = PyOption(PyString())
        result = DataType._from_py_data_type(py_option)
        assert isinstance(result, gr.Option)
        assert result == gr.Option(gr.String())

    def test_string(self) -> None:
        string = gr.String()
        assert isinstance(string._inner(), PyString)

        assert str(string) == "String"

        assert string.__repr__() == "DataType.String"

        assert gr.String() == gr.String()
        assert gr.String() != gr.Int()

    def test_int(self) -> None:
        integer = gr.Int()
        assert isinstance(integer._inner(), PyInt)

        assert str(integer) == "Int"

        assert integer.__repr__() == "DataType.Int"

        assert gr.Int() == gr.Int()
        assert gr.Int() != gr.String()

    def test_float(self) -> None:
        float = gr.Float()
        assert isinstance(float._inner(), PyFloat)

        assert str(float) == "Float"

        assert float.__repr__() == "DataType.Float"

        assert gr.Float() == gr.Float()
        assert gr.Float() != gr.String()

    def test_bool(self) -> None:
        bool = gr.Bool()
        assert isinstance(bool._inner(), PyBool)

        assert str(bool) == "Bool"

        assert bool.__repr__() == "DataType.Bool"

        assert gr.Bool() == gr.Bool()
        assert gr.Bool() != gr.String()

    def test_datetime(self) -> None:
        datetime = gr.DateTime()
        assert isinstance(datetime._inner(), PyDateTime)

        assert str(datetime) == "DateTime"

        assert datetime.__repr__() == "DataType.DateTime"

        assert gr.DateTime() == gr.DateTime()
        assert gr.DateTime() != gr.String()

    def test_duration(self) -> None:
        duration = gr.Duration()
        assert isinstance(duration._inner(), PyDuration)

        assert str(duration) == "Duration"

        assert duration.__repr__() == "DataType.Duration"

        assert gr.Duration() == gr.Duration()
        assert gr.Duration() != gr.String()

    def test_null(self) -> None:
        null = gr.Null()
        assert isinstance(null._inner(), PyNull)

        assert str(null) == "Null"

        assert null.__repr__() == "DataType.Null"

        assert gr.Null() == gr.Null()
        assert gr.Null() != gr.String()

    def test_any(self) -> None:
        any = gr.Any()
        assert isinstance(any._inner(), PyAny)

        assert str(any) == "Any"

        assert any.__repr__() == "DataType.Any"

        assert gr.Any() == gr.Any()
        assert gr.Any() != gr.String()

    def test_union(self) -> None:
        union = gr.Union(gr.String(), gr.Int())
        assert isinstance(union._inner(), PyUnion)

        assert str(union) == "Union(String, Int)"

        assert union.__repr__() == "DataType.Union(DataType.String, DataType.Int)"

        union = gr.Union(gr.String(), gr.Union(gr.Int(), gr.Bool()))
        assert isinstance(union._inner(), PyUnion)

        assert str(union) == "Union(String, Union(Int, Bool))"

        assert (
            union.__repr__()
            == "DataType.Union(DataType.String, DataType.Union(DataType.Int, DataType.Bool))"
        )

        assert gr.Union(gr.String(), gr.Int()) == gr.Union(gr.String(), gr.Int())
        assert gr.Union(gr.String(), gr.Int()) != gr.Union(gr.Int(), gr.String())

    def test_option(self) -> None:
        option = gr.Option(gr.String())
        assert isinstance(option._inner(), PyOption)

        assert str(option) == "Option(String)"

        assert option.__repr__() == "DataType.Option(DataType.String)"

        assert gr.Option(gr.String()) == gr.Option(gr.String())
        assert gr.Option(gr.String()) != gr.Option(gr.Int())


if __name__ == "__main__":
    run_test = unittest.TestLoader().loadTestsFromTestCase(TestDataType)
    unittest.TextTestRunner(verbosity=2).run(run_test)
