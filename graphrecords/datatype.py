"""GraphRecord-associated data types."""

from __future__ import annotations

import typing
from abc import ABC, abstractmethod
from typing import Generic, TypeAlias, TypeVar

from graphrecords._graphrecords.datatype import (
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

PyDataType: TypeAlias = typing.Union[
    PyString,
    PyInt,
    PyFloat,
    PyBool,
    PyDateTime,
    PyDuration,
    PyNull,
    PyAny,
    PyUnion,
    PyOption,
]


class DataType(ABC):
    """Abstract class for data types."""

    @abstractmethod
    def _inner(self) -> PyDataType: ...

    @abstractmethod
    def __str__(self) -> str:
        """Returns a user-friendly string representation of the data type."""
        ...

    @abstractmethod
    def __repr__(self) -> str:
        """Returns the string representation of the data type."""
        ...

    @abstractmethod
    def __eq__(self, value: object) -> bool:
        """Checks if the data type is equal to another data type."""
        ...

    @staticmethod
    def _from_py_data_type(py_datatype: PyDataType) -> DataType:
        """Converts a PyDataType to a DataType.

        Args:
            py_datatype (PyDataType): The PyDataType to convert.

        Returns:
            DataType: The converted DataType.
        """
        if isinstance(py_datatype, PyString):
            return String()
        if isinstance(py_datatype, PyInt):
            return Int()
        if isinstance(py_datatype, PyFloat):
            return Float()
        if isinstance(py_datatype, PyBool):
            return Bool()
        if isinstance(py_datatype, PyDateTime):
            return DateTime()
        if isinstance(py_datatype, PyDuration):
            return Duration()
        if isinstance(py_datatype, PyNull):
            return Null()
        if isinstance(py_datatype, PyAny):
            return Any()
        if isinstance(py_datatype, PyUnion):
            return Union(
                DataType._from_py_data_type(py_datatype.left),
                DataType._from_py_data_type(py_datatype.right),
            )
        return Option(DataType._from_py_data_type(py_datatype.datatype))


class String(DataType):
    """Data type for strings."""

    _string: PyString

    def __init__(self) -> None:
        """Initializes the String data type."""
        self._string = PyString()

    def _inner(self) -> PyDataType:
        return self._string

    def __str__(self) -> str:
        """Returns a user-friendly string representation of the data type."""
        return "String"

    def __repr__(self) -> str:
        """Returns the string representation of the data type."""
        return "DataType.String"

    def __eq__(self, value: object) -> bool:
        """Checks if the data type of the value is equal to this data type.

        Args:
            value (object): The value to compare.

        Returns:
            bool: True if the data type is equal to this data type, otherwise
                False.
        """
        return isinstance(value, String)


class Int(DataType):
    """Data type for integers."""

    _int: PyInt

    def __init__(self) -> None:
        """Initializes the Int data type."""
        self._int = PyInt()

    def _inner(self) -> PyDataType:
        return self._int

    def __str__(self) -> str:
        """Returns a user-friendly string representation of the data type."""
        return "Int"

    def __repr__(self) -> str:
        """Returns the string representation of the data type."""
        return "DataType.Int"

    def __eq__(self, value: object) -> bool:
        """Checks if the data type of the value is equal to this data type.

        Args:
            value (object): The value to compare.

        Returns:
            bool: True if the data type is equal to this data type, otherwise
                False.
        """
        return isinstance(value, Int)


class Float(DataType):
    """Data type for floating-point numbers."""

    _float: PyFloat

    def __init__(self) -> None:
        """Initializes the Float data type."""
        self._float = PyFloat()

    def _inner(self) -> PyDataType:
        return self._float

    def __str__(self) -> str:
        """Returns a user-friendly string representation of the data type."""
        return "Float"

    def __repr__(self) -> str:
        """Returns the string representation of the data type."""
        return "DataType.Float"

    def __eq__(self, value: object) -> bool:
        """Checks if the data type of the value is equal to this data type.

        Args:
            value (object): The value to compare.

        Returns:
            bool: True if the data type is equal to this data type, otherwise
                False.
        """
        return isinstance(value, Float)


class Bool(DataType):
    """Data type for boolean values."""

    _bool: PyBool

    def __init__(self) -> None:
        """Initializes the Bool data type."""
        self._bool = PyBool()

    def _inner(self) -> PyDataType:
        return self._bool

    def __str__(self) -> str:
        """Returns a user-friendly string representation of the data type."""
        return "Bool"

    def __repr__(self) -> str:
        """Returns the string representation of the data type."""
        return "DataType.Bool"

    def __eq__(self, value: object) -> bool:
        """Checks if the data type of the value is equal to this data type.

        Args:
            value (object): The value to compare.

        Returns:
            bool: True if the data type is equal to this data type, otherwise
                False.
        """
        return isinstance(value, Bool)


class DateTime(DataType):
    """Data type for date and time values."""

    _datetime: PyDateTime

    def __init__(self) -> None:
        """Initializes the DateTime data type."""
        self._datetime = PyDateTime()

    def _inner(self) -> PyDataType:
        return self._datetime

    def __str__(self) -> str:
        """Returns a user-friendly string representation of the data type."""
        return "DateTime"

    def __repr__(self) -> str:
        """Returns the string representation of the data type."""
        return "DataType.DateTime"

    def __eq__(self, value: object) -> bool:
        """Checks if the data type of the value is equal to this data type.

        Args:
            value (object): The value to compare.

        Returns:
            bool: True if the data type is equal to this data type, otherwise
                False.
        """
        return isinstance(value, DateTime)


class Duration(DataType):
    """Data type for duration (timedelta)."""

    _duration: PyDuration

    def __init__(self) -> None:
        """Initializes the Duration data type."""
        self._duration = PyDuration()

    def _inner(self) -> PyDataType:
        return self._duration

    def __str__(self) -> str:
        """Returns a user-friendly string representation of the data type."""
        return "Duration"

    def __repr__(self) -> str:
        """Returns the string representation of the data type."""
        return "DataType.Duration"

    def __eq__(self, value: object) -> bool:
        """Checks if the data type of the value is equal to this data type.

        Args:
            value (object): The value to compare.

        Returns:
            bool: True if the data type is equal to this data type, otherwise
                False.
        """
        return isinstance(value, Duration)


class Null(DataType):
    """Data type for null values."""

    _null: PyNull

    def __init__(self) -> None:
        """Initializes the Null data type."""
        self._null = PyNull()

    def _inner(self) -> PyDataType:
        return self._null

    def __str__(self) -> str:
        """Returns a user-friendly string representation of the data type."""
        return "Null"

    def __repr__(self) -> str:
        """Returns the string representation of the data type."""
        return "DataType.Null"

    def __eq__(self, value: object) -> bool:
        """Checks if the data type of the value is equal to this data type.

        Args:
            value (object): The value to compare.

        Returns:
            bool: True if the data type is equal to this data type, otherwise
                False.
        """
        return isinstance(value, Null)


class Any(DataType):
    """Data type for any values."""

    _any: PyAny

    def __init__(self) -> None:
        """Initializes the Any data type."""
        self._any = PyAny()

    def _inner(self) -> PyDataType:
        return self._any

    def __str__(self) -> str:
        """Returns a user-friendly string representation of the data type."""
        return "Any"

    def __repr__(self) -> str:
        """Returns the string representation of the data type."""
        return "DataType.Any"

    def __eq__(self, value: object) -> bool:
        """Checks if the data type of the value is equal to this data type.

        Args:
            value (object): The value to compare.

        Returns:
            bool: True if the data type is equal to this data type, otherwise
                False.
        """
        return isinstance(value, Any)


U1 = TypeVar("U1", bound=DataType)
U2 = TypeVar("U2", bound=DataType)


class Union(DataType, Generic[U1, U2]):
    """Data type for unions of data types."""

    _union: PyUnion

    def __init__(self, left: U1, right: U2) -> None:
        """Initializes the Union data type.

        Args:
            left (U1): The first data type of the union.
            right (U2): The second data type of the union.
        """
        self._union = PyUnion(left._inner(), right._inner())

    def _inner(self) -> PyDataType:
        return self._union

    @property
    def left(self) -> DataType:
        """The first data type of the union.

        Returns:
            DataType: The first data type of the union.
        """
        return DataType._from_py_data_type(self._union.left)

    @property
    def right(self) -> DataType:
        """The second data type of the union.

        Returns:
            DataType: The second data type of the union.
        """
        return DataType._from_py_data_type(self._union.right)

    def __str__(self) -> str:
        """Returns a user-friendly string representation of the data type."""
        return f"Union({self.left}, {self.right})"

    def __repr__(self) -> str:
        """Returns the string representation of the data type."""
        return f"DataType.Union({self.left!r}, {self.right!r})"

    def __eq__(self, value: object) -> bool:
        """Checks if the data type of the value is equal to this data type.

        Args:
            value (object): The value to compare.

        Returns:
            bool: True if the data type is equal to this data type, otherwise
                False.
        """
        if not isinstance(value, Union):
            return False

        return (self.left == value.left and self.right == value.right) or (
            self.left == value.right and self.right == value.left
        )


T = TypeVar("T", bound=DataType)


class Option(DataType, Generic[T]):
    """Data type for optional values."""

    _option: PyOption

    def __init__(self, datatype: T) -> None:
        """Initializes the Option data type.

        Args:
            datatype (T): The data type of the optional value.
        """
        self._option = PyOption(datatype._inner())

    def _inner(self) -> PyDataType:
        return self._option

    @property
    def datatype(self) -> DataType:
        """The data type of the optional value.

        Returns:
            DataType: The data type of the optional value.
        """
        return DataType._from_py_data_type(self._option.datatype)

    def __str__(self) -> str:
        """Returns a user-friendly string representation of the data type."""
        return f"Option({self.datatype})"

    def __repr__(self) -> str:
        """Returns the string representation of the data type."""
        return f"DataType.Option({self.datatype!r})"

    def __eq__(self, value: object) -> bool:
        """Checks if the data type of the value is equal to this data type.

        Args:
            value (object): The value to compare.

        Returns:
            bool: True if the data type is equal to this data type, otherwise
                False.
        """
        return isinstance(value, Option) and self.datatype == value.datatype
