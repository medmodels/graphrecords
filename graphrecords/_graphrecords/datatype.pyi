from typing import TypeAlias, Union

PyDataType: TypeAlias = Union[
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

class PyString: ...
class PyInt: ...
class PyFloat: ...
class PyBool: ...
class PyDateTime: ...
class PyDuration: ...
class PyNull: ...
class PyAny: ...

class PyUnion:
    left: PyDataType
    right: PyDataType

    def __init__(self, left: PyDataType, right: PyDataType) -> None: ...

class PyOption:
    datatype: PyDataType

    def __init__(self, datatype: PyDataType) -> None: ...
