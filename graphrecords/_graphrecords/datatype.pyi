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
    dtype1: PyDataType
    dtype2: PyDataType

    def __init__(self, dtype1: PyDataType, dtype2: PyDataType) -> None: ...

class PyOption:
    dtype: PyDataType

    def __init__(self, dtype: PyDataType) -> None: ...
