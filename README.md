# PyDolphinDB

A C++ boosted DolphinDB Python3 API based on [Pybind11](https://github.com/pybind/pybind11), [DolphinDB Cpp API](https://github.com/dolphindb/api-cplusplus) and [DolphinDB Python3 API](https://github.com/dolphindb/api-python3)

## Core features

- Connect to the DolphinDB server
- Login with encrption
- Run scripts and fetch the results
- RPC
- Upload supported Python objects
- Streaming

A detailed tutorial (`pydolphindb/doc/tutorial.md`, still in progress) about:

- Develop a Python C++ extension via pybind11
- Use DolphinDB C++ API to build the C++ extension
- Package your Python module and upload it to PyPi

## Fast data conversion

pydolphindb can map the [DolphinDB data structures](https://www.dolphindb.com/help/) to Python builtins

```
Python          DolphinDB
NoneType        VOID
bool            BOOL
int             LONG
float           DOUBLE
str             STRING
set             SET
tuple           VECTOR
list            VECTOR
dict            DICTIONARY
```

For efficiency and accuracy, pydolphindb support data conversions between numpy(`array`)/pandas(`DataFrame`) and DolphinDB

```
numpy dtype     DolphinDB
bool            BOOL
int8            CHAR
int16           SHORT
int32           INT
int64           LONG
float32         FLOAT
float64         DOUBLE
object          STRING or ANY (based on type inference)
datetime64[D]   DATE
datetime64[M]   MONTH
datetime64[ms]  TIMESTAMP
datetime64[m]   MINUTE
datetime64[s]   DATETIME
datetime64[ns]  NANOTIMESTAMP

pandas
DataFrame       TABLE
```

**pandas does not support datetime64 otherthan datetime64[ns]**

## Build

1.prerequisite

- CMake
- Python
- OpenSSL 1.0.2

2.start build in the `pydolphindb` folder

```
cmake . -DPYTHON_EXECUTABLE=/path/to/Python -DOPENSSL_LIB_PATH=/path/to/openssl-1.0.2/lib -DCMAKE_BUILD_TYPE=Release

make

make install
```

3.use `pydolphindb/pydolphindb` as a Python module folder

**installing pydolphindb via `setup.py` will be supported later**

## Tested compilers

1.GCC 4.8.5 or newer

2.Cygwin/GCC

## Tested Python

Python 3.4~3.7

Python 2.7 with minor modifications

## Quick start

run the following script in Python3 after installation

```
import pydolphindb as pydb
s = pydb.session()
isSuccess = s.connect(DOLPHINDB_IP, DOLPHINDB_PORT)
if isSuccess:
    s.run("1..100")
```

## About

This project was created by JasonYuchen(jasonyuchen@foxmail.com).

### License

pydolphindb is provided under MIT license that can be found in the `LICENSE` file.

[pybind11 license](https://github.com/pybind/pybind11/blob/master/LICENSE)

[DolphinDB Cpp API license](https://github.com/dolphindb/api-cplusplus/blob/master/LICENSE)

[DolphinDB Python3 API license](https://github.com/dolphindb/api-python3/blob/master/LICENSE)
