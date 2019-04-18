#include <pybind11/numpy.h>

#include <DolphinDB.h>
#include <Util.h>

#include "Utils.h"

#if defined(__GNUC__) && __GNUC__ >= 4
#define LIKELY(x) (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
#define LIKELY(x) (x)
#define UNLIKELY(x) (x)
#endif

namespace pydolphindb {

namespace pymodule {
const module numpy_ = module::import("numpy");
const module pandas_ = module::import("pandas");
}  // namespace pymodule

namespace pyfunction {
const handle isnan_ = pymodule::numpy_.attr("isnan");
const handle sum_ = pymodule::numpy_.attr("sum");
}  // namespace pyfunction

namespace pytype {
const handle datetime64_ = pymodule::numpy_.attr("datetime64");
const handle pddataframe_ = pymodule::pandas_.attr("DataFrame")().get_type().inc_ref();
const handle nparray_ = py::array().get_type().inc_ref();
const handle npbool_ = py::dtype("bool").inc_ref();
const handle npint8_ = py::dtype("int8").inc_ref();
const handle npint16_ = py::dtype("int16").inc_ref();
const handle npint32_ = py::dtype("int32").inc_ref();
const handle npint64_ = py::dtype("int64").inc_ref();
const handle npfloat32_ = py::dtype("float32").inc_ref();
const handle npfloat64_ = py::dtype("float64").inc_ref();
const handle npdatetime64M_ = py::dtype("datetime64[M]").inc_ref();
const handle npdatetime64D_ = py::dtype("datetime64[D]").inc_ref();
const handle npdatetime64m_ = py::dtype("datetime64[m]").inc_ref();
const handle npdatetime64s_ = py::dtype("datetime64[s]").inc_ref();
const handle npdatetime64ms_ = py::dtype("datetime64[ms]").inc_ref();
const handle npdatetime64ns_ = py::dtype("datetime64[ns]").inc_ref();
const handle npdatetime64_ = py::dtype("datetime64").inc_ref();
const handle npobject_ = py::dtype("object").inc_ref();
const handle pynone_ = py::none().get_type().inc_ref();
const handle pybool_ = py::bool_().get_type().inc_ref();
const handle pyint_ = py::int_().get_type().inc_ref();
const handle pyfloat_ = py::float_().get_type().inc_ref();
const handle pystr_ = py::str().get_type().inc_ref();
const handle pybytes_ = py::bytes().get_type().inc_ref();
const handle pyset_ = py::set().get_type().inc_ref();
const handle pytuple_ = py::tuple().get_type().inc_ref();
const handle pylist_ = py::list().get_type().inc_ref();
const handle pydict_ = py::dict().get_type().inc_ref();
}  // namespace pytype

namespace utils {
std::string DataCategoryToString(ddb::DATA_CATEGORY cate) noexcept {
    switch (cate) {
        case ddb::NOTHING:
            return "NOTHING";
        case ddb::LOGICAL:
            return "LOGICAL";
        case ddb::INTEGRAL:
            return "INTEGRAL";
        case ddb::FLOATING:
            return "FLOATING";
        case ddb::TEMPORAL:
            return "TEMPORAL";
        case ddb::LITERAL:
            return "LITERAL";
        case ddb::SYSTEM:
            return "SYSTEM";
        case ddb::MIXED:
            return "MIXED";
        default:
            return "UNRECOGNIZED CATEGORY " + std::to_string(cate);
    }
}

std::string DataFormToString(ddb::DATA_FORM form) noexcept {
    switch (form) {
        case ddb::DF_SCALAR:
            return "SCALAR";
        case ddb::DF_VECTOR:
            return "VECTOR";
        case ddb::DF_PAIR:
            return "PAIR";
        case ddb::DF_MATRIX:
            return "MATRIX";
        case ddb::DF_SET:
            return "SET";
        case ddb::DF_DICTIONARY:
            return "DICTIONARY";
        case ddb::DF_TABLE:
            return "TABLE";
        case ddb::DF_CHART:
            return "CHART";
        case ddb::DF_CHUNK:
            return "CHUNK";
        default:
            return "UNRECOGNIZED FORM " + std::to_string(form);
    }
}

std::string DataTypeToString(ddb::DATA_TYPE type) noexcept {
    switch (type) {
        case ddb::DT_VOID:
            return "VOID";
        case ddb::DT_BOOL:
            return "BOOL";
        case ddb::DT_CHAR:
            return "CHAR";
        case ddb::DT_SHORT:
            return "SHORT";
        case ddb::DT_INT:
            return "INT";
        case ddb::DT_LONG:
            return "LONG";
        case ddb::DT_DATE:
            return "DATE";
        case ddb::DT_MONTH:
            return "MONTH";
        case ddb::DT_TIME:
            return "TIME";
        case ddb::DT_MINUTE:
            return "MINUTE";
        case ddb::DT_SECOND:
            return "SECOND";
        case ddb::DT_DATETIME:
            return "DATETIME";
        case ddb::DT_TIMESTAMP:
            return "TIMESTAMP";
        case ddb::DT_NANOTIME:
            return "NANOTIME";
        case ddb::DT_NANOTIMESTAMP:
            return "NANOTIMESTAMP";
        case ddb::DT_FLOAT:
            return "FLOAT";
        case ddb::DT_DOUBLE:
            return "DOUBLE";
        case ddb::DT_SYMBOL:
            return "SYMBOL";
        case ddb::DT_STRING:
            return "STRING";
        case ddb::DT_UUID:
            return "UUID";
        case ddb::DT_FUNCTIONDEF:
            return "FUNCTIONDEF";
        case ddb::DT_HANDLE:
            return "HANDLE";
        case ddb::DT_CODE:
            return "CODE";
        case ddb::DT_DATASOURCE:
            return "DATASOURCE";
        case ddb::DT_RESOURCE:
            return "RESOURCE";
        case ddb::DT_ANY:
            return "ANY";
        case ddb::DT_COMPRESS:
            return "COMPRESS";
        case ddb::DT_DICTIONARY:
            return "DICTIONARY";
        case ddb::DT_OBJECT:
            return "OBJECT";
        default:
            return "UNRECOGNIZED TYPE " + std::to_string(type);
    }
}

inline void SET_NPNAN(void *p, size_t len) {
    std::fill(reinterpret_cast<uint64_t*>(p),
              reinterpret_cast<uint64_t*>(p)+len,
              9221120237041090560LL);
}

inline void SET_DDBNAN(void *p, size_t len) {
    std::fill(reinterpret_cast<double*>(p),
              reinterpret_cast<double*>(p)+len,
              ddb::DBL_NMIN);
}

inline bool IS_NPNAN(void *p) {
    return *reinterpret_cast<uint64_t*>(p) == 9221120237041090560LL;
}

ddb::DATA_TYPE DataTypeFromNumpyArray(py::array array) {
    py::dtype type = array.dtype();
    if (type.equal(pytype::npbool_))
        return ddb::DT_BOOL;
    else if (type.equal(pytype::npint8_))
        return ddb::DT_CHAR;
    else if (type.equal(pytype::npint16_))
        return ddb::DT_SHORT;
    else if (type.equal(pytype::npint32_))
        return ddb::DT_INT;
    else if (type.equal(pytype::npint64_))
        return ddb::DT_LONG;
    else if (type.equal(pytype::npfloat32_))
        return ddb::DT_FLOAT;
    else if (type.equal(pytype::npfloat64_))
        return ddb::DT_DOUBLE;
    else if (type.equal(pytype::npdatetime64M_))
        return ddb::DT_MONTH;
    else if (type.equal(pytype::npdatetime64D_))
        return ddb::DT_DATE;
    else if (type.equal(pytype::npdatetime64m_))
        return ddb::DT_MINUTE;
    else if (type.equal(pytype::npdatetime64s_))
        return ddb::DT_SECOND;
    else if (type.equal(pytype::npdatetime64s_))
        return ddb::DT_DATETIME;
    else if (type.equal(pytype::npdatetime64ms_))
        return ddb::DT_TIMESTAMP;
    else if (type.equal(pytype::npdatetime64ms_))
        return ddb::DT_TIME;
    else if (type.equal(pytype::npdatetime64ns_))
        return ddb::DT_NANOTIMESTAMP;
    else if (type.equal(pytype::npdatetime64_))  // np.array of null datetime64
        return ddb::DT_NANOTIMESTAMP;
    else if (type.equal(pytype::npobject_))
        return ddb::DT_ANY;
    else
        return ddb::DT_ANY;
}


py::object toPython(ddb::ConstantSP obj, void (*nullValuePolicyForVector)(ddb::VectorSP)) {
    if (obj.isNull() || obj->isNothing() || obj->isNull()) {
        return py::none();
    }
    ddb::DATA_TYPE type = obj->getType();
    ddb::DATA_FORM form = obj->getForm();
    if (form == ddb::DF_VECTOR) {
        ddb::VectorSP ddbVec = obj;
        nullValuePolicyForVector(ddbVec);
        size_t size = ddbVec->size();
        switch (type) {
            case ddb::DT_VOID:
            {
                py::array pyVec;
                pyVec.resize({size});
                return pyVec;
            }
            case ddb::DT_BOOL:
            {
                py::array pyVec(py::dtype("bool"), {size}, {});
                ddbVec->getBool(0, size, reinterpret_cast<char*>(pyVec.mutable_data()));
                if (UNLIKELY(ddbVec->hasNull())) {
                    // Play with the raw api of Python, be careful about the ref count
                    pyVec = pyVec.attr("astype")("object");
                    PyObject **p = reinterpret_cast<PyObject**>(pyVec.mutable_data());
                    for (size_t i = 0; i < size; ++i) {
                        if (UNLIKELY(ddbVec->getBool(i) == INT8_MIN)) {
                            Py_DECREF(p[i]);
                            p[i] = pymodule::numpy_.attr("nan").ptr();
                        }
                    }
                }
                return pyVec;
            }
            case ddb::DT_CHAR:
            {
                py::array pyVec(py::dtype("int8"), {size}, {});
                ddbVec->getChar(0, size, reinterpret_cast<char *>(pyVec.mutable_data()));
                if (UNLIKELY(ddbVec->hasNull())) {
                    pyVec = pyVec.attr("astype")("float64");
                    double *p = reinterpret_cast<double *>(pyVec.mutable_data());
                    for (size_t i = 0; i < size; ++i) {
                        if (UNLIKELY(ddbVec->getChar(i) == INT8_MIN)) {
                            utils::SET_NPNAN(p+i, 1);
                        }
                    }
                }
                return pyVec;
            }
            case ddb::DT_SHORT:
            {
                py::array pyVec(py::dtype("int16"), {size}, {});
                ddbVec->getShort(0, size, reinterpret_cast<short *>(pyVec.mutable_data()));
                if (UNLIKELY(ddbVec->hasNull())) {
                    pyVec = pyVec.attr("astype")("float64");
                    double *p = reinterpret_cast<double *>(pyVec.mutable_data());
                    for (size_t i = 0; i < size; ++i) {
                        if (UNLIKELY(ddbVec->getShort(i) == INT16_MIN)) {
                            utils::SET_NPNAN(p+i, 1);
                        }
                    }
                }
                return pyVec;
            }
            case ddb::DT_INT:
            {
                py::array pyVec(py::dtype("int32"), {size}, {});
                ddbVec->getInt(0, size, reinterpret_cast<int*>(pyVec.mutable_data()));
                if (UNLIKELY(ddbVec->hasNull())) {
                    pyVec = pyVec.attr("astype")("float64");
                    double *p = reinterpret_cast<double*>(pyVec.mutable_data());
                    for (size_t i = 0; i < size; ++i) {
                        if (UNLIKELY(ddbVec->getInt(i) == INT32_MIN)) {
                            utils::SET_NPNAN(p+i, 1);
                        }
                    }
                }
                return pyVec;
            }
            case ddb::DT_LONG:
            {
                py::array pyVec(py::dtype("int64"), {size}, {});
                ddbVec->getLong(0, size, reinterpret_cast<long long*>(pyVec.mutable_data()));
                if (UNLIKELY(ddbVec->hasNull())) {
                    pyVec = pyVec.attr("astype")("float64");
                    double *p = reinterpret_cast<double*>(pyVec.mutable_data());
                    for (size_t i = 0; i < size; ++i) {
                        if (UNLIKELY(ddbVec->getLong(i) == INT64_MIN)) {
                            utils::SET_NPNAN(p+i, 1);
                        }
                    }
                }
                return pyVec;
            }
            case ddb::DT_DATE:
            {
                py::array pyVec(py::dtype("datetime64[D]"), {size}, {});
                ddbVec->getLong(0, size, reinterpret_cast<long long*>(pyVec.mutable_data()));
                return pyVec;
            }
            case ddb::DT_MONTH:
            {
                py::array pyVec(py::dtype("datetime64[M]"), {size}, {});
                ddbVec->getLong(0, size, reinterpret_cast<long long*>(pyVec.mutable_data()));
                long long *p = reinterpret_cast<long long*>(pyVec.mutable_data());
                for (size_t i = 0; i < size; ++i) {
                    if (UNLIKELY(p[i] == INT64_MIN)) {
                        continue;
                    }
                    p[i] -= 1970*12;
                }
                return pyVec;
            }
            case ddb::DT_TIME:
            {
                py::array pyVec(py::dtype("datetime64[ms]"), {size}, {});
                ddbVec->getLong(0, size, reinterpret_cast<long long*>(pyVec.mutable_data()));
                return pyVec;
            }
            case ddb::DT_MINUTE:
            {
                py::array pyVec(py::dtype("datetime64[m]"), {size}, {});
                ddbVec->getLong(0, size, reinterpret_cast<long long*>(pyVec.mutable_data()));
                return pyVec;
            }
            case ddb::DT_SECOND:
            {
                py::array pyVec(py::dtype("datetime64[s]"), {size}, {});
                ddbVec->getLong(0, size, reinterpret_cast<long long*>(pyVec.mutable_data()));
                return pyVec;
            }
            case ddb::DT_DATETIME:
            {
                py::array pyVec(py::dtype("datetime64[s]"), {size}, {});
                ddbVec->getLong(0, size, reinterpret_cast<long long*>(pyVec.mutable_data()));
                return pyVec;
            }
            case ddb::DT_TIMESTAMP:
            {
                py::array pyVec(py::dtype("datetime64[ms]"), {size}, {});
                ddbVec->getLong(0, size, reinterpret_cast<long long*>(pyVec.mutable_data()));
                return pyVec;
            }
            case ddb::DT_NANOTIME:
            {
                py::array pyVec(py::dtype("datetime64[ns]"), {size}, {});
                ddbVec->getLong(0, size, reinterpret_cast<long long*>(pyVec.mutable_data()));
                return pyVec;
            }
            case ddb::DT_NANOTIMESTAMP:
            {
                py::array pyVec(py::dtype("datetime64[ns]"), {size}, {});
                ddbVec->getLong(0, size, reinterpret_cast<long long*>(pyVec.mutable_data()));
                return pyVec;
            }
            case ddb::DT_FLOAT:
            {
                py::array pyVec(py::dtype("float32"), {size}, {});
                ddbVec->getFloat(0, size, reinterpret_cast<float*>(pyVec.mutable_data()));
                if (UNLIKELY(ddbVec->hasNull())) {
                    pyVec = pyVec.attr("astype")("float64");
                    auto p = reinterpret_cast<double*>(pyVec.mutable_data());
                    for (size_t i = 0; i < size; ++i) {
                        if (UNLIKELY(ddbVec->getFloat(i) == ddb::FLT_NMIN)) {
                            utils::SET_NPNAN(p+i, 1);
                        }
                    }
                }
                return pyVec;
            }
            case ddb::DT_DOUBLE:
            {
                py::array pyVec(py::dtype("float64"), {size}, {});
                ddbVec->getDouble(0, size, reinterpret_cast<double*>(pyVec.mutable_data()));
                if (UNLIKELY(ddbVec->hasNull())) {
                    auto p = reinterpret_cast<double*>(pyVec.mutable_data());
                    for (size_t i = 0; i < size; ++i) {
                        if (UNLIKELY(ddbVec->getDouble(i) == ddb::DBL_NMIN)) {
                            utils::SET_NPNAN(p+i, 1);
                        }
                    }
                }
                return pyVec;
            }
            case ddb::DT_SYMBOL:
            case ddb::DT_STRING:
            {
                // handle numpy.array of symbols/strings
                auto* objects = new PyObject*[size];
                py::capsule deleter(objects, [](void *pp) { delete[] reinterpret_cast<PyObject*>(pp);});
                for (size_t i = 0; i < size; ++i) {
                    objects[i] = py::str(ddbVec->getString(i)).inc_ref().ptr();
                }
                return py::array(py::dtype("object"), {size}, {}, objects, deleter);
            }
            case ddb::DT_ANY:
            {
                // handle numpy.array of objects
                auto* objects = new PyObject*[size];
                py::capsule deleter(objects, [](void *pp) { delete[] reinterpret_cast<PyObject*>(pp);});
                for (size_t i = 0; i < size; ++i) {
                    objects[i] = toPython(ddbVec->get(i)).inc_ref().ptr();
                }
                return py::array(py::dtype("object"), {size}, {}, objects, deleter);
            }
            default:
            {
                throw std::runtime_error("type error in Vector: " + utils::DataTypeToString(type));
            };
        }
    } else if (form == ddb::DF_TABLE) {
        ddb::TableSP ddbTbl = obj;
        size_t columnSize = ddbTbl->columns();
        py::dict columns;
        py::dict columnTypes;
        vector<ddb::ConstantSP> ddbColumns;
        for (size_t i = 0; i < columnSize; ++i) {
            ddb::ConstantSP col = obj->getColumn(i);
            columns[ddbTbl->getColumnName(i).data()] = toPython(col);
            assert(py::isinstance(columns[ddbTbl->getColumnName(i).data()], py::array().get_type()));
        }
        py::object dataframe = pymodule::pandas_.attr("DataFrame")(columns);
        return dataframe;
    } else if (form == ddb::DF_SCALAR) {
        switch (type) {
            case ddb::DT_VOID:
                return py::none();
            case ddb::DT_BOOL:
                return py::bool_(obj->getBool());
            case ddb::DT_CHAR:
            case ddb::DT_SHORT:
            case ddb::DT_INT:
            case ddb::DT_LONG:
                return py::int_(obj->getLong());
            case ddb::DT_DATE:
                return pytype::datetime64_(obj->getLong(), "D");
            case ddb::DT_MONTH:
                return pytype::datetime64_(obj->getLong(), "M");
            case ddb::DT_TIME:
                return pytype::datetime64_(obj->getLong(), "ms");
            case ddb::DT_MINUTE:
                return pytype::datetime64_(obj->getLong(), "m");
            case ddb::DT_SECOND:
                return pytype::datetime64_(obj->getLong(), "s");
            case ddb::DT_DATETIME:
                return pytype::datetime64_(obj->getLong(), "s");
            case ddb::DT_TIMESTAMP:
                return pytype::datetime64_(obj->getLong(), "ms");
            case ddb::DT_NANOTIME:
                return pytype::datetime64_(obj->getLong(), "ns");
            case ddb::DT_NANOTIMESTAMP:
                return pytype::datetime64_(obj->getLong(), "ns");
            case ddb::DT_FLOAT:
            case ddb::DT_DOUBLE:
                return py::float_(obj->getDouble());
            case ddb::DT_SYMBOL:
            case ddb::DT_STRING:
                return py::str(obj->getString());
            default:
                throw std::runtime_error("type error in Scalar: " + utils::DataTypeToString(type));
        }
    } else if (form == ddb::DF_DICTIONARY) {
        ddb::DictionarySP ddbDict = obj;
        ddb::DATA_TYPE keyType = ddbDict->getKeyType();
        if (keyType != ddb::DT_STRING && keyType != ddb::DT_SYMBOL && ddbDict->keys()->getCategory() != ddb::INTEGRAL) {
            throw std::runtime_error("currently only string, symbol or integral key is supported in dictionary");
        }
        ddb::VectorSP keys = ddbDict->keys();
        ddb::VectorSP values = ddbDict->values();
        py::dict pyDict;
        if (keyType == ddb::DT_STRING) {
            for (size_t i = 0; i < keys->size(); ++i) {
                pyDict[keys->getString(i).data()] = toPython(values->get(i));
            }
        } else {
            for (size_t i = 0; i < keys->size(); ++i) {
                pyDict[py::int_(keys->getLong(i))] = toPython(values->get(i));
            }
        }
        return pyDict;
    } else if (form == ddb::DF_MATRIX) {
        ddb::ConstantSP ddbMat = obj;
        size_t rows = ddbMat->rows();
        size_t cols = ddbMat->columns();
        // FIXME: currently only support numerical matrix
        if (ddbMat->getCategory() == ddb::MIXED) {
            throw std::runtime_error("currently only support single typed matrix");
        }
        ddbMat->setForm(ddb::DF_VECTOR);
        py::array pyMat = toPython(ddbMat);
        py::object pyMatRowLabel = toPython(ddbMat->getRowLabel());
        py::object pyMatColLabel = toPython(ddbMat->getColumnLabel());
        pyMat.resize({cols, rows});
        pyMat = pyMat.attr("transpose")();
        py::list pyMatList;
        pyMatList.append(pyMat);
        pyMatList.append(pyMatRowLabel);
        pyMatList.append(pyMatColLabel);
        return pyMatList;
    } else if (form == ddb::DF_PAIR) {
        ddb::VectorSP ddbPair = obj;
        py::list pyPair;
        for (size_t i = 0; i < ddbPair->size(); ++i) {
            pyPair.append(toPython(ddbPair->get(i)));
        }
        return pyPair;
    } else if (form == ddb::DF_SET) {
        ddb::VectorSP ddbSet = obj->keys();
        py::set pySet;
        for (size_t i = 0; i < ddbSet->size(); ++i) {
            pySet.add(toPython(ddbSet->get(i)));
        }
        return pySet;
    } else {
        throw std::runtime_error("form error: " + utils::DataFormToString(form));
    }
}

ddb::ConstantSP toDolphinDB(py::object obj) {
    if (py::isinstance(obj, pytype::nparray_)) {
        ddb::DATA_TYPE type = utils::DataTypeFromNumpyArray(obj);
        py::array pyVec = obj;
        if (UNLIKELY(pyVec.ndim() > 2)) {
            throw std::runtime_error("numpy.ndarray with dimension > 2 is not supported");
        }
        if (pyVec.ndim() == 1) {
            size_t size = pyVec.size();
            ddb::VectorSP ddbVec;
            ddbVec = ddb::Util::createVector(type, 0, size);
            switch (type) {
                case ddb::DT_BOOL:
                {
                    ddbVec->appendBool(reinterpret_cast<char*>(pyVec.mutable_data()), size);
                    return ddbVec;
                }
                case ddb::DT_CHAR:
                {
                    ddbVec->appendChar(reinterpret_cast<char*>(pyVec.mutable_data()), size);
                    return ddbVec;
                }
                case ddb::DT_SHORT:
                {
                    ddbVec->appendShort(reinterpret_cast<short*>(pyVec.mutable_data()), size);
                    return ddbVec;
                }
                case ddb::DT_INT:
                {
                    ddbVec->appendInt(reinterpret_cast<int*>(pyVec.mutable_data()), size);
                    return ddbVec;
                }
                case ddb::DT_LONG:
                {
                    ddbVec->appendLong(reinterpret_cast<long long*>(pyVec.mutable_data()), size);
                    return ddbVec;
                }
                case ddb::DT_DATE:
                {
                    ddbVec->appendLong(reinterpret_cast<long long*>(pyVec.mutable_data()), size);
                    return ddbVec;
                }
                case ddb::DT_MONTH:
                {
                    ddbVec->appendLong(reinterpret_cast<long long*>(pyVec.mutable_data()), size);
                    return ddbVec;
                }
                case ddb::DT_TIME:
                {
                    ddbVec->appendLong(reinterpret_cast<long long*>(pyVec.mutable_data()), size);
                    return ddbVec;
                }
                case ddb::DT_MINUTE:
                {
                    ddbVec->appendLong(reinterpret_cast<long long*>(pyVec.mutable_data()), size);
                    return ddbVec;
                }
                case ddb::DT_SECOND:
                {
                    ddbVec->appendLong(reinterpret_cast<long long*>(pyVec.mutable_data()), size);
                    return ddbVec;
                }
                case ddb::DT_DATETIME:
                {
                    ddbVec->appendLong(reinterpret_cast<long long*>(pyVec.mutable_data()), size);
                    return ddbVec;
                }
                case ddb::DT_TIMESTAMP:
                {
                    ddbVec->appendLong(reinterpret_cast<long long*>(pyVec.mutable_data()), size);
                    return ddbVec;
                }
                case ddb::DT_NANOTIME:
                {
                    ddbVec->appendLong(reinterpret_cast<long long*>(pyVec.mutable_data()), size);
                    return ddbVec;
                }
                case ddb::DT_NANOTIMESTAMP:
                {
                    ddbVec->appendLong(reinterpret_cast<long long*>(pyVec.mutable_data()), size);
                    return ddbVec;
                }
                case ddb::DT_FLOAT:
                {
                    ddbVec->appendFloat(reinterpret_cast<float*>(pyVec.mutable_data()), size);
                    return ddbVec;
                }
                case ddb::DT_DOUBLE:
                {
                    // special handle for np.nan value as type(np.nan)=float
                    ddbVec->appendDouble(reinterpret_cast<double*>(pyVec.mutable_data()), size);
                    if (pyfunction::isnan_(pyfunction::sum_(pyVec)).cast<bool>()) {
                        auto p = reinterpret_cast<double*>(pyVec.mutable_data());
                        for (size_t i = 0; i < size; ++i) {
                            if (UNLIKELY(*reinterpret_cast<long long*>(p+i) == 9221120237041090560LL)) {
                                ddbVec->setDouble(i, ddb::DBL_NMIN);
                            }
                        }
                    }
                    return ddbVec;
                }
                case ddb::DT_SYMBOL:
                case ddb::DT_STRING:
                case ddb::DT_ANY:
                {
                    // extra check (determine string vector or any vector)
                    type = ddb::DT_STRING;
                    for (auto it = pyVec.begin(); it != pyVec.end(); ++it) {
                        if (!py::isinstance(*it, py::str().get_type())) {
                            type = ddb::DT_ANY;
                            break;
                        }
                    }
                    ddbVec = ddb::Util::createVector(type, 0, size);
                    if (type == ddb::DT_ANY) {
                        for (auto it = pyVec.begin(); it != pyVec.end(); ++it) {
                            ddb::ConstantSP item = toDolphinDB(py::reinterpret_borrow<py::object>(*it));
                            ddbVec->append(item);
                        }
                    } else {
                        vector<std::string> strs;
                        for (auto it = pyVec.begin(); it != pyVec.end(); ++it) {
                            strs.emplace_back(py::reinterpret_borrow<py::str>(*it).cast<std::string>());
                        }
                        ddbVec->appendString(strs.data(), strs.size());
                    }
                    return ddbVec;
                }
                default:
                {
                    throw std::runtime_error("type error in numpy: " + utils::DataTypeToString(type));
                }
            }
        } else {
            size_t rows = pyVec.shape(0);
            size_t cols = pyVec.shape(1);
            size_t size = rows * cols;
            pyVec = pyVec.attr("transpose")().attr("reshape")(pyVec.size());
            ddb::ConstantSP ddbVec = toDolphinDB(pyVec);
            // FIXME: consider batch?
            ddb::ConstantSP ddbMat = ddb::Util::createMatrix(type, cols, rows, cols);
            for (size_t i = 0; i < cols; ++i) {
                for (size_t j = 0; j < rows; ++j) {
                    ddbMat->set(i, j, ddbVec->get(i * rows + j));
                }
            }
            return ddbMat;
        }
    } else if (py::isinstance(obj, pytype::pddataframe_)) {
        py::object dataframe = obj;
        py::object pyLabel = dataframe.attr("columns");
        size_t columnSize = pyLabel.attr("size").cast<size_t>();
        vector<std::string> columnNames;
        columnNames.reserve(columnSize);
        for (auto it = pyLabel.begin(); it != pyLabel.end(); ++it) {
            columnNames.emplace_back(it->cast<std::string>());
        }
        vector<ddb::ConstantSP> columns;
        columns.reserve(columnSize);
        for (size_t i = 0; i < columnSize; ++i) {
            columns.emplace_back(toDolphinDB(py::array(dataframe[columnNames[i].data()])));
        }
        ddb::TableSP ddbTbl = ddb::Util::createTable(columnNames, columns);
        return ddbTbl;
    } else if (py::isinstance(obj, pytype::pynone_)) {
        return ddb::Util::createNullConstant(ddb::DT_DOUBLE);
    } else if (py::isinstance(obj, pytype::pybool_)) {
        auto result = obj.cast<bool>();
        return ddb::Util::createBool(result);
    } else if (py::isinstance(obj, pytype::pyint_)) {
        auto result = obj.cast<long long>();
        return ddb::Util::createLong(result);
    } else if (py::isinstance(obj, pytype::pyfloat_)) {
        auto result = obj.cast<double>();
        if (*(long long *)&result == 9221120237041090560LL) {
            result = ddb::DBL_NMIN;
        }
        return ddb::Util::createDouble(result);
    } else if (py::isinstance(obj, pytype::pystr_)) {
        auto result = obj.cast<std::string>();
        return ddb::Util::createString(result);
    } else if (py::isinstance(obj, pytype::pybytes_)) {
        auto result = obj.cast<std::string>();
        return ddb::Util::createString(result);
    } else if (py::isinstance(obj, pytype::pyset_)) {
        py::set pySet = obj;
        size_t size = pySet.size();
        vector<ddb::ConstantSP> _ddbSet;
        ddb::DATA_TYPE type = ddb::DT_VOID;
        ddb::DATA_FORM form = ddb::DF_SCALAR;
        int types = 0;
        int forms = 1;
        for (auto it = pySet.begin(); it != pySet.end(); ++it) {
            _ddbSet.push_back(toDolphinDB(py::reinterpret_borrow<py::object>(*it)));
            if (_ddbSet.back()->isNull()) {
                continue;
            }
            ddb::DATA_TYPE tmpType = _ddbSet.back()->getType();
            ddb::DATA_FORM tmpForm = _ddbSet.back()->getForm();
            if (tmpType != type) {
                types++;
                type = tmpType;
            }
            if (tmpForm != form) {
                forms++;
            }
        }
        if (types >= 2 || forms >= 2) {
            type = ddb::DT_ANY;
        } else if (types == 0) {
            throw std::runtime_error("can not create all None set");
        }
        ddb::SetSP ddbSet = ddb::Util::createSet(type, 0);
        for (size_t i = 0; i < size; ++i) {
            ddbSet->append(_ddbSet[i]);
        }
        return ddbSet;
    } else if (py::isinstance(obj, pytype::pytuple_)) {
        py::tuple tuple = obj;
        size_t size = tuple.size();
        vector<ddb::ConstantSP> _ddbVec;
        ddb::DATA_TYPE type = ddb::DT_VOID;
        ddb::DATA_FORM form = ddb::DF_SCALAR;
        int types = 0;
        int forms = 1;
        for (size_t i = 0; i < size; ++i) {
            _ddbVec.push_back(toDolphinDB(tuple[i]));
            if (_ddbVec.back()->isNull()) {
                continue;
            }
            ddb::DATA_TYPE tmpType = _ddbVec.back()->getType();
            ddb::DATA_FORM tmpForm = _ddbVec.back()->getForm();
            if (tmpType != type) {
                types++;
                type = tmpType;
            }
            if (tmpForm != form) {
                forms++;
            }
        }
        if (types >= 2 || forms >= 2) {
            type = ddb::DT_ANY;
        } else if (types == 0) {
            throw std::runtime_error("can not create all None vector");
        }
        ddb::VectorSP ddbVec = ddb::Util::createVector(type, 0, size);
        for (size_t i = 0; i < size; ++i) {
            ddbVec->append(_ddbVec[i]);
        }
        return ddbVec;
    } else if (py::isinstance(obj, pytype::pylist_)) {
        py::list list = obj;
        size_t size = list.size();
        vector<ddb::ConstantSP> _ddbVec;
        ddb::DATA_TYPE type = ddb::DT_VOID;
        ddb::DATA_FORM form = ddb::DF_SCALAR;
        int types = 0;
        int forms = 1;
        for (size_t i = 0; i < size; ++i) {
            _ddbVec.push_back(toDolphinDB(list[i]));
            if (_ddbVec.back()->isNull()) {
                continue;
            }
            ddb::DATA_TYPE tmpType = _ddbVec.back()->getType();
            ddb::DATA_FORM tmpForm = _ddbVec.back()->getForm();
            if (tmpType != type) {
                types++;
                type = tmpType;
            }
            if (tmpForm != form) {
                forms++;
            }
        }
        if (types >= 2 || forms >= 2) {
            type = ddb::DT_ANY;
        } else if (types == 0) {
            throw std::runtime_error("can not create all None vector");
        }
        ddb::VectorSP ddbVec = ddb::Util::createVector(type, 0, size);
        for (size_t i = 0; i < size; ++i) {
            ddbVec->append(_ddbVec[i]);
        }
        return ddbVec;
    } else if (py::isinstance(obj, pytype::pydict_)) {
        py::dict pyDict = obj;
        size_t size = pyDict.size();
        vector<ddb::ConstantSP> _ddbKeyVec;
        vector<ddb::ConstantSP> _ddbValVec;
        ddb::DATA_TYPE keyType = ddb::DT_VOID;
        ddb::DATA_TYPE valType = ddb::DT_VOID;
        ddb::DATA_FORM keyForm = ddb::DF_SCALAR;
        ddb::DATA_FORM valForm = ddb::DF_SCALAR;
        int keyTypes = 0;
        int valTypes = 0;
        int keyForms = 1;
        int valForms = 1;
        for (auto it = pyDict.begin(); it != pyDict.end(); ++it) {
            _ddbKeyVec.push_back(toDolphinDB(py::reinterpret_borrow<py::object>(it->first)));
            _ddbValVec.push_back(toDolphinDB(py::reinterpret_borrow<py::object>(it->second)));
            if (_ddbKeyVec.back()->isNull() || _ddbValVec.back()->isNull()) {
                continue;
            }
            ddb::DATA_TYPE tmpKeyType = _ddbKeyVec.back()->getType();
            ddb::DATA_TYPE tmpValType = _ddbValVec.back()->getType();
            ddb::DATA_FORM tmpKeyForm = _ddbKeyVec.back()->getForm();
            ddb::DATA_FORM tmpValForm = _ddbValVec.back()->getForm();
            if (tmpKeyType != keyType) {
                keyTypes++;
                keyType = tmpKeyType;
            }
            if (tmpValType != valType) {
                valTypes++;
                valType = tmpValType;
            }
            if (tmpKeyForm != keyForm) {
                keyForms++;
            }
            if (tmpValForm != valForm) {
                valForms++;
            }
        }
        if (keyTypes >= 2 || keyType == ddb::DT_BOOL || keyForm >= 2) {
            throw std::runtime_error("the key type can not be BOOL or ANY");
        }
        if (valTypes >= 2 || valForm >= 2) {
            valType = ddb::DT_ANY;
        } else if (keyTypes == 0 || valTypes == 0) {
            throw std::runtime_error("can not create all None vector in dictionary");
        }
        ddb::VectorSP ddbKeyVec = ddb::Util::createVector(keyType, 0, size);
        ddb::VectorSP ddbValVec = ddb::Util::createVector(valType, 0, size);
        for (size_t i = 0; i < size; ++i) {
            ddbKeyVec->append(_ddbKeyVec[i]);
            ddbValVec->append(_ddbValVec[i]);
        }
        ddb::DictionarySP ddbDict = ddb::Util::createDictionary(keyType, valType);
        ddbDict->set(ddbKeyVec, ddbValVec);
        return ddbDict;
    } else if (py::isinstance(obj, pytype::datetime64_)) {
        if (py::getattr(obj, "dtype").equal(pytype::npdatetime64M_)) {
            return ddb::Util::createMonth(1970, 1 + obj.attr("astype")("int64").cast<long long>());
        } else if (py::getattr(obj, "dtype").equal(pytype::npdatetime64D_)) {
            return ddb::Util::createDate(obj.attr("astype")("int64").cast<long long>());
        } else if (py::getattr(obj, "dtype").equal(pytype::npdatetime64m_)) {
            return ddb::Util::createMinute(obj.attr("astype")("int64").cast<long long>());
        } else if (py::getattr(obj, "dtype").equal(pytype::npdatetime64s_)) {
            return ddb::Util::createSecond(obj.attr("astype")("int64").cast<long long>());
        } else if (py::getattr(obj, "dtype").equal(pytype::npdatetime64ms_)) {
            return ddb::Util::createTimestamp(obj.attr("astype")("int64").cast<long long>());
        } else if (py::getattr(obj, "dtype").equal(pytype::npdatetime64ns_)) {
            return ddb::Util::createNanoTimestamp(obj.attr("astype")("int64").cast<long long>());
        } else {
            throw std::runtime_error("unsupported numpy.datetime64 dtype");
        }
    } else {
        throw std::runtime_error("unrecognized Python type: " + py::str(obj.get_type()).cast<std::string>());
    }
}
}  // namespace utils

}  // namespace pydolphindb
