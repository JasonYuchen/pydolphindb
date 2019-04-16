#ifndef PYDOLPHINDB_UTILS_H_
#define PYDOLPHINDB_UTILS_H_

#include <pybind11/pybind11.h>
#include <string>
#include <Types.h>

namespace pydolphindb
{

namespace py = pybind11;
namespace ddb = dolphindb;
using py::module;
using py::handle;

struct PyModule {
    static const module numpy_;
    static const module pandas_;
};

struct PyFunction {
    static const handle isnan_;
    static const handle sum_;
};

struct PyType {
    // type, equal to np.datetime64
    static const handle datetime64_;

    // pandas types (use isinstance)
    static const handle pddataframe_;

    // numpy dtypes (instances of dtypes, use equal)
    static const handle nparray_;
    static const handle npbool_;
    static const handle npint8_;
    static const handle npint16_;
    static const handle npint32_;
    static const handle npint64_;
    static const handle npfloat32_;
    static const handle npfloat64_;
    static const handle npdatetime64M_;
    static const handle npdatetime64D_;
    static const handle npdatetime64m_;
    static const handle npdatetime64s_;
    static const handle npdatetime64ms_;
    static const handle npdatetime64ns_;
    static const handle npdatetime64_;  // dtype, equal to np.datetime64().dtype
    static const handle npobject_;

    // python types (use isinstance)
    static const handle pynone_;
    static const handle pybool_;
    static const handle pyint_;
    static const handle pyfloat_;
    static const handle pystr_;
    static const handle pybytes_;
    static const handle pyset_;
    static const handle pytuple_;
    static const handle pylist_;
    static const handle pydict_;

    // null map
    static const uint64_t npnan_ = 9221120237041090560LL;
};

struct Utils {
    static std::string DataCategoryToString(ddb::DATA_CATEGORY cate) noexcept;
    static std::string DataFormToString(ddb::DATA_FORM form) noexcept;
    static std::string DataTypeToString(ddb::DATA_TYPE type) noexcept;
    static inline void SET_NPNAN(void *p, size_t len = 1);
    static inline void SET_DDBNAN(void *p, size_t len = 1);
    static inline bool IS_NPNAN(void *p);
    static ddb::DATA_TYPE DataTypeFromNumpyArray(py::array array);
    static py::object toPython(ddb::ConstantSP obj);
    static ddb::ConstantSP toDolphinDB(py::object obj);
};

}  // namespace pydolphindb

#endif  // PYDOLPHINDB_UTILS_H_
