// MIT License

// Copyright (c) 2019 jasonyuchen

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#ifndef PYDOLPHINDB_UTILS_H_
#define PYDOLPHINDB_UTILS_H_

#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>

#include <string>

#include <DolphinDB.h>
#include <Types.h>

#define DISALLOW_COPY_MOVE_AND_ASSIGN(TypeName)     \
TypeName(const TypeName&) = delete;                 \
TypeName& operator=(const TypeName&) = delete;      \
TypeName(TypeName&&) = delete;                      \
TypeName& operator=(const TypeName&&) = delete

namespace pydolphindb
{

namespace py = pybind11;
namespace ddb = dolphindb;
using py::module;
using py::handle;

namespace pymodule
{

extern const module numpy_;
extern const module pandas_;

}

namespace pyfunction
{

extern const handle isnan_;
extern const handle sum_;

}

namespace pytype
{

// type, equal to np.datetime64
extern const handle datetime64_;

// pandas types (use isinstance)
extern const handle pddataframe_;

// numpy dtypes (instances of dtypes, use equal)
extern const handle nparray_;
extern const handle npbool_;
extern const handle npint8_;
extern const handle npint16_;
extern const handle npint32_;
extern const handle npint64_;
extern const handle npfloat32_;
extern const handle npfloat64_;
extern const handle npdatetime64M_;
extern const handle npdatetime64D_;
extern const handle npdatetime64m_;
extern const handle npdatetime64s_;
extern const handle npdatetime64ms_;
extern const handle npdatetime64ns_;
extern const handle npdatetime64_;  // dtype, equal to np.datetime64().dtype
extern const handle npobject_;

// python types (use isinstance)
extern const handle pynone_;
extern const handle pybool_;
extern const handle pyint_;
extern const handle pyfloat_;
extern const handle pystr_;
extern const handle pybytes_;
extern const handle pyset_;
extern const handle pytuple_;
extern const handle pylist_;
extern const handle pydict_;

// null map
constexpr uint64_t npnan_ = 9221120237041090560LL;

}

namespace utils
{

std::string DataCategoryToString(ddb::DATA_CATEGORY cate) noexcept;
std::string DataFormToString(ddb::DATA_FORM form) noexcept;
std::string DataTypeToString(ddb::DATA_TYPE type) noexcept;
inline void SET_NPNAN(void *p, size_t len = 1);
inline void SET_DDBNAN(void *p, size_t len = 1);
inline bool IS_NPNAN(void *p);
ddb::DATA_TYPE DataTypeFromNumpyArray(py::array array);
py::object toPython(ddb::ConstantSP obj, void (*nullValuePolicyForVector)(ddb::VectorSP) = [](ddb::VectorSP){});
ddb::ConstantSP toDolphinDB(py::object obj);

}  // namespace utils

}  // namespace pydolphindb

#endif  // PYDOLPHINDB_UTILS_H_
