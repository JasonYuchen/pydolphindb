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

#ifndef PYDOLPHINDB_SESSION_H_
#define PYDOLPHINDB_SESSION_H_

#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>

#include <string>
#include <mutex>

#include <DolphinDB.h>
#include <Util.h>

#include "Utils.h"

namespace pydolphindb
{

namespace py = pybind11;
namespace ddb = dolphindb;

class Session {
 public:
  Session();
  ~Session() = default;
  bool connect(
    const std::string &host,
    int port,
    const std::string &userId,
    const std::string &password);
  void login(
    const std::string &userId,
    const std::string &password,
    bool enableEncryption);
  void close();
  void upload(py::dict namedObjects);
  py::object run(const std::string &script);
  py::object run(const std::string &funcName, py::args args);
  void nullValueToZero();
  void nullValueToNan();
 private:
  DISALLOW_COPY_MOVE_AND_ASSIGN(Session);
  std::mutex mutex_;
  std::string host_;
  int port_;
  std::string userId_;
  std::string password_;
  bool encrypted_;
  ddb::DBConnection dbConnection_;
  std::function<void(ddb::VectorSP)> nullValuePolicy_;
};

}  // namespace pydolphindb

#endif  // PYDOLPHINDB_SESSION_H_
