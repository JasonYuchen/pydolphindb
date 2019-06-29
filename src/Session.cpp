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

#include <vector>

#include "Session.h"

#if defined(__GNUC__) && __GNUC__ >= 4
#define LIKELY(x) (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
#define LIKELY(x) (x)
#define UNLIKELY(x) (x)
#endif

namespace pydolphindb
{

Session::Session()
  : mutex_()
  , host_()
  , port_(-1)
  , userId_()
  , password_()
  , encrypted_(true)
  , dbConnection_()
  , nullValuePolicy_([](ddb::VectorSP){})
{

}

bool Session::connect(
  const std::string &host,
  int port,
  const std::string &userId,
  const std::string &password)
{
  std::lock_guard<std::mutex> guard(mutex_);
  host_ = host;
  port_ = port;
  userId_ = userId;
  password_ = password;
  bool isSuccess = false;
  try {
    isSuccess = dbConnection_.connect(host_, port_, userId_, password_);
  } catch (std::exception &ex) {
    throw std::runtime_error(std::string("<Server Exception> connect: ") +
      ex.what());
  }
  return isSuccess;
}

void Session::login(
  const std::string &userId,
  const std::string &password,
  bool enableEncryption)
{
  std::lock_guard<std::mutex> guard(mutex_);
  try {
    dbConnection_.login(userId, password, enableEncryption);
  } catch (std::exception &ex) {
    throw std::runtime_error(std::string("<Server Exception> login: ") +
      ex.what());
  }
}

void Session::close()
{
  std::lock_guard<std::mutex> guard(mutex_);
  host_ = "";
  port_ = 0;
  userId_ = "";
  password_ = "";
  dbConnection_.close();
}

void Session::upload(py::dict namedObjects)
{
  vector<std::string> names;
  vector<ddb::ConstantSP> objs;
  for (auto it = namedObjects.begin(); it != namedObjects.end(); ++it) {
    if (!py::isinstance(it->first, pytype::pystr_) &&
      !py::isinstance(it->first, pytype::pybytes_)) {
      throw std::runtime_error("<Python API Exception> upload: non-string key"
        "in upload dictionary is not allowed");
    }
    names.push_back(it->first.cast<std::string>());
    objs.push_back(
      utils::toDolphinDB(py::reinterpret_borrow<py::object>(it->second)));
  }
  try {
    dbConnection_.upload(names, objs);
  } catch (std::exception &ex) {
    throw std::runtime_error(std::string("<Server Exception> in upload: ") +
      ex.what());
  }
}

py::object Session::run(const std::string &script)
{
  ddb::ConstantSP result;
  try {
    result = dbConnection_.run(script);
  } catch (std::exception &ex) {
    throw std::runtime_error(std::string("<Server Exception> in run: ") +
      ex.what());
  }
  py::object ret = utils::toPython(result);
  return ret;
}

py::object Session::run(
  const std::string &funcName,
  py::args args)
{
  vector<ddb::ConstantSP> ddbArgs;
  for (auto it = args.begin(); it != args.end(); ++it) {
    ddbArgs.push_back(
      utils::toDolphinDB(py::reinterpret_borrow<py::object>(*it)));
  }
  ddb::ConstantSP result;
  try {
    result = dbConnection_.run(funcName, ddbArgs);
  } catch (std::exception &ex) {
    throw std::runtime_error(std::string("<Server Exception> in call: ") +
      ex.what());
  }
  py::object ret = utils::toPython(result);
  return ret;
}

void Session::nullValueToZero()
{
  nullValuePolicy_ = [](ddb::VectorSP vec) {
    if (!vec->hasNull() ||
      vec->getCategory() == ddb::TEMPORAL ||
      vec->getType() == ddb::DT_STRING ||
      vec->getType() == ddb::DT_SYMBOL) {
      return;
    } else {
      ddb::ConstantSP val = ddb::Util::createConstant(ddb::DT_LONG);
      val->setLong(0);
      vec->nullFill(val);
      assert(!vec->hasNull());
    }
  };
}

void Session::nullValueToNan()
{
  nullValuePolicy_ = [](ddb::VectorSP) {};
}

}  // namespace pydolphindb

