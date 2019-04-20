#ifndef PYDOLPHINDB_SESSION_H_
#define PYDOLPHINDB_SESSION_H_

#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>

#include <string>
#include <mutex>

#include <DolphinDB.h>
#include <Util.h>

#include "src/Utils.h"

namespace pydolphindb {

namespace py = pybind11;
namespace ddb = dolphindb;

class Session {
 public:
    Session();
    Session(const Session &) = delete;
    Session &operator=(const Session &) = delete;

    bool connect(const std::string &host,
                 int port,
                 const std::string &userId,
                 const std::string &password);

    void login(const std::string &userId,
               const std::string &password,
               bool enableEncryption);

    void close();

    void upload(py::dict namedObjects);

    py::object run(const std::string &script);

    py::object run(const std::string &funcName,
                   py::args args);

    void nullValueToZero();

    void nullValueToNan();

    ~Session();

 private:
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
