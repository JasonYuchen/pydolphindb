#ifndef PYDOLPHINDB_SESSION_H_
#define PYDOLPHINDB_SESSION_H_

#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>

#include <string>
#include <unordered_map>

#include <DolphinDB.h>
#include <Util.h>
#include <Streaming.h>

#include "Utils.h"

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

    py::object run(const string &script);

    py::object run(const string &funcName,
                   py::args args);

    void nullValueToZero();

    void nullValueToNan();

    void enableStreaming(int listeningPort);

    void subscribe(const string &host,
                   int port,
                   py::object handler,
                   const string &tableName,
                   const string &actionName,
                   long long offset,
                   bool resub,
                   py::array filter);

    void unsubscribe(string host,
                     int port,
                     string tableName,
                     string actionName);

    py::list getSubscriptionTopics();

    ~Session();

 private:
    ddb::Mutex mutex_;
    std::string host_;
    int port_;
    std::string userId_;
    std::string password_;
    bool encrypted_;
    ddb::DBConnection dbConnection_;
    ddb::SmartPointer<ddb::ThreadedClient> subscriber_;
    int listeningPort_;
    std::unordered_map<string, ddb::ThreadSP> topicThread_;
    void (*nullValuePolicy_)(ddb::VectorSP);
};

}  // namespace pydolphindb

#endif  // PYDOLPHINDB_SESSION_H_
