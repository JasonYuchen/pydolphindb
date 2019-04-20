#ifndef PYDOLPHINDB_STREAMING_H_
#define PYDOLPHINDB_STREAMING_H_

#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>

#include <string>
#include <unordered_map>
#include <mutex>
#include <memory>

#include <DolphinDB.h>
#include <Streaming.h>

namespace pydolphindb {

namespace py = pybind11;
namespace ddb = dolphindb;

class Streaming {
 public:
    Streaming();
    Streaming(const Streaming &) = delete;
    Streaming &operator=(const Streaming &) = delete;

    void listen(int listeningPort);

    void subscribe(const std::string &host,
                   int port,
                   py::object handler,
                   const std::string &tableName,
                   const std::string &actionName,
                   long long offset,
                   bool resub,
                   py::array filter);

    void unsubscribe(std::string host,
                     int port,
                     std::string tableName,
                     std::string actionName);

    py::list getSubscriptionTopics();

    ~Streaming();

 private:
    std::mutex mutex_;
    std::unique_ptr<ddb::ThreadedClient> subscriber_;
    int listeningPort_;
    std::unordered_map<std::string, ddb::ThreadSP> topicThread_;
};

}  // namespace pydolphindb

#endif  // PYDOLPHINDB_STREAMING_H_
