// MIT License

// Copyright (c) [2019] [jasonyuchen]

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

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
    Streaming(Streaming &&) = delete;
    Streaming &operator=(Streaming &&) = delete;

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
