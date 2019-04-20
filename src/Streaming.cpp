#include <vector>

#include "src/Streaming.h"

namespace pydolphindb {

Streaming::Streaming()
    : mutes_()
    , subscriber_(nullptr)
    , listeningPort_(-1)
    , topicThread_() {}

void Streaming::listen(int listeningPort) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (subscriber_.isNull()) {
        listeningPort_ = listeningPort;
        subscriber_ = std::make_unique<ddb::ThreadedClient>(ddb::ThreadedClient(listeningPort));
    } else {
        throw std::runtime_error("<Python API Exception> enableStreaming: streaming is already enabled on port " + std::to_string(listeningPort_));
    }
}

void Streaming::subscribe(const std::string &host,
                          int port,
                          py::object handler,
                          const std::string &tableName,
                          const std::string &actionName,
                          long long offset,
                          bool resub,
                          py::array filter) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (subscriber_.isNull()) {
        throw std::runtime_error("<Python API Exception> subscribe: streaming is not enabled");
    }
    std::string topic = host + "/" + std::to_string(port) + "/" + tableName + "/" + actionName;
    if (topicThread_.find(topic) != topicThread_.end()) {
        throw std::runtime_error("<Python API Exception> subscribe: subscription " + topic + " already exists");
    }
    ddb::MessageHandler ddbHandler = [handler, this](ddb::Message msg) {
        // handle GIL
        py::gil_scoped_acquire acquire;
        size_t size = msg->size();
        py::list pyMsg;
        for (size_t i = 0; i < size; ++i) {
            pyMsg.append(utils::toPython(msg->get(i)));
        }
        handler(pyMsg);
    };
    ddb::VectorSP ddbFilter = filter.size() ? utils::toDolphinDB(filter) : nullptr;
    ddb::ThreadSP thread = subscriber_->subscribe(host, port, ddbHandler, tableName, actionName, offset, resub, ddbFilter);
    topicThread_[topic] = thread;
}

void Streaming::unsubscribe(std::string host,
                            int port,
                            std::string tableName,
                            std::string actionName) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (subscriber_.isNull()) {
        throw std::runtime_error("<Python API Exception> unsubscribe: streaming is not enabled");
    }
    std::string topic = host + "/" + std::to_string(port) + "/" + tableName + "/" + actionName;
    if (topicThread_.find(topic) == topicThread_.end()) {
        throw std::runtime_error("<Python API Exception> unsubscribe: subscription " + topic + " not exists");
    }
    subscriber_->unsubscribe(host, port, tableName, actionName);
    topicThread_.erase(topic);
}

py::list Streaming::getSubscriptionTopics() {
    std::lock_guard<std::mutex> guard(mutex_);
    py::list topics;
    for (auto &it : topicThread_) {
        topics.append(it.first);
    }
    return topics;
}

Streaming::~Streaming() {
    for (auto &it : topicThread_) {
        std::vector<std::string> args = ddb::Util::split(it.first, '/');
        try {
            unsubscribe(args[0], std::stoi(args[1]), args[2], args[3]);
        } catch (std::exception &ex) {
            std::cout << "<Python API Exception> ~Session: " << ex.what() << std::endl;
        }
    }
    for (auto &it : topicThread_) {
        it.second->join();
    }
}

}  // namespace pydolphindb
