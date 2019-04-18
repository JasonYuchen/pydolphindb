#include "Session.h"

#if defined(__GNUC__) && __GNUC__ >= 4
#define LIKELY(x) (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
#define LIKELY(x) (x)
#define UNLIKELY(x) (x)
#endif

namespace pydolphindb {

Session::Session()
    : host_()
    , port_(-1)
    , userId_()
    , password_()
    , encrypted_(true)
    , dbConnection_()
    , subscriber_(nullptr)
    , listeningPort_(-1)
    , topicThread_()
    , nullValuePolicy_([](ddb::VectorSP){}) {}

bool Session::connect(const std::string &host,
                      int port,
                      const std::string &userId,
                      const std::string &password) {
    host_ = host;
    port_ = port;
    userId_ = userId;
    password_ = password;
    bool isSuccess = false;
    try {
        isSuccess = dbConnection_.connect(host_, port_, userId_, password_);
    } catch (std::exception &ex) {
        throw std::runtime_error(std::string("<Server Exception> connect: ") + ex.what());
    }
    return isSuccess;
}

void Session::login(const std::string &userId,
                    const std::string &password,
                    bool enableEncryption) {
    try {
        dbConnection_.login(userId, password, enableEncryption);
    } catch (std::exception &ex) {
        throw std::runtime_error(std::string("<Server Exception> login: ") + ex.what());
    }
}

void Session::close() {
    host_ = "";
    port_ = 0;
    userId_ = "";
    password_ = "";
    dbConnection_.close();
}

void Session::upload(py::dict namedObjects) {
    vector<std::string> names;
    vector<ddb::ConstantSP> objs;
    for (auto it = namedObjects.begin(); it != namedObjects.end(); ++it) {
        if (!py::isinstance(it->first, pytype::pystr_) && !py::isinstance(it->first, pytype::pybytes_)) {
            throw std::runtime_error("<Python API Exception> upload: non-string key in upload dictionary is not allowed");
        }
        names.push_back(it->first.cast<std::string>());
        objs.push_back(utils::toDolphinDB(py::reinterpret_borrow<py::object>(it->second)));
    }
    try {
        dbConnection_.upload(names, objs);
    } catch (std::exception &ex) {
        throw std::runtime_error(std::string("<Server Exception> in upload: ") + ex.what());
    }
}

py::object Session::run(const string &script) {
    ddb::ConstantSP result;
    try {
        result = dbConnection_.run(script);
    } catch (std::exception &ex) {
        throw std::runtime_error(std::string("<Server Exception> in run: ") + ex.what());
    }
    py::object ret = utils::toPython(result);
    return ret;
}

py::object Session::run(const string &funcName,
                        py::args args) {
    vector<ddb::ConstantSP> ddbArgs;
    for (auto it = args.begin(); it != args.end(); ++it) {
        ddbArgs.push_back(utils::toDolphinDB(py::reinterpret_borrow<py::object>(*it)));
    }
    ddb::ConstantSP result;
    try {
        result = dbConnection_.run(funcName, ddbArgs);
    } catch (std::exception &ex) {
        throw std::runtime_error(std::string("<Server Exception> in call: ") + ex.what());
    }
    py::object ret = utils::toPython(result);
    return ret;
}

void Session::nullValueToZero() {
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

void Session::nullValueToNan() {
    nullValuePolicy_ = [](ddb::VectorSP) {};
}

void Session::enableStreaming(int listeningPort) {
    if (subscriber_.isNull()) {
        listeningPort_ = listeningPort;
        subscriber_ = new ddb::ThreadedClient(listeningPort);
    } else {
        throw std::runtime_error("<Python API Exception> enableStreaming: streaming is already enabled on port " + std::to_string(listeningPort_));
    }
}

void Session::subscribe(const string &host,
                        int port,
                        py::object handler,
                        const string &tableName,
                        const string &actionName,
                        long long offset,
                        bool resub,
                        py::array filter) {
    if (subscriber_.isNull()) {
        throw std::runtime_error("<Python API Exception> subscribe: streaming is not enabled");
    }
    string topic = host + "/" + std::to_string(port) + "/" + tableName + "/" + actionName;
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

void Session::unsubscribe(string host,
                          int port,
                          string tableName,
                          string actionName) {
    if (subscriber_.isNull()) {
        throw std::runtime_error("<Python API Exception> unsubscribe: streaming is not enabled");
    }
    string topic = host + "/" + std::to_string(port) + "/" + tableName + "/" + actionName;
    if (topicThread_.find(topic) == topicThread_.end()) {
        throw std::runtime_error("<Python API Exception> unsubscribe: subscription " + topic + " not exists");
    }
    subscriber_->unsubscribe(host, port, tableName, actionName);
    topicThread_.erase(topic);
}

py::list Session::getSubscriptionTopics() {
    py::list topics;
    for (auto &it : topicThread_) {
        topics.append(it.first);
    }
    return topics;
}

Session::~Session() {
    for (auto &it : topicThread_) {
        vector<std::string> args = ddb::Util::split(it.first, '/');
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

