//
// Created by jccai on 3/28/19.
//

#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <DolphinDB.h>
#include <Streaming.h>
#include <Util.h>
#include <vector>
#include <string>
#include <unordered_map>

namespace py = pybind11;
namespace ddb = dolphindb;

#if defined(__GNUC__) && __GNUC__ >= 4
#define LIKELY(x) (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
#define LIKELY(x) (x)
#define UNLIKELY(x) (x)
#endif


// FIXME: not thread safe


PYBIND11_MODULE(dolphindbcpp, m) {
    m.doc() = R"pbdoc(dolphindbcpp: this is a C++ boosted DolphinDB Python API)pbdoc";

    py::class_<SessionImpl>(m, "sessionimpl")
            .def(py::init<>())
            .def("connect", &SessionImpl::connect)
            .def("login", &SessionImpl::login)
            .def("close", &SessionImpl::close)
            .def("run", (py::object (SessionImpl::*)(const std::string&))&SessionImpl::run)
            .def("run", (py::object (SessionImpl::*)(const std::string&, py::args))&SessionImpl::run)
            .def("upload", &SessionImpl::upload)
            .def("nullValueToZero", &SessionImpl::nullValueToZero)
            .def("nullValueToNan", &SessionImpl::nullValueToNan)
            .def("enableStreaming", &SessionImpl::enableStreaming)
            .def("subscribe", &SessionImpl::subscribe)
            .def("unsubscribe", &SessionImpl::unsubscribe)
            .def("getSubscriptionTopics", &SessionImpl::getSubscriptionTopics);

#ifdef VERSION_INFO
    m.attr("__version__") = VERSION_INFO;
#else
    m.attr("__version__") = "dev";
#endif
}
