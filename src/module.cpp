#include <pybind11/pybind11.h>

#include "Session.h"

namespace py = pybind11;
namespace ddb = dolphindb;

using Session = pydolphindb::Session;

PYBIND11_MODULE(pydolphindb, m) {
    m.doc() = R"pbdoc(pydolphindb: yet another C++ implemented DolphinDB Python)pbdoc";

    py::class_<Session>(m, "session")
            .def(py::init<>())
            .def("connect", &Session::connect)
            .def("login", &Session::login)
            .def("close", &Session::close)
            .def("run", (py::object (Session::*)(const std::string&))&Session::run)
            .def("run", (py::object (Session::*)(const std::string&, py::args))&Session::run)
            .def("upload", &Session::upload)
            .def("nullValueToZero", &Session::nullValueToZero)
            .def("nullValueToNan", &Session::nullValueToNan)
            .def("enableStreaming", &Session::enableStreaming)
            .def("subscribe", &Session::subscribe)
            .def("unsubscribe", &Session::unsubscribe)
            .def("getSubscriptionTopics", &Session::getSubscriptionTopics);

#ifdef VERSION_INFO
    m.attr("__version__") = VERSION_INFO;
#else
    m.attr("__version__") = "unknown";
#endif
}
