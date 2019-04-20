#include <pybind11/pybind11.h>

#include "src/Session.h"
#include "src/Streaming.h"

namespace py = pybind11;
namespace ddb = dolphindb;

using Session = pydolphindb::Session;
using Streaming = pydolphindb::Streaming;

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
        .def("nullValueToNan", &Session::nullValueToNan);

    py::class_<Streaming>(m, "streaming")
        .def(py::init<>())
        .def("listen", &Streaming::listen)
        .def("subscribe", &Streaming::subscribe)
        .def("unsubscribe", &Streaming::unsubscribe)
        .def("getSubscriptionTopics", &Streaming::getSubscriptionTopics);

#ifdef VERSION_INFO
    m.attr("__version__") = VERSION_INFO;
#else
    m.attr("__version__") = "unknown";
#endif
}
