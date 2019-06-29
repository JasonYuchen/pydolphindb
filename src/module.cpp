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

#include <pybind11/pybind11.h>

#include "Session.h"
#include "Streaming.h"

namespace py = pybind11;
namespace ddb = dolphindb;

using Session = pydolphindb::Session;
using Streaming = pydolphindb::Streaming;

PYBIND11_MODULE(pydolphindbimpl, m) {
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
