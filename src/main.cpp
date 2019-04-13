#include <pybind11/pybind11.h>
#include <DolphinDB.h>

int add(int i, int j) {
    dolphindb::DBConnection conn;
    conn.connect("115.239.209.189",18651);
    return i + j;
}

PYBIND11_MODULE(fastdolphindb, m) {
    m.doc() = "pybind11 example plugin"; // optional module docstring

    m.def("add", &add, "A function which adds two numbers");

}
