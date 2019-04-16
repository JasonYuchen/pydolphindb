#include "Utils.h"
#include <pybind11/numpy.h>

namespace pydolphindb
{

const module PyModule::numpy_ = module::import("numpy");
const module PyModule::pandas_ = module::import("pandas");

const handle PyFunction::isnan_ = numpy_.attr("isnan");
const handle PyFunction::sum_ = numpy_.attr("sum");

const handle PyType::datetime64_ = numpy_.attr("datetime64");
const handle PyType::pddataframe_ = pandas_.attr("DataFrame")().get_type().inc_ref();
const handle PyType::nparray_ = array().get_type().inc_ref();
const handle PyType::npbool_ = dtype("bool").inc_ref();
const handle PyType::npint8_ = dtype("int8").inc_ref();
const handle PyType::npint16_ = dtype("int16").inc_ref();
const handle PyType::npint32_ = dtype("int32").inc_ref();
const handle PyType::npint64_ = dtype("int64").inc_ref();
const handle PyType::npfloat32_ = dtype("float32").inc_ref();
const handle PyType::npfloat64_ = dtype("float64").inc_ref();
const handle PyType::npdatetime64M_ = dtype("datetime64[M]").inc_ref();
const handle PyType::npdatetime64D_ = dtype("datetime64[D]").inc_ref();
const handle PyType::npdatetime64m_ = dtype("datetime64[m]").inc_ref();
const handle PyType::npdatetime64s_ = dtype("datetime64[s]").inc_ref();
const handle PyType::npdatetime64ms_ = dtype("datetime64[ms]").inc_ref();
const handle PyType::npdatetime64ns_ = dtype("datetime64[ns]").inc_ref();
const handle PyType::npdatetime64_ = dtype("datetime64").inc_ref();
const handle PyType::npobject_ = dtype("object").inc_ref();
const handle PyType::pynone_ = none().get_type().inc_ref();
const handle PyType::pybool_ = bool_().get_type().inc_ref();
const handle PyType::pyint_ = int_().get_type().inc_ref();
const handle PyType::pyfloat_ = float_().get_type().inc_ref();
const handle PyType::pystr_ = str().get_type().inc_ref();
const handle PyType::pybytes_ = bytes().get_type().inc_ref();
const handle PyType::pyset_ = set().get_type().inc_ref();
const handle PyType::pytuple_ = tuple().get_type().inc_ref();
const handle PyType::pylist_ = list().get_type().inc_ref();
const handle PyType::pydict_ = dict().get_type().inc_ref();


std::string Utils::DataCategoryToString(ddb::DATA_CATEGORY cate) noexcept {
    switch (cate) {
        case ddb::NOTHING:
            return "NOTHING";
        case ddb::LOGICAL:
            return "LOGICAL";
        case ddb::INTEGRAL:
            return "INTEGRAL";
        case ddb::FLOATING:
            return "FLOATING";
        case ddb::TEMPORAL:
            return "TEMPORAL";
        case ddb::LITERAL:
            return "LITERAL";
        case ddb::SYSTEM:
            return "SYSTEM";
        case ddb::MIXED:
            return "MIXED";
        default:
            return "UNRECOGNIZED CATEGORY " + std::to_string(cate);
    }
}

std::string Utils::DataFormToString(ddb::DATA_FORM form) noexcept {
    switch (form) {
        case ddb::DF_SCALAR:
            return "SCALAR";
        case ddb::DF_VECTOR:
            return "VECTOR";
        case ddb::DF_PAIR:
            return "PAIR";
        case ddb::DF_MATRIX:
            return "MATRIX";
        case ddb::DF_SET:
            return "SET";
        case ddb::DF_DICTIONARY:
            return "DICTIONARY";
        case ddb::DF_TABLE:
            return "TABLE";
        case ddb::DF_CHART:
            return "CHART";
        case ddb::DF_CHUNK:
            return "CHUNK";
        default:
            return "UNRECOGNIZED FORM " + std::to_string(form);
    }
}

std::string Utils::DataTypeToString(ddb::DATA_TYPE type) noexcept {
    switch (type) {
        case ddb::DT_VOID:
            return "VOID";
        case ddb::DT_BOOL:
            return "BOOL";
        case ddb::DT_CHAR:
            return "CHAR";
        case ddb::DT_SHORT:
            return "SHORT";
        case ddb::DT_INT:
            return "INT";
        case ddb::DT_LONG:
            return "LONG";
        case ddb::DT_DATE:
            return "DATE";
        case ddb::DT_MONTH:
            return "MONTH";
        case ddb::DT_TIME:
            return "TIME";
        case ddb::DT_MINUTE:
            return "MINUTE";
        case ddb::DT_SECOND:
            return "SECOND";
        case ddb::DT_DATETIME:
            return "DATETIME";
        case ddb::DT_TIMESTAMP:
            return "TIMESTAMP";
        case ddb::DT_NANOTIME:
            return "NANOTIME";
        case ddb::DT_NANOTIMESTAMP:
            return "NANOTIMESTAMP";
        case ddb::DT_FLOAT:
            return "FLOAT";
        case ddb::DT_DOUBLE:
            return "DOUBLE";
        case ddb::DT_SYMBOL:
            return "SYMBOL";
        case ddb::DT_STRING:
            return "STRING";
        case ddb::DT_UUID:
            return "UUID";
        case ddb::DT_FUNCTIONDEF:
            return "FUNCTIONDEF";
        case ddb::DT_HANDLE:
            return "HANDLE";
        case ddb::DT_CODE:
            return "CODE";
        case ddb::DT_DATASOURCE:
            return "DATASOURCE";
        case ddb::DT_RESOURCE:
            return "RESOURCE";
        case ddb::DT_ANY:
            return "ANY";
        case ddb::DT_COMPRESS:
            return "COMPRESS";
        case ddb::DT_DICTIONARY:
            return "DICTIONARY";
        case ddb::DT_OBJECT:
            return "OBJECT";
        default:
            return "UNRECOGNIZED TYPE " + std::to_string(type);
    }
}

inline void Utils::SET_NPNAN(void *p, size_t len = 1) {
    std::fill(reinterpret_cast<uint64_t*>(p),
              reinterpret_cast<uint64_t*>(p)+len,
              9221120237041090560LL);
}

inline void Utils::SET_DDBNAN(void *p, size_t len = 1) {
    std::fill(reinterpret_cast<double*>(p),
              reinterpret_cast<double*>(p)+len,
              ddb::DBL_NMIN);
}

inline bool Utils::IS_NPNAN(void *p) {
    return *reinterpret_cast<uint64_t*>(p) == 9221120237041090560LL;
}

}  // namespace pydolphindb
