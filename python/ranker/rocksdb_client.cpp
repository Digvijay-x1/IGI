#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <rocksdb/db.h>
#include <string>
#include <stdexcept>

namespace py = pybind11;

class RocksDBReader {
    rocksdb::DB* db;
    bool is_open;
public:
    RocksDBReader(const std::string& path) : db(nullptr), is_open(false) {
        rocksdb::Options options;
        // Use default comparator (Bytewise)
        rocksdb::Status status = rocksdb::DB::OpenForReadOnly(options, path, &db);
        if (!status.ok()) {
            throw std::runtime_error("Failed to open RocksDB: " + status.ToString());
        }
        is_open = true;
    }

    ~RocksDBReader() {
        close();
    }

    py::object get(const py::bytes& key) {
        if (!is_open) return py::none();
        
        std::string key_str = key;
        std::string value;
        rocksdb::Status status = db->Get(rocksdb::ReadOptions(), key_str, &value);
        
        if (status.IsNotFound()) {
            return py::none();
        }
        if (!status.ok()) {
            throw std::runtime_error("Error reading key: " + status.ToString());
        }
        return py::bytes(value);
    }
    
    void close() {
        if (is_open && db) {
            delete db;
            db = nullptr;
            is_open = false;
        }
    }
};

PYBIND11_MODULE(rocksdb_client, m) {
    py::class_<RocksDBReader>(m, "RocksDBReader")
        .def(py::init<const std::string&>())
        .def("get", &RocksDBReader::get)
        .def("close", &RocksDBReader::close);
}
