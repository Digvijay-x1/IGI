#ifndef WARC_WRITER_HPP
#define WARC_WRITER_HPP

#include <string>
#include <fstream>
#include <vector>
#include <cstdint>

namespace crawler {

struct WarcRecordInfo {
    int64_t offset;
    int64_t length;
};

class WarcWriter {
public:
    explicit WarcWriter(const std::string& filename);
    ~WarcWriter();

    // Writes a compressed WARC record and returns its offset and length
    WarcRecordInfo write_record(const std::string& url, const std::string& content);

private:
    std::ofstream file_stream;
    std::string filename;

    std::string create_warc_header(const std::string& url, size_t content_length);
    std::string compress_string(const std::string& str);
    std::string generate_uuid();
};

} // namespace crawler

#endif // WARC_WRITER_HPP
