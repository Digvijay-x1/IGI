#include "../src/utils.hpp"
#include "../../crawler/src/warc_writer.hpp"
#include <iostream>
#include <fstream>
#include <cassert>
#include <filesystem>
#include <vector>

// Simple assertion macro
#define ASSERT(condition, message) \
    do { \
        if (!(condition)) { \
            std::cerr << "Assertion failed: " << (message) << "\n" \
                      << "File: " << __FILE__ << ", Line: " << __LINE__ << std::endl; \
            std::exit(EXIT_FAILURE); \
        } \
    } while (false)

void test_crawler_indexer_integration() {
    std::string filename = "test_integration.warc.gz";
    if (std::filesystem::exists(filename)) {
        std::filesystem::remove(filename);
    }

    std::string url = "http://example.com";
    std::string content = "<html><body>Integration Test</body></html>";
    long offset = 0;
    long length = 0;

    // 1. Crawler writes a record
    {
        crawler::WarcWriter writer(filename);
        auto info = writer.write_record(url, content);
        offset = info.offset;
        length = info.length;
    }

    ASSERT(std::filesystem::exists(filename), "WARC file should exist");

    // 2. Indexer reads and processes the record
    std::ifstream infile(filename, std::ios::binary);
    ASSERT(infile.is_open(), "Indexer should be able to open the file");

    infile.seekg(offset);
    std::vector<char> buffer(length);
    infile.read(buffer.data(), length);
    
    ASSERT(infile.gcount() == length, "Indexer should read the exact length");

    std::string compressed_data(buffer.begin(), buffer.end());
    std::string full_warc_record = indexer::decompress_gzip(compressed_data);

    // Verify content is present in the decompressed record
    ASSERT(full_warc_record.find(content) != std::string::npos, "Decompressed record should contain original HTML");
    ASSERT(full_warc_record.find(url) != std::string::npos, "Decompressed record should contain URL");

    std::filesystem::remove(filename);
    std::cout << "test_crawler_indexer_integration passed" << std::endl;
}

int main() {
    try {
        test_crawler_indexer_integration();
        std::cout << "All integration tests passed!" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Integration test failed: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
