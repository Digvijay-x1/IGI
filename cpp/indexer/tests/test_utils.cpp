#include "../src/utils.hpp"
#include <iostream>
#include <cassert>
#include <vector>
#include <string>
#include <gumbo.h>
#include <sstream>
#include <zlib.h>

// Simple assertion macro
#define ASSERT(condition, message) \
    do { \
        if (!(condition)) { \
            std::cerr << "Assertion failed: " << (message) << "\n" \
                      << "File: " << __FILE__ << ", Line: " << __LINE__ << std::endl; \
            std::exit(EXIT_FAILURE); \
        } \
    } while (false)

// --- Test: tokenize ---
void test_tokenize_basic() {
    auto tokens = indexer::tokenize("Hello World");
    ASSERT(tokens.size() == 2, "Should have 2 tokens");
    ASSERT(tokens[0] == "hello", "First token should be 'hello'");
    ASSERT(tokens[1] == "world", "Second token should be 'world'");
    std::cout << "test_tokenize_basic passed" << std::endl;
}

void test_tokenize_min_length() {
    auto tokens = indexer::tokenize("a ab abc abcd");
    // "a" and "ab" should be filtered out (< 3 chars)
    ASSERT(tokens.size() == 2, "Should have 2 tokens");
    ASSERT(tokens[0] == "abc", "First token should be 'abc'");
    ASSERT(tokens[1] == "abcd", "Second token should be 'abcd'");
    std::cout << "test_tokenize_min_length passed" << std::endl;
}

void test_tokenize_special_chars() {
    auto tokens = indexer::tokenize("hello-world, this is a test!");
    ASSERT(tokens.size() == 4, "Should have 4 tokens");
    std::cout << "test_tokenize_special_chars passed" << std::endl;
}

// --- Test: clean_text ---
void test_clean_text_simple() {
    const char* html = "<html><body><p>Hello World</p></body></html>";
    GumboOutput* output = gumbo_parse(html);
    std::string text = indexer::clean_text(output->root);
    gumbo_destroy_output(&kGumboDefaultOptions, output);
    // Text should contain "Hello World" (with possible surrounding whitespace)
    ASSERT(text.find("Hello World") != std::string::npos, "Should extract 'Hello World'");
    std::cout << "test_clean_text_simple passed" << std::endl;
}

void test_clean_text_ignores_script() {
    const char* html = "<html><body><script>alert('evil')</script><p>Clean</p></body></html>";
    GumboOutput* output = gumbo_parse(html);
    std::string text = indexer::clean_text(output->root);
    gumbo_destroy_output(&kGumboDefaultOptions, output);
    ASSERT(text.find("alert") == std::string::npos, "Should not contain script content");
    ASSERT(text.find("Clean") != std::string::npos, "Should contain 'Clean'");
    std::cout << "test_clean_text_ignores_script passed" << std::endl;
}

void test_clean_text_ignores_style() {
    const char* html = "<html><head><style>body{color:red}</style></head><body><p>Styled</p></body></html>";
    GumboOutput* output = gumbo_parse(html);
    std::string text = indexer::clean_text(output->root);
    gumbo_destroy_output(&kGumboDefaultOptions, output);
    ASSERT(text.find("color") == std::string::npos, "Should not contain style content");
    ASSERT(text.find("Styled") != std::string::npos, "Should contain 'Styled'");
    std::cout << "test_clean_text_ignores_style passed" << std::endl;
}

// --- Test: decompress_gzip ---
// Helper to compress a string with gzip
std::string compress_gzip(const std::string& data) {
    z_stream zs;
    zs.zalloc = Z_NULL;
    zs.zfree = Z_NULL;
    zs.opaque = Z_NULL;

    if (deflateInit2(&zs, Z_BEST_COMPRESSION, Z_DEFLATED, 16 + MAX_WBITS, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
        throw std::runtime_error("deflateInit2 failed");
    }

    zs.next_in = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(data.data()));
    zs.avail_in = data.size();

    std::string output;
    char buffer[32768];

    do {
        zs.next_out = reinterpret_cast<Bytef*>(buffer);
        zs.avail_out = sizeof(buffer);

        int ret = deflate(&zs, Z_FINISH);
        if (ret == Z_STREAM_ERROR) {
            deflateEnd(&zs);
            throw std::runtime_error("deflate failed");
        }

        output.append(buffer, sizeof(buffer) - zs.avail_out);
    } while (zs.avail_out == 0);

    deflateEnd(&zs);
    return output;
}

void test_decompress_gzip_basic() {
    std::string original = "This is a test string for gzip compression.";
    std::string compressed = compress_gzip(original);
    std::string decompressed = indexer::decompress_gzip(compressed);
    ASSERT(decompressed == original, "Decompressed data should match original");
    std::cout << "test_decompress_gzip_basic passed" << std::endl;
}

void test_decompress_gzip_empty() {
    std::string original = "";
    std::string compressed = compress_gzip(original);
    std::string decompressed = indexer::decompress_gzip(compressed);
    ASSERT(decompressed == original, "Decompressed empty string should be empty");
    std::cout << "test_decompress_gzip_empty passed" << std::endl;
}

int main() {
    try {
        test_tokenize_basic();
        test_tokenize_min_length();
        test_tokenize_special_chars();
        test_clean_text_simple();
        test_clean_text_ignores_script();
        test_clean_text_ignores_style();
        test_decompress_gzip_basic();
        test_decompress_gzip_empty();
        std::cout << "All tests passed!" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
