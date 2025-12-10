#ifndef INDEXER_UTILS_HPP
#define INDEXER_UTILS_HPP

#include <string>
#include <vector>
#include <gumbo.h>

namespace indexer {

// Get an environment variable or a default value.
std::string get_env_or_default(const char* var, const std::string& def);

// Build the database connection string from environment variables.
std::string build_db_conn_str();

// Extract clean text from a Gumbo parse tree, ignoring script/style tags.
// Also extracts the title if found.
struct ExtractedContent {
    std::string text;
    std::string title;
};
ExtractedContent extract_content(GumboNode* node);

// Decompress a gzip-compressed string.
std::string decompress_gzip(const std::string& compressed_data);

// Tokenize a string into words (lowercase, alphanumeric, min length 3).
std::vector<std::string> tokenize(const std::string& text);

} // namespace indexer

#endif // INDEXER_UTILS_HPP
