#include "utils.hpp"

#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <fstream>
#include <algorithm>
#include <set>
#include <thread>
#include <chrono>
#include <pqxx/pqxx>
#include <hiredis/hiredis.h>
#include <rocksdb/db.h>
#include <gumbo.h>

using namespace indexer;

// --- Config ---
const std::string REDIS_HOST = get_env_or_default("REDIS_HOST", "redis_service");
const std::string DB_CONN_STR = build_db_conn_str();
const std::string ROCKSDB_PATH = get_env_or_default("ROCKSDB_PATH", "/shared_data/search_index.db");
const std::string WARC_BASE_PATH = get_env_or_default("WARC_BASE_PATH", "/shared_data/");

int main() {
    std::cout << "--- Indexer Service Started ---" << std::endl;

    // 1. Connect to Redis
    redisContext *redis = redisConnect(REDIS_HOST.c_str(), 6379);
    if (redis == NULL || redis->err) {
        std::cerr << "Redis connection failed" << std::endl;
        return 1;
    }

    // 2. Connect to Postgres
    pqxx::connection* C = nullptr;
    int retries = 10;
    while (retries > 0) {
        try {
            C = new pqxx::connection(DB_CONN_STR);
            if (C->is_open()) {
                std::cout << "Connected to DB" << std::endl;
                break;
            }
        } catch (const std::exception &e) {
            std::cerr << "Postgres connection attempt failed" << std::endl;
            if (C) { delete C; C = nullptr; }
        }
        std::cout << "Retrying Postgres connection in 5 seconds..." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(5));
        retries--;
    }

    if (!C || !C->is_open()) {
        std::cerr << "Failed to connect to Postgres after retries." << std::endl;
        redisFree(redis);
        return 1;
    }

    // 3. Open RocksDB
    rocksdb::DB* db;
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status status = rocksdb::DB::Open(options, ROCKSDB_PATH, &db);
    if (!status.ok()) {
        std::cerr << "RocksDB Open failed: " << status.ToString() << std::endl;
        delete C;
        redisFree(redis);
        return 1;
    }

    while (true) {
        // A. Pop from Queue
        redisReply *reply = (redisReply*)redisCommand(redis, "BLPOP indexing_queue 0");
        if (reply == NULL || reply->type != REDIS_REPLY_ARRAY) {
            if (reply) freeReplyObject(reply);
            continue; // Should not happen with BLPOP unless timeout/error
        }

        if (reply->elements < 2 || reply->element[1] == nullptr || reply->element[1]->str == nullptr) {
            freeReplyObject(reply);
            continue;
        }

        std::string doc_id_str = reply->element[1]->str;
        int doc_id;
        try {
            doc_id = std::stoi(doc_id_str);
        } catch (const std::exception&) {
            freeReplyObject(reply);
            continue;
        }
        freeReplyObject(reply);
        
        std::cout << "Indexing Doc ID: " << doc_id << std::endl;

        try {
            // B. Get Metadata
            pqxx::work W(*C);
            pqxx::row row = W.exec_params1("SELECT file_path, \"offset\", length FROM documents WHERE id = $1", doc_id);
            std::string file_path = WARC_BASE_PATH + row[0].as<std::string>();
            long offset = row[1].as<long>();
            long length = row[2].as<long>();
            W.commit();

            // C. Read WARC Record
            std::ifstream infile(file_path, std::ios::binary);
            if (!infile) {
                std::cerr << "Could not open file: " << file_path << std::endl;
                continue;
            }
            infile.seekg(offset);
            std::vector<char> buffer(length);
            infile.read(buffer.data(), length);
            std::streamsize readBytes = infile.gcount();
            if (readBytes != length) {
                std::cerr << "Failed to read full record: expected " << length << " bytes, got " << readBytes << std::endl;
                continue;
            }
            std::string compressed_data(buffer.begin(), buffer.end());
            
            // D. Decompress & Parse
            std::string full_warc_record = decompress_gzip(compressed_data);
            // Skip WARC headers (find first double newline)
            size_t header_end = full_warc_record.find("\r\n\r\n");
            if (header_end == std::string::npos) continue;
            
            std::string html_content = full_warc_record.substr(header_end + 4);
            
            GumboOutput* output = gumbo_parse(html_content.c_str());
            std::string plain_text = clean_text(output->root);
            gumbo_destroy_output(&kGumboDefaultOptions, output);

            // E. Tokenize & Index
            std::vector<std::string> tokens = tokenize(plain_text);
            std::set<std::string> unique_tokens(tokens.begin(), tokens.end()); // Simple boolean index for now

            for (const auto& token : unique_tokens) {
                std::string current_list;
                status = db->Get(rocksdb::ReadOptions(), token, &current_list);
                
                std::set<std::string> doc_ids;
                if (status.ok() && !current_list.empty()) {
                    std::stringstream ss(current_list);
                    std::string id;
                    while (std::getline(ss, id, ',')) {
                        doc_ids.insert(id);
                    }
                }
                
                std::string doc_id_str = std::to_string(doc_id);
                if (doc_ids.find(doc_id_str) == doc_ids.end()) {
                    doc_ids.insert(doc_id_str);
                    current_list = "";
                    for (auto it = doc_ids.begin(); it != doc_ids.end(); ++it) {
                        if (it != doc_ids.begin()) current_list += ",";
                        current_list += *it;
                    }
                    db->Put(rocksdb::WriteOptions(), token, current_list);
                }
            }

            // F. Update Doc Length
            pqxx::work W2(*C);
            W2.exec_params("UPDATE documents SET doc_length = $1 WHERE id = $2", tokens.size(), doc_id);
            W2.commit();
            
            std::cout << "Indexed " << tokens.size() << " words for Doc " << doc_id << std::endl;

        } catch (const std::exception &e) {
            std::cerr << "Error indexing doc " << doc_id << ": " << e.what() << std::endl;
        }
    }

    delete db;
    delete C;
    redisFree(redis);
    return 0;
}
