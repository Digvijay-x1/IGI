cmake_minimum_required(VERSION 3.10)
project(Indexer)

set(CMAKE_CXX_STANDARD 17)

find_package(ZLIB REQUIRED)

add_executable(indexer main.cpp utils.cpp)

target_link_libraries(indexer pqxx pq hiredis rocksdb gumbo z)

# Testing
enable_testing()

add_executable(test_indexer ../tests/test_utils.cpp utils.cpp)
target_link_libraries(test_indexer gumbo z)

add_executable(test_integration ../tests/test_integration.cpp utils.cpp ../../crawler/src/warc_writer.cpp)
target_link_libraries(test_integration gumbo z)

add_test(NAME IndexerUtilsTest COMMAND test_indexer)
add_test(NAME IndexerIntegrationTest COMMAND test_integration)
