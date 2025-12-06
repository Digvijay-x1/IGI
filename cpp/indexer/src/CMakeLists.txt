cmake_minimum_required(VERSION 3.10)
project(Indexer)

set(CMAKE_CXX_STANDARD 17)

find_package(ZLIB REQUIRED)

add_executable(indexer main.cpp)

target_link_libraries(indexer pqxx pq hiredis rocksdb gumbo z)
