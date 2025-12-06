#include <iostream>
#include <thread>
#include <chrono>

int main() {
    std::cout << "--- Indexer Service Started ---" << std::endl;
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }
    return 0;
}
