#include <iostream>
#include <thread>
#include <chrono>

int main() {
    std::cout << "Crawler Service Started..." << std::endl;
    
    while (true) {
        std::cout << "Crawler is idle. Waiting for jobs..." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }
    
    return 0;
}
