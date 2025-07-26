cmake_minimum_required(VERSION 3.16)
project(CryptoFeed CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# --- Find Dependencies ---
# Find Boost (required for Beast)
find_package(Boost 1.71.0 REQUIRED COMPONENTS system)

# Find OpenSSL (required for secure websockets)
find_package(OpenSSL REQUIRED)

# Find the pthreads library (REQUIRED FOR std::thread)
find_package(Threads REQUIRED)

# --- Add simdjson ---
# We tell CMake to build a library from the source files in the src/ directory
add_library(simdjson src/simdjson.cpp)
target_include_directories(simdjson PUBLIC src/)

# --- Build The Executables ---
add_executable(data_receiver_server data_receiver_server.cpp)
add_executable(binance_listener_low_latency binance_listener.cpp)

# --- Link Libraries ---
target_link_libraries(data_receiver_server PRIVATE) # No special libs needed for this one

target_link_libraries(binance_listener_low_latency
    PRIVATE
    # For Boost
    Boost::system

    # For OpenSSL
    OpenSSL::SSL
    OpenSSL::Crypto

    # Our simdjson library
    simdjson

    # For std::thread
    Threads::Threads
)

target_include_directories(binance_listener_low_latency PRIVATE ${Boost_INCLUDE_DIRS})
