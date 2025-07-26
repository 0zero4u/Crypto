cmake_minimum_required(VERSION 3.16)
project(LowLatencyListener CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# --- Find Dependencies ---
# Find Boost (required for Beast)
find_package(Boost 1.71.0 REQUIRED COMPONENTS system)

# Find OpenSSL (required for secure websockets)
find_package(OpenSSL REQUIRED)

# --- Add simdjson ---
# Easiest way: add the source files directly to your project folder
# and add them to the executable.
# Or use find_package if installed system-wide.
add_library(simdjson src/simdjson.cpp) # Assuming you placed it in src/
target_include_directories(simdjson PUBLIC src/) # Assuming header is in src/

# --- Build The Executable ---
add_executable(binance_listener_low_latency binance_listener.cpp)

# --- Link Libraries ---
target_link_libraries(binance_listener_low_latency
    PRIVATE
    # For Boost
    Boost::system

    # For OpenSSL
    OpenSSL::SSL
    OpenSSL::Crypto

    # Our simdjson library
    simdjson

    # pthreads for std::thread
    Threads::Threads
)

target_include_directories(binance_listener_low_latency PRIVATE ${Boost_INCLUDE_DIRS})
