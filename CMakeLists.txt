sudo apt-get update && \
sudo apt-get install -y libboost-all-dev libssl-dev && \
cat <<'EOF' > ~/CryptoLive/CMakeLists.txt
cmake_minimum_required(VERSION 3.16)
project(CryptoFeed CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# --- Find ALL Dependencies ---
find_package(uWebSockets REQUIRED)
find_package(Boost 1.71.0 REQUIRED COMPONENTS system)
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)

# --- Add simdjson ---
add_library(simdjson src/simdjson.cpp)
target_include_directories(simdjson PUBLIC src/)

# --- Build The Executables ---
add_executable(data_receiver_server data_receiver_server.cpp)
add_executable(binance_listener_low_latency binance_listener.cpp)

# --- Link Libraries For BOTH Executables ---

# Link the Receiver to the uWebSockets library
target_link_libraries(data_receiver_server PRIVATE uWebSockets::uWebSockets)

# Link the Listener to ALL of its dependencies
target_link_libraries(binance_listener_low_latency
    PRIVATE
    Boost::system
    OpenSSL::SSL
    OpenSSL::Crypto
    simdjson
    Threads::Threads
)

target_include_directories(binance_listener_low_latency PRIVATE ${Boost_INCLUDE_DIRS})
EOF

cd ~/CryptoLive && \
rm -rf build && \
mkdir build && \
cd build && \
cmake .. && \
make
