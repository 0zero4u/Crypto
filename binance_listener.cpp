#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio/strand.hpp>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <csignal>
#include <atomic>

// The world's fastest JSON parser
#include "simdjson.h"

namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
namespace net = boost::asio;
using tcp = boost::asio::ip::tcp;

// --- Configuration ---
const std::string UPSTREAM_HOST = "fstream.binance.com";
const std::string UPSTREAM_PORT = "443";
const std::string UPSTREAM_PATH = "/ws/btcusdt@trade";

const std::string DOWNSTREAM_HOST = "127.0.0.1";
const std::string DOWNSTREAM_PORT = "8082";
const std::string DOWNSTREAM_PATH = "/internal";

const double MINIMUM_TICK_SIZE = 0.2;
const int RECONNECT_INTERVAL_MS = 3000;

// --- Global State ---
std::atomic<bool> g_running(true);
std::atomic<double> g_last_sent_trade_price(0.0);

// --- Thread-Safe Queue for Inter-thread Communication ---
template<typename T>
class ThreadSafeQueue {
public:
    void push(T value) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_queue.push(std::move(value));
        m_cond.notify_one();
    }

    bool pop(T& value) {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_cond.wait(lock, [this]{ return !m_queue.empty() || !g_running; });
        if (!g_running) return false;
        value = std::move(m_queue.front());
        m_queue.pop();
        return true;
    }

private:
    std::queue<T> m_queue;
    std::mutex m_mutex;
    std::condition_variable m_cond;
};

// --- Low-Level WebSocket Client Class ---
class WebSocketClient {
public:
    WebSocketClient(
        std::string host,
        std::string port,
        std::string path,
        std::function<void(const std::string&)> on_message
    ) : m_host(host), m_port(port), m_path(path), m_on_message(on_message), m_ioc(), m_ws(m_ioc) {}

    void run() {
        while (g_running) {
            try {
                tcp::resolver resolver(m_ioc);
                auto const results = resolver.resolve(m_host, m_port);
                net::connect(m_ws.next_layer(), results.begin(), results.end());
                m_ws.handshake(m_host, m_path);

                std::cout << "[Client:" << m_host << "] Connected." << std::endl;

                // Enter the read loop
                while(g_running) {
                    beast::flat_buffer buffer;
                    m_ws.read(buffer);
                    m_on_message(beast::buffers_to_string(buffer.data()));
                }
            } catch (const beast::system_error& se) {
                if (se.code() == websocket::error::closed) {
                     // This is a graceful closure
                } else {
                    std::cerr << "[Client:" << m_host << "] Beast Error: " << se.what() << std::endl;
                }
            } catch (const std::exception& e) {
                std::cerr << "[Client:" << m_host << "] Error: " << e.what() << std::endl;
            }

            if (!g_running) break;

            // Cleanup before reconnecting
            if (m_ws.is_open()) {
                try { m_ws.close(websocket::close_code::normal); } catch(...) {}
            }
            m_ioc.restart();
            m_ws = websocket::stream<tcp::socket>(m_ioc);

            std::this_thread::sleep_for(std::chrono::milliseconds(RECONNECT_INTERVAL_MS));
            std::cerr << "[Client:" << m_host << "] Reconnecting..." << std::endl;
        }
    }

    void send(const std::string& message) {
        try {
            if (m_ws.is_open()) {
                m_ws.write(net::buffer(message));
            }
        } catch (const std::exception& e) {
             std::cerr << "[Client:" << m_host << "] Send Error: " << e.what() << std::endl;
        }
    }

    void stop() {
        try {
            if (m_ws.is_open()) {
                m_ws.close(websocket::close_code::normal);
            }
        } catch(...) {}
    }


private:
    std::string m_host;
    std::string m_port;
    std::string m_path;
    std::function<void(const std::string&)> m_on_message;
    net::io_context m_ioc;
    websocket::stream<tcp::socket> m_ws;
};


// --- Main ---
void signalHandler(int signum) {
    std::cout << "\n[Main] Signal (" << signum << ") received. Shutting down." << std::endl;
    g_running = false;
}

int main() {
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);

    ThreadSafeQueue<std::string> message_queue;

    // --- Upstream Thread (Binance) ---
    // This thread's only job is to receive from Binance and push to a queue.
    std::thread upstream_thread([&message_queue]() {
        // Allocate the high-speed parser once and reuse it.
        simdjson::ondemand::parser parser;

        auto on_binance_message = [&](const std::string& msg) {
            try {
                simdjson::ondemand::document doc = parser.iterate(msg);
                if (std::string_view(doc["e"]) == "trade") {
                    // Extract price as a string_view to avoid allocation
                    std::string_view price_sv = doc["p"];
                    double current_trade_price = std::stod(std::string(price_sv));
                    
                    double last_price = g_last_sent_trade_price.load(std::memory_order_relaxed);

                    if (last_price == 0.0) {
                        g_last_sent_trade_price.store(current_trade_price);
                        return;
                    }

                    double price_difference = current_trade_price - last_price;

                    if (std::abs(price_difference) >= MINIMUM_TICK_SIZE) {
                        // Manual JSON construction: ultra-fast, no library overhead
                        std::string payload = "{\"type\":\"S\",\"p\":" + std::string(price_sv) + "}";
                        message_queue.push(payload);
                        g_last_sent_trade_price.store(current_trade_price);
                    }
                }
            } catch (const std::exception& e) {
                // This can happen if Binance sends a non-trade message or on parse error
                // std::cerr << "[Upstream] JSON process error: " << e.what() << std::endl;
            }
        };

        WebSocketClient binance_client(UPSTREAM_HOST, UPSTREAM_PORT, UPSTREAM_PATH, on_binance_message);
        binance_client.run();
        binance_client.stop();
        std::cout << "[Upstream] Thread finished." << std::endl;
    });

    // --- Downstream Thread (Internal Receiver) ---
    // This thread's only job is to pop from the queue and send to the receiver.
    std::thread downstream_thread([&message_queue]() {
        WebSocketClient receiver_client(DOWNSTREAM_HOST, DOWNSTREAM_PORT, DOWNSTREAM_PATH, 
            [](const std::string& msg){ /* We don't expect messages from the receiver */ });

        std::thread sender_thread([&]() {
            receiver_client.run();
            receiver_client.stop();
        });

        std::string msg_to_send;
        while (message_queue.pop(msg_to_send)) {
            receiver_client.send(msg_to_send);
        }
        
        if (sender_thread.joinable()) sender_thread.join();
        std::cout << "[Downstream] Thread finished." << std::endl;
    });

    // Wait for shutdown signal
    if (upstream_thread.joinable()) upstream_thread.join();
    if (downstream_thread.joinable()) downstream_thread.join();

    std::cout << "[Main] Application terminated." << std::endl;
    return 0;
    }
