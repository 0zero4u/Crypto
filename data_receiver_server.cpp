#include <uwebsockets/App.h>
#include <iostream>
#include <string>
#include <vector>
#include <csignal>

// --- Configuration ---
const int PUBLIC_PORT = 8081;
const int INTERNAL_LISTENER_PORT = 8082;
// Disconnect clients if they don't respond to a ping within 20 seconds.
const int CLIENT_IDLE_TIMEOUT_SECONDS = 20; 
// The topic that clients subscribe to and the listener publishes to.
const std::string BROADCAST_TOPIC = "trades";

// --- Application State ---
// uWebSockets is not thread-safe by default, but we run it on a single thread.
// This pointer allows the signal handler to gracefully shut down the server.
struct us_listen_socket_t *public_listen_socket = nullptr;
struct us_listen_socket_t *internal_listen_socket = nullptr;

// Per-client data
struct PerSocketData {
    int clientId;
};

// --- Graceful Shutdown ---
void signalHandler(int signum) {
    std::cout << "\n[Receiver] Signal (" << signum << ") received. Shutting down." << std::endl;
    
    if (public_listen_socket) {
        uWS::us_listen_socket_close(0, public_listen_socket);
        public_listen_socket = nullptr;
    }
    if (internal_listen_socket) {
        uWS::us_listen_socket_close(0, internal_listen_socket);
        internal_listen_socket = nullptr;
    }
}

int main() {
    // Register signal handlers for graceful shutdown
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);

    int client_id_counter = 0;

    uWS::App()
        // --- Handler for Public Android Clients ---
        .ws<PerSocketData>("/public", {
            /* Settings */
            .compression = uWS::SHARED_COMPRESSOR,
            .maxPayloadLength = 512, // Small payload expected
            .idleTimeout = CLIENT_IDLE_TIMEOUT_SECONDS,

            /* Handlers */
            .upgrade = [&client_id_counter](auto *res, auto *req, auto *context) {
                // This is called before the connection is established.
                // We can reject connections here if needed.
                 res->template upgrade<PerSocketData>({
                    .clientId = client_id_counter++
                }, req->getHeader("sec-websocket-key"),
                    req->getHeader("sec-websocket-protocol"),
                    req->getHeader("sec-websocket-extensions"),
                    context);
            },
            .open = [](auto *ws) {
                auto* data = ws->getUserData();
                std::cout << "[Receiver] Public client connected. ID: " << data->clientId << std::endl;
                ws->subscribe(BROADCAST_TOPIC);
            },
            .message = [](auto *ws, std::string_view message, uWS::OpCode opCode) {
                // Public clients are listen-only, ignore any incoming messages.
            },
            .close = [](auto *ws, int code, std::string_view message) {
                auto* data = ws->getUserData();
                std::cout << "[Receiver] Public client disconnected. ID: " << data->clientId << std::endl;
            }
        })
        // --- Handler for the Internal Binance Listener ---
        .ws<PerSocketData>("/internal", {
            /* Settings */
            .compression = uWS::DISABLED,
            .maxPayloadLength = 1024,
            .idleTimeout = 0, // No timeout for the internal listener

            /* Handlers */
            .open = [](auto *ws) {
                std::cout << "[Receiver] Internal listener connected." << std::endl;
            },
            .message = [](auto *ws, std::string_view message, uWS::OpCode opCode) {
                // Message received from the listener, broadcast it to all public clients.
                ws->publish(BROADCAST_TOPIC, message, opCode, false);
            },
            .close = [](auto *ws, int code, std::string_view message) {
                std::cout << "[Receiver] Internal listener disconnected." << std::endl;
            }
        })
        // --- Port Listeners ---
        .listen("0.0.0.0", PUBLIC_PORT, [&public_listen_socket](auto *socket) {
            if (socket) {
                public_listen_socket = socket;
                std::cout << "[Receiver] Public server listening on port " << PUBLIC_PORT << std::endl;
            }
        })
        .listen("0.0.0.0", INTERNAL_LISTENER_PORT, [&internal_listen_socket](auto *socket) {
            if (socket) {
                internal_listen_socket = socket;
                std::cout << "[Receiver] Internal server listening on port " << INTERNAL_LISTENER_PORT << std::endl;
            }
        })
        .run(); // This call blocks until the server is shut down

    std::cout << "[Receiver] Server has been shut down." << std::endl;
    return 0;
        }
