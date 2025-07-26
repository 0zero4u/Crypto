const uWS = require('uWebSockets.js');

// --- Server Configuration ---
const PUBLIC_PORT = 8081;
const INTERNAL_LISTENER_PORT = 8082;
// uWS's built-in idle timeout is more efficient than manual ping/pong.
// A client will be disconnected if no data is sent or received for this duration.
const ANDROID_CLIENT_IDLE_TIMEOUT_S = 130; // 130 seconds (a bit more than the 120s heartbeat)
const SHUTDOWN_TIMEOUT_MS = 5000;

// --- Global Error Handlers ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[Receiver] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`, err.stack || err);
    process.exit(1);
});
process.on('unhandledRejection', (reason, promise) => {
    console.error(`[Receiver] PID: ${process.pid} --- FATAL: UNHANDLED PROMISE REJECTION`, reason);
    process.exit(1);
});

// --- Manually manage the list of connected clients, like the original script ---
const publicClients = new Set();
console.log(`[Receiver] PID: ${process.pid} --- Initializing uWebSockets.js server...`);

let listenSocketPublic, listenSocketInternal;

// --- Broadcast function, similar to the original script's logic ---
function broadcastToPublicClients(message, isBinary) {
    if (publicClients.size > 0) {
        publicClients.forEach(client => {
            // Check backpressure before sending to avoid overwhelming a client
            if (client.getBufferedAmount() === 0) {
                client.send(message, isBinary);
            }
        });
    }
}

const app = uWS.App({})
    // --- Internal Server (for binance_listener.js) ---
    .ws('/internal', {
        idleTimeout: 0, // No timeout for the trusted internal listener
        open: (ws) => {
            const listenerIp = Buffer.from(ws.getRemoteAddressAsText()).toString();
            console.log(`[Receiver] binance_listener.js connected internally from ${listenerIp}`);
        },
        message: (ws, message, isBinary) => {
            // A message from the listener triggers a broadcast to all public clients
            broadcastToPublicClients(message, isBinary);
        },
        close: (ws, code, message) => {
            const listenerIp = Buffer.from(ws.getRemoteAddressAsText()).toString();
            console.log(`[Receiver] binance_listener.js disconnected internally from ${listenerIp}. Code: ${code}`);
        }
    })
    // --- Public Server (for Android Clients) ---
    .ws('/public', {
        idleTimeout: ANDROID_CLIENT_IDLE_TIMEOUT_S,
        open: (ws) => {
            const clientIp = Buffer.from(ws.getRemoteAddressAsText()).toString();
            console.log(`[Receiver] Android client connected from IP: ${clientIp}. Total clients: ${publicClients.size + 1}`);
            // Add the new client to our manually managed list
            publicClients.add(ws);
        },
        message: (ws, message, isBinary) => {
            // Android clients are not expected to send messages
        },
        close: (ws, code, message) => {
            const clientIp = Buffer.from(ws.getRemoteAddressAsText()).toString();
            // Remove the disconnected client from our list
            publicClients.delete(ws);
            console.log(`[Receiver] Android client disconnected from IP: ${clientIp}. Code: ${code}. Total clients: ${publicClients.size}`);
        }
    })
    .listen('0.0.0.0', INTERNAL_LISTENER_PORT, (token) => {
        listenSocketInternal = token;
        if (token) {
            console.log(`[Receiver] Internal server listening for listener on port ${INTERNAL_LISTENER_PORT}`);
        } else {
            console.error(`[Receiver] FATAL: Failed to listen on internal port ${INTERNAL_LISTENER_PORT}`);
            process.exit(1);
        }
    })
    .listen('0.0.0.0', PUBLIC_PORT, (token) => {
        listenSocketPublic = token;
        if (token) {
            console.log(`[Receiver] Public server for Android clients listening on port ${PUBLIC_PORT}`);
        } else {
            console.error(`[Receiver] FATAL: Failed to listen on public port ${PUBLIC_PORT}`);
            process.exit(1);
        }
    });

// --- Graceful Shutdown ---
function initiateShutdown(signal) {
    console.log(`\n[Receiver] PID: ${process.pid} --- ${signal} received. Shutting down.`);

    if (listenSocketPublic) {
        uWS.us_listen_socket_close(listenSocketPublic);
        listenSocketPublic = null;
    }
    if (listenSocketInternal) {
        uWS.us_listen_socket_close(listenSocketInternal);
        listenSocketInternal = null;
    }

    setTimeout(() => {
        console.log('[Receiver] Exiting.');
        process.exit(0);
    }, 1000).unref(); // Allow 1 second for sockets to close
}

process.on('SIGINT', () => initiateShutdown('SIGINT'));
process.on('SIGTERM', () => initiateShutdown('SIGTERM'));
