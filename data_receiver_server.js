// data_receiver_server.js (Modified for minimal JSON string, adjusted heartbeats, and reduced logging)
// CORRECTED VERSION: Removed restrictive internal IP check to allow cloud VM connections managed by firewall rules.

const WebSocket = require('ws');

const PUBLIC_PORT = 8081;
const INTERNAL_LISTENER_PORT = 8082;
// Adjusted Heartbeat Frequency: Ping every 120 seconds. Client has 120 seconds to respond with a pong.
const ANDROID_CLIENT_HEARTBEAT_INTERVAL_MS = 120000;
const SHUTDOWN_TIMEOUT_MS = 5000;

// --- Global State ---
let wssAndroidClients = null;
let wssListenerSource = null;
let shuttingDown = false;
let forceExitTimer = null;

// --- Unified Shutdown Logic ---
function attemptServerClosure(server, serverName, onDone) {
    if (!server) {
        onDone();
        return;
    }

    console.log(`[Receiver] PID: ${process.pid} --- Closing ${serverName} and terminating its clients...`);

    if (server.clients && typeof server.clients.forEach === 'function') {
        server.clients.forEach(client => {
            if (client.readyState === WebSocket.OPEN) {
                client.terminate();
            }
            if (client.heartbeatInterval) {
                clearInterval(client.heartbeatInterval);
            }
        });
    }

    server.close((err) => {
        if (err) {
            console.error(`[Receiver] PID: ${process.pid} --- Error closing ${serverName}: ${err.message}`);
        } else {
            console.log(`[Receiver] PID: ${process.pid} --- ${serverName} closed.`);
        }
        onDone();
    });
}

function initiateShutdown(signal, exitCode = 1) {
    if (shuttingDown) {
        return;
    }
    shuttingDown = true;
    console.log(`\n[Receiver] PID: ${process.pid} --- ${signal} signal received. Initiating shutdown sequence...`);

    if (forceExitTimer) clearTimeout(forceExitTimer);
    forceExitTimer = setTimeout(() => {
        console.error(`[Receiver] PID: ${process.pid} --- Shutdown timeout (${SHUTDOWN_TIMEOUT_MS}ms). Forcing exit.`);
        process.exit(exitCode);
    }, SHUTDOWN_TIMEOUT_MS).unref();

    let serversPendingClosure = 0;
    if (wssAndroidClients) serversPendingClosure++;
    if (wssListenerSource) serversPendingClosure++;

    if (serversPendingClosure === 0) {
        console.log(`[Receiver] PID: ${process.pid} --- No servers were initialized or running. Exiting.`);
        if (forceExitTimer) clearTimeout(forceExitTimer);
        setTimeout(() => process.exit(exitCode), 200);
        return;
    }

    const onServerProcessed = () => {
        serversPendingClosure--;
        if (serversPendingClosure <= 0) {
            if (forceExitTimer) clearTimeout(forceExitTimer);
            console.log(`[Receiver] PID: ${process.pid} --- All server shutdown procedures attempted. Exiting gracefully.`);
            setTimeout(() => process.exit(exitCode), 200);
        }
    };

    if (wssAndroidClients) {
        attemptServerClosure(wssAndroidClients, 'Public WebSocket server (Android clients)', onServerProcessed);
    }
    if (wssListenerSource) {
        attemptServerClosure(wssListenerSource, 'Internal WebSocket server (listener source)', onServerProcessed);
    }
}

// --- Global Error Handlers ---
process.on('unhandledRejection', (reason, promise) => {
  console.error(`[Receiver] PID: ${process.pid} --- FATAL: Unhandled Rejection at:`, promise, 'reason:', reason instanceof Error ? reason.stack : reason);
  initiateShutdown('UNHANDLED_REJECTION', 1);
});

process.on('uncaughtException', (error) => {
  console.error(`[Receiver] PID: ${process.pid} --- FATAL: Uncaught Exception:`, error.stack || error);
  initiateShutdown('UNCAUGHT_EXCEPTION', 1);
});

// --- Public WebSocket Server (for Android Clients) ---
try {
    wssAndroidClients = new WebSocket.Server({ port: PUBLIC_PORT });
    console.log(`[Receiver] PID: ${process.pid} --- Public WebSocket server for Android clients started on port ${PUBLIC_PORT}`);

    let androidClientCounter = 0;

    wssAndroidClients.on('connection', (ws, req) => {
        ws.clientId = `android-${androidClientCounter++}`;
        const clientIp = req.socket.remoteAddress || (req.headers['x-forwarded-for'] ? req.headers['x-forwarded-for'].toString().split(',')[0].trim() : null);
        console.log(`[Receiver] PID: ${process.pid} --- Android client ${ws.clientId} (IP: ${clientIp || 'unknown'}) connected.`);

        ws.isAlive = true;
        ws.on('pong', () => {
            ws.isAlive = true;
        });

        ws.heartbeatInterval = setInterval(() => {
            if (!ws.isAlive) {
                console.log(`[Receiver] PID: ${process.pid} --- Android client ${ws.clientId} (IP: ${clientIp || 'unknown'}) did not respond to ping. Terminating.`);
                clearInterval(ws.heartbeatInterval);
                return ws.terminate();
            }
            ws.isAlive = false;
            try {
                ws.ping(() => {});
            } catch (pingError) {
                console.error(`[Receiver] PID: ${process.pid} --- Error pinging Android client ${ws.clientId}: ${pingError.message}. Terminating.`);
                clearInterval(ws.heartbeatInterval);
                ws.terminate();
            }
        }, ANDROID_CLIENT_HEARTBEAT_INTERVAL_MS);

        ws.on('message', (message) => {
            // Android clients are not expected to send messages that require server processing in this setup
        });

        ws.on('close', (code, reason) => {
            clearInterval(ws.heartbeatInterval);
            const reasonStr = reason ? reason.toString() : 'N/A';
            console.log(`[Receiver] PID: ${process.pid} --- Android client ${ws.clientId} (IP: ${clientIp || 'unknown'}) disconnected. Code: ${code}, Reason: ${reasonStr}`);
        });

        ws.on('error', (err) => {
            clearInterval(ws.heartbeatInterval);
            console.error(`[Receiver] PID: ${process.pid} --- Android client ${ws.clientId} (IP: ${clientIp || 'unknown'}) error: ${err.message}`);
        });
    });

    wssAndroidClients.on('error', (err) => {
        console.error(`[Receiver] PID: ${process.pid} --- Public WebSocket Server Error: ${err.message}`, err.stack || '');
        initiateShutdown('PUBLIC_SERVER_ERROR', 1);
    });

} catch (e) {
    console.error(`[Receiver] PID: ${process.pid} --- FATAL ERROR starting Public WebSocket server: ${e.message}`, e.stack || '');
    initiateShutdown('PUBLIC_SERVER_INIT_FAILURE', 1);
}

// --- Internal WebSocket Server (for binance_listener.js) ---
try {
    wssListenerSource = new WebSocket.Server({ port: INTERNAL_LISTENER_PORT });
    console.log(`[Receiver] PID: ${process.pid} --- Internal WebSocket server (expecting minimal JSON strings) started on port ${INTERNAL_LISTENER_PORT}`);

    wssListenerSource.on('connection', (wsListener, req) => {
        const listenerIp = req.socket.remoteAddress;

        // The restrictive IP check that was here has been removed.
        // Security is now handled by the Google Cloud firewall rules.

        console.log(`[Receiver] PID: ${process.pid} --- binance_listener.js connected internally from ${listenerIp}`);

        wsListener.on('message', (message) => {
            try {
                const minimalJsonStringFromListener = message.toString();

                if (!wssAndroidClients || !wssAndroidClients.clients) {
                    console.error(`[Receiver] PID: ${process.pid} --- CRITICAL PRE-BROADCAST: wssAndroidClients or its clients set is null/undefined! Data not broadcasted.`);
                    return;
                }

                if (wssAndroidClients.clients.size > 0) {
                    wssAndroidClients.clients.forEach(androidClient => {
                        if (androidClient.readyState === WebSocket.OPEN) {
                            try {
                                androidClient.send(minimalJsonStringFromListener);
                            } catch (sendError) {
                                console.error(`[Receiver] PID: ${process.pid} --- Send Error to Android client ${androidClient.clientId || 'unknown'}: ${sendError.message}`);
                            }
                        }
                    });
                }
            } catch (e) {
                 console.error(`[Receiver] PID: ${process.pid} --- CRITICAL: ERROR in wsListener.on("message") processing: ${e.message}`, e.stack);
            }
        });

        wsListener.on('close', (code, reason) => {
            const reasonStr = reason ? reason.toString() : 'N/A';
            console.log(`[Receiver] PID: ${process.pid} --- binance_listener.js disconnected internally from ${listenerIp}. Code: ${code}, Reason: ${reasonStr}`);
        });

        wsListener.on('error', (err) => {
            console.error(`[Receiver] PID: ${process.pid} --- Error with internal binance_listener.js connection from ${listenerIp}: ${err.message}`);
        });
    });

    wssListenerSource.on('error', (err) => {
        console.error(`[Receiver] PID: ${process.pid} --- Internal WebSocket Server Error: ${err.message}`, err.stack || '');
        initiateShutdown('INTERNAL_SERVER_ERROR', 1);
    });

} catch (e) {
    console.error(`[Receiver] PID: ${process.pid} --- FATAL ERROR starting Internal WebSocket server: ${e.message}`, e.stack || '');
    initiateShutdown('INTERNAL_SERVER_INIT_FAILURE', 1);
}

// --- Graceful Shutdown Signals ---
process.on('SIGINT', () => initiateShutdown('SIGINT', 0));
process.on('SIGTERM', () => initiateShutdown('SIGTERM', 0));

if (!shuttingDown) {
    console.log(`[Receiver] PID: ${process.pid} --- data_receiver_server.js script initialized. (Minimal JSON Mode, Heartbeats @ ${ANDROID_CLIENT_HEARTBEAT_INTERVAL_MS/1000}s)`);
}
