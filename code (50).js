// --- START OF FILE data_receiver_server.js ---
// Final Version: Logging is reduced to critical events and errors only.

const WebSocket = require('ws');

const PUBLIC_PORT = 8081;
const INTERNAL_LISTENER_PORT = 8082;
const ANDROID_CLIENT_HEARTBEAT_INTERVAL_MS = 120000;
const SHUTDOWN_TIMEOUT_MS = 5000;

let wssAndroidClients = null;
let wssListenerSource = null;
let shuttingDown = false;
let forceExitTimer = null;

// --- Critical Error & Shutdown Handlers ---
process.on('unhandledRejection', (reason) => {
  console.error(`[Receiver] PID: ${process.pid} --- FATAL: Unhandled Rejection:`, reason instanceof Error ? reason.stack : reason);
  initiateShutdown('UNHANDLED_REJECTION', 1);
});
process.on('uncaughtException', (error) => {
  console.error(`[Receiver] PID: ${process.pid} --- FATAL: Uncaught Exception:`, error.stack || error);
  initiateShutdown('UNCAUGHT_EXCEPTION', 1);
});
process.on('SIGINT', () => initiateShutdown('SIGINT', 0));
process.on('SIGTERM', () => initiateShutdown('SIGTERM', 0));

function initiateShutdown(signal, exitCode = 1) {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`\n[Receiver] PID: ${process.pid} --- ${signal} signal received. Initiating shutdown...`);
    forceExitTimer = setTimeout(() => {
        console.error(`[Receiver] PID: ${process.pid} --- Shutdown timeout! Forcing exit.`);
        process.exit(exitCode);
    }, SHUTDOWN_TIMEOUT_MS).unref();

    let serversPending = [wssAndroidClients, wssListenerSource].filter(Boolean).length;
    if (serversPending === 0) {
        clearTimeout(forceExitTimer);
        setTimeout(() => process.exit(exitCode), 200);
        return;
    }
    const onServerClosed = () => {
        serversPending--;
        if (serversPending <= 0) {
            clearTimeout(forceExitTimer);
            console.log(`[Receiver] PID: ${process.pid} --- Shutdown complete. Exiting.`);
            setTimeout(() => process.exit(exitCode), 200);
        }
    };
    if (wssAndroidClients) attemptServerClosure(wssAndroidClients, 'Public Server', onServerClosed);
    if (wssListenerSource) attemptServerClosure(wssListenerSource, 'Internal Server', onServerClosed);
}

function attemptServerClosure(server, serverName, onDone) {
    server.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) client.terminate();
        if (client.heartbeatInterval) clearInterval(client.heartbeatInterval);
    });
    server.close(err => {
        if (err) console.error(`[Receiver] PID: ${process.pid} --- Error closing ${serverName}: ${err.message}`);
        onDone();
    });
}

// --- Public WebSocket Server (for Android Clients) ---
try {
    wssAndroidClients = new WebSocket.Server({ port: PUBLIC_PORT });
    wssAndroidClients.on('connection', (ws) => {
        ws.isAlive = true;
        ws.on('pong', () => { ws.isAlive = true; });
        ws.heartbeatInterval = setInterval(() => {
            if (!ws.isAlive) {
                clearInterval(ws.heartbeatInterval);
                return ws.terminate();
            }
            ws.isAlive = false;
            try { ws.ping(() => {}); } catch (pingError) {
                console.error(`[Receiver] PID: ${process.pid} --- Error pinging client: ${pingError.message}. Terminating.`);
                clearInterval(ws.heartbeatInterval);
                ws.terminate();
            }
        }, ANDROID_CLIENT_HEARTBEAT_INTERVAL_MS);
        ws.on('close', () => clearInterval(ws.heartbeatInterval));
        ws.on('error', (err) => {
            console.error(`[Receiver] PID: ${process.pid} --- Android client error: ${err.message}`);
            clearInterval(ws.heartbeatInterval);
        });
    });
    wssAndroidClients.on('error', (err) => {
        console.error(`[Receiver] PID: ${process.pid} --- Public Server Error: ${err.message}`);
        initiateShutdown('PUBLIC_SERVER_ERROR', 1);
    });
} catch (e) {
    console.error(`[Receiver] PID: ${process.pid} --- FATAL ERROR starting Public Server: ${e.message}`);
    initiateShutdown('PUBLIC_SERVER_INIT_FAILURE', 1);
}

// --- Internal WebSocket Server (for data listeners) ---
try {
    wssListenerSource = new WebSocket.Server({ port: INTERNAL_LISTENER_PORT });
    wssListenerSource.on('connection', (wsListener, req) => {
        const listenerIp = req.socket.remoteAddress;
        if (listenerIp !== '127.0.0.1' && listenerIp !== '::1' && listenerIp !== '::ffff:127.0.0.1') {
            console.warn(`[Receiver] PID: ${process.pid} --- UNAUTHORIZED connection to internal port from IP: ${listenerIp}. Closing.`);
            return wsListener.terminate();
        }
        console.log(`[Receiver] PID: ${process.pid} --- ✅ Internal data listener connected from ${listenerIp}`);

        wsListener.on('message', (message) => {
            if (wssAndroidClients && wssAndroidClients.clients.size > 0) {
                wssAndroidClients.clients.forEach(androidClient => {
                    if (androidClient.readyState === WebSocket.OPEN) {
                        androidClient.send(message, (err) => {
                            if (err) console.error(`[Receiver] PID: ${process.pid} --- Send Error to client: ${err.message}`);
                        });
                    }
                });
            }
        });
        wsListener.on('close', () => console.log(`[Receiver] PID: ${process.pid} --- ❌ Internal data listener from ${listenerIp} disconnected.`));
        wsListener.on('error', (err) => console.error(`[Receiver] PID: ${process.pid} --- Error with internal listener from ${listenerIp}: ${err.message}`));
    });
    wssListenerSource.on('error', (err) => {
        console.error(`[Receiver] PID: ${process.pid} --- Internal Server Error: ${err.message}`);
        initiateShutdown('INTERNAL_SERVER_ERROR', 1);
    });
} catch (e) {
    console.error(`[Receiver] PID: ${process.pid} --- FATAL ERROR starting Internal Server: ${e.message}`);
    initiateShutdown('INTERNAL_SERVER_INIT_FAILURE', 1);
}

// --- Final Startup Log ---
if (!shuttingDown) {
    console.log(`[Receiver] PID: ${process.pid} --- Data Receiver started. Public Port: ${PUBLIC_PORT}, Internal Port: ${INTERNAL_LISTENER_PORT}`);
}
// --- END OF FILE data_receiver_server.js ---