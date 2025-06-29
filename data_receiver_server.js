const WebSocket = require('ws');
const kcp = require('ikc-ts');
const dgram = require('dgram');

const PUBLIC_PORT = 8081;
const INTERNAL_LISTENER_PORT = 8082; // This is now a KCP/UDP port
const KCP_CONV_ID = 42; // Must match the listener's ID
const ANDROID_CLIENT_HEARTBEAT_INTERVAL_MS = 120000;
const SHUTDOWN_TIMEOUT_MS = 5000;

// --- Global State ---
let wssAndroidClients = null;
let udpServer = null; // Changed from wssListenerSource
let kcpUpdateInterval = null;
let shuttingDown = false;
let forceExitTimer = null;

// --- Unified Shutdown Logic ---
function initiateShutdown(signal, exitCode = 1) {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`\n[Receiver] PID: ${process.pid} --- ${signal} signal received. Initiating shutdown...`);

    if (forceExitTimer) clearTimeout(forceExitTimer);
    forceExitTimer = setTimeout(() => {
        console.error(`[Receiver] PID: ${process.pid} --- Shutdown timeout. Forcing exit.`);
        process.exit(exitCode);
    }, SHUTDOWN_TIMEOUT_MS).unref();
    
    if (kcpUpdateInterval) clearInterval(kcpUpdateInterval);

    // Close public WSS server
    if (wssAndroidClients) {
        console.log('[Receiver] Closing public WSS server and clients...');
        wssAndroidClients.clients.forEach(client => client.terminate());
        wssAndroidClients.close(() => console.log('[Receiver] Public WSS server closed.'));
    }

    // Close internal UDP server
    if (udpServer) {
        console.log('[Receiver] Closing internal UDP server...');
        udpServer.close(() => console.log('[Receiver] Internal UDP server closed.'));
    }
    
    setTimeout(() => {
        console.log(`[Receiver] PID: ${process.pid} --- Exiting gracefully.`);
        process.exit(exitCode);
    }, 2000).unref();
}


// --- Global Error Handlers ---
process.on('unhandledRejection', (reason, p) => {
  console.error('[Receiver] FATAL: Unhandled Rejection:', p, 'reason:', reason);
  initiateShutdown('UNHANDLED_REJECTION', 1);
});
process.on('uncaughtException', (err) => {
  console.error('[Receiver] FATAL: Uncaught Exception:', err);
  initiateShutdown('UNCAUGHT_EXCEPTION', 1);
});

// --- Public WebSocket Server (for Android Clients) ---
// THIS SECTION IS LARGELY UNCHANGED
try {
    wssAndroidClients = new WebSocket.Server({ port: PUBLIC_PORT });
    console.log(`[Receiver] Public WSS server for Android clients started on port ${PUBLIC_PORT}`);
    let androidClientCounter = 0;
    wssAndroidClients.on('connection', (ws, req) => {
        ws.clientId = `android-${androidClientCounter++}`;
        const clientIp = req.socket.remoteAddress || req.headers['x-forwarded-for']?.split(',')[0].trim();
        console.log(`[Receiver] Android client ${ws.clientId} (IP: ${clientIp || 'unknown'}) connected.`);
        ws.isAlive = true;
        ws.on('pong', () => { ws.isAlive = true; });
        ws.heartbeatInterval = setInterval(() => {
            if (!ws.isAlive) {
                console.log(`[Receiver] Android client ${ws.clientId} did not respond. Terminating.`);
                return ws.terminate();
            }
            ws.isAlive = false;
            ws.ping(() => {});
        }, ANDROID_CLIENT_HEARTBEAT_INTERVAL_MS);
        ws.on('close', () => clearInterval(ws.heartbeatInterval));
        ws.on('error', (err) => console.error(`[Receiver] Android client ${ws.clientId} error: ${err.message}`));
    });
} catch (e) {
    console.error(`[Receiver] FATAL ERROR starting Public WSS server: ${e.message}`);
    initiateShutdown('PUBLIC_SERVER_INIT_FAILURE', 1);
}


// --- Internal KCP/UDP Server (for binance_listener.js) ---
try {
    const kcpSessions = {}; 
    udpServer = dgram.createSocket('udp4');

    udpServer.on('error', (err) => {
        console.error(`[Receiver-KCP] UDP Server Error:\n${err.stack}`);
        udpServer.close();
        initiateShutdown('INTERNAL_SERVER_ERROR', 1);
    });

    udpServer.on('message', (msg, rinfo) => {
        const sessionKey = `${rinfo.address}:${rinfo.port}`;
        let kcpSession = kcpSessions[sessionKey];

        if (!kcpSession) {
            console.log(`[Receiver-KCP] New KCP session from ${sessionKey}`);
            const context = { remoteAddress: rinfo.address, remotePort: rinfo.port };
            kcpSession = new kcp.KCP(KCP_CONV_ID, context);
            kcpSession.nodelay(true, 10, 2, true); // Match sender's settings
            kcpSession.output((data, size, ctx) => {
                udpServer.send(data, 0, size, ctx.remotePort, ctx.remoteAddress);
            });
            kcpSessions[sessionKey] = kcpSession;
        }

        kcpSession.input(msg);
        kcpSession.update(Date.now());

        let received;
        while ((received = kcpSession.recv())) {
            const minimalJsonString = received.toString();
            if (wssAndroidClients?.clients.size > 0) {
                wssAndroidClients.clients.forEach(client => {
                    if (client.readyState === WebSocket.OPEN) {
                        client.send(minimalJsonString);
                    }
                });
            }
        }
    });

    udpServer.on('listening', () => {
        const address = udpServer.address();
        console.log(`[Receiver] Internal KCP/UDP server listening on ${address.address}:${address.port}`);
    });
    
    // Listen on all available network interfaces
    udpServer.bind(INTERNAL_LISTENER_PORT, '0.0.0.0');

} catch (e) {
    console.error(`[Receiver] FATAL ERROR starting Internal KCP/UDP server: ${e.message}`);
    initiateShutdown('INTERNAL_SERVER_INIT_FAILURE', 1);
}


// --- Graceful Shutdown Signals ---
process.on('SIGINT', () => initiateShutdown('SIGINT', 0));
process.on('SIGTERM', () => initiateShutdown('SIGTERM', 0));

console.log(`[Receiver] PID: ${process.pid} --- data_receiver_server.js script initialized.`);
