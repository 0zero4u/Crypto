// File: data_receiver_server.js
const WebSocket = require('ws');

// Port for Android clients (and other external clients) to connect to
const PUBLIC_PORT = 8081; // Android app connects here: ws://YOUR_VPS_IP:8081
// Port for binance_listener_modified.js to send data to (internal only)
const INTERNAL_LISTENER_PORT = 8082; // binance_listener_modified.js connects here: ws://localhost:8082

// --- Server for Android Clients ---
const wssAndroidClients = new WebSocket.Server({ port: PUBLIC_PORT });
console.log(`[Receiver] Public WebSocket server for Android clients started on port ${PUBLIC_PORT}`);

wssAndroidClients.on('connection', (ws, req) => {
    const clientIp = req.socket.remoteAddress || req.headers['x-forwarded-for'];
    console.log(`[Receiver] Android client connected from IP: ${clientIp}`);

    ws.on('message', (message) => {
        console.log(`[Receiver] Received message from Android client ${clientIp}: ${message.toString()}`);
        // Handle messages from Android if needed (e.g., subscription requests for other symbols in future)
    });

    ws.on('close', (code, reason) => {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Receiver] Android client disconnected (${clientIp}). Code: ${code}, Reason: ${reasonStr}`);
    });

    ws.on('error', (err) => {
        console.error(`[Receiver] Android client error (${clientIp}):`, err.message);
    });
});

wssAndroidClients.on('error', (err) => {
    console.error('[Receiver] Public WebSocket Server Error:', err.message);
});


// --- Server for Internal Data from binance_listener_modified.js ---
const wssListenerSource = new WebSocket.Server({ port: INTERNAL_LISTENER_PORT });
console.log(`[Receiver] Internal WebSocket server for binance_listener.js started on port ${INTERNAL_LISTENER_PORT}`);

wssListenerSource.on('connection', (wsListener, req) => {
    const listenerIp = req.socket.remoteAddress;
    // Security: Only allow connections from localhost for this internal port
    if (listenerIp !== '127.0.0.1' && listenerIp !== '::1' && listenerIp !== '::ffff:127.0.0.1') {
        console.warn(`[Receiver] UNAUTHORIZED connection attempt to internal listener port from IP: ${listenerIp}. Closing.`);
        wsListener.close();
        return;
    }
    console.log(`[Receiver] binance_listener.js connected internally from ${listenerIp}`);

    wsListener.on('message', (message) => {
        const messageString = message.toString();
        // console.log('[Receiver] Received data from binance_listener.js internally.'); // For debugging

        // Broadcast this messageString to all connected Android clients
        wssAndroidClients.clients.forEach(androidClient => {
            if (androidClient.readyState === WebSocket.OPEN) {
                androidClient.send(messageString);
            }
        });
    });

    wsListener.on('close', (code, reason) => {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Receiver] binance_listener.js disconnected internally. Code: ${code}, Reason: ${reasonStr}`);
    });

    wsListener.on('error', (err) => {
        console.error('[Receiver] Error with internal binance_listener.js connection:', err.message);
    });
});

wssListenerSource.on('error', (err) => {
    console.error('[Receiver] Internal WebSocket Server Error:', err.message);
});


// Graceful shutdown
process.on('SIGINT', () => {
    console.log('[Receiver] Shutting down servers...');
    wssAndroidClients.close(() => {
        console.log('[Receiver] Public server closed.');
    });
    wssListenerSource.close(() => {
        console.log('[Receiver] Internal server closed.');
        process.exit(0);
    });
});
