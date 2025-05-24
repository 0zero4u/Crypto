// File: data_receiver_server.js
const WebSocket = require('ws');

// Port for Android clients (and other external clients) to connect to
const PUBLIC_PORT = 8081; // Android app connects here: ws://YOUR_VPS_IP:8081
// Port for binance_listener_modified.js to send data to (internal only)
const INTERNAL_LISTENER_PORT = 8082; // binance_listener_modified.js connects here: ws://localhost:8082

// --- Server for Android Clients ---
const wssAndroidClients = new WebSocket.Server({ port: PUBLIC_PORT });
console.log(`[Receiver] Public WebSocket server for Android clients started on port ${PUBLIC_PORT}`);

let androidClientCounter = 0; // For unique client IDs in logs

wssAndroidClients.on('connection', (ws, req) => {
    ws.clientId = `android-${androidClientCounter++}`; // Assign a simple ID for logging
    const clientIp = req.socket.remoteAddress || req.headers['x-forwarded-for']; // Get client IP
    console.log(`[Receiver] Android client ${ws.clientId} (IP: ${clientIp}) connected.`);

    ws.on('message', (message) => {
        console.log(`[Receiver] Received message from Android client ${ws.clientId} (IP: ${clientIp}): ${message.toString()}`);
        // Handle messages from Android if needed (e.g., subscription requests for other symbols in future)
    });

    ws.on('close', (code, reason) => {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Receiver] Android client ${ws.clientId} (IP: ${clientIp}) disconnected. Code: ${code}, Reason: ${reasonStr}`);
    });

    ws.on('error', (err) => {
        console.error(`[Receiver] Android client ${ws.clientId} (IP: ${clientIp}) error: ${err.message}`, err.stack || '');
    });
});

wssAndroidClients.on('error', (err) => {
    console.error(`[Receiver] Public WebSocket Server Error: ${err.message}`, err.stack || '');
});


// --- Server for Internal Data from binance_listener_modified.js ---
const wssListenerSource = new WebSocket.Server({ port: INTERNAL_LISTENER_PORT });
console.log(`[Receiver] Internal WebSocket server for binance_listener.js started on port ${INTERNAL_LISTENER_PORT}`);

wssListenerSource.on('connection', (wsListener, req) => {
    const listenerIp = req.socket.remoteAddress;
    // Security: Only allow connections from localhost for this internal port
    if (listenerIp !== '127.0.0.1' && listenerIp !== '::1' && listenerIp !== '::ffff:127.0.0.1') {
        console.warn(`[Receiver] UNAUTHORIZED connection attempt to internal listener port from IP: ${listenerIp}. Closing connection.`);
        wsListener.close();
        return;
    }
    console.log(`[Receiver] binance_listener.js connected internally from ${listenerIp}`);

    wsListener.on('message', (message) => {
        const messageString = message.toString();
        // console.log('[Receiver] Received data from binance_listener.js internally, preparing to broadcast.'); // Uncomment for very verbose logging

        let broadcastCount = 0;
        let attemptedBroadcastCount = 0;

        wssAndroidClients.clients.forEach(androidClient => {
            attemptedBroadcastCount++;
            let androidClientIdentifier = (androidClient.clientId || "unknown Android client");
            if (androidClient._socket && androidClient._socket.remoteAddress) { // Add IP to identifier if available
                androidClientIdentifier += ` (IP: ${androidClient._socket.remoteAddress})`;
            }


            if (androidClient.readyState === WebSocket.OPEN) {
                try {
                    // console.log(`[Receiver] Attempting to send to Android client ${androidClientIdentifier}`); // Very verbose, enable if needed
                    androidClient.send(messageString);
                    broadcastCount++;
                } catch (sendError) {
                    console.error(`[Receiver] CRITICAL: Error sending data to ${androidClientIdentifier}. Error: ${sendError.message}`, sendError.stack || '');
                    // Optionally, log part of the message if you suspect it's problematic (be careful with log size)
                    // console.error(`[Receiver] Data that caused send error (first 200 chars): ${messageString.substring(0, 200)}`);
                }
            } else {
                // console.log(`[Receiver] Android client ${androidClientIdentifier} not open (state: ${androidClient.readyState}). Skipping send.`); // Verbose
            }
        });
        // console.log(`[Receiver] Broadcast attempted to ${attemptedBroadcastCount} clients, successfully sent to ${broadcastCount} Android clients.`); // Uncomment for detailed broadcast summary
    });

    wsListener.on('close', (code, reason) => {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Receiver] binance_listener.js disconnected internally from ${listenerIp}. Code: ${code}, Reason: ${reasonStr}`);
    });

    wsListener.on('error', (err) => {
        console.error(`[Receiver] Error with internal binance_listener.js connection from ${listenerIp}: ${err.message}`, err.stack || '');
    });
});

wssListenerSource.on('error', (err) => {
    console.error(`[Receiver] Internal WebSocket Server Error: ${err.message}`, err.stack || '');
});


// Graceful shutdown
process.on('SIGINT', () => {
    console.log('\n[Receiver] SIGINT received. Shutting down servers...');
    wssAndroidClients.close(() => {
        console.log('[Receiver] Public server for Android clients closed.');
    });
    wssListenerSource.close(() => {
        console.log('[Receiver] Internal server for binance_listener closed.');
        // Allow time for close messages to be logged before exiting
        setTimeout(() => process.exit(0), 500);
    });
});

process.on('SIGTERM', () => {
    console.log('\n[Receiver] SIGTERM received. Shutting down servers...');
    wssAndroidClients.close(() => {
        console.log('[Receiver] Public server for Android clients closed.');
    });
    wssListenerSource.close(() => {
        console.log('[Receiver] Internal server for binance_listener closed.');
        setTimeout(() => process.exit(0), 500);
    });
});

// Optional: Catch unhandled promise rejections and uncaught exceptions for more robust logging
process.on('unhandledRejection', (reason, promise) => {
  console.error('[Receiver] Unhandled Rejection at:', promise, 'reason:', reason instanceof Error ? reason.stack : reason);
  // Application specific logging, throwing an error, or other logic here
});

process.on('uncaughtException', (error) => {
  console.error('[Receiver] Uncaught Exception:', error.stack || error);
  // PM2 will likely restart the process anyway, but good to log it clearly.
  // Consider a more graceful shutdown here if possible, but often it's too late.
  process.exit(1); // Mandatory exit after uncaught exception
});
