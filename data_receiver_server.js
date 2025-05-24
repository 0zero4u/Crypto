// File: data_receiver_server.js
const WebSocket = require('ws');

// Port for Android clients (and other external clients) to connect to
const PUBLIC_PORT = 8081; // Android app connects here: ws://YOUR_VPS_IP:8081
// Port for binance_listener_modified.js to send data to (internal only)
const INTERNAL_LISTENER_PORT = 8082; // binance_listener_modified.js connects here: ws://localhost:8082

// --- Server for Android Clients ---
const wssAndroidClients = new WebSocket.Server({ port: PUBLIC_PORT });
console.log(`[Receiver] PID: ${process.pid} --- Public WebSocket server for Android clients started on port ${PUBLIC_PORT}`);

let androidClientCounter = 0; // For unique client IDs in logs

wssAndroidClients.on('connection', (ws, req) => {
    ws.clientId = `android-${androidClientCounter++}`; // Assign a simple ID for logging
    const clientIp = req.socket.remoteAddress || req.headers['x-forwarded-for']; // Get client IP
    console.log(`[Receiver] PID: ${process.pid} --- Android client ${ws.clientId} (IP: ${clientIp}) connected.`);

    ws.on('message', (message) => {
        console.log(`[Receiver] PID: ${process.pid} --- Received message from Android client ${ws.clientId} (IP: ${clientIp}): ${message.toString().substring(0,100)}...`); // Log preview
        // Handle messages from Android if needed (e.g., subscription requests for other symbols in future)
    });

    ws.on('close', (code, reason) => {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Receiver] PID: ${process.pid} --- Android client ${ws.clientId} (IP: ${clientIp}) disconnected. Code: ${code}, Reason: ${reasonStr}`);
    });

    ws.on('error', (err) => {
        console.error(`[Receiver] PID: ${process.pid} --- Android client ${ws.clientId} (IP: ${clientIp}) error: ${err.message}`, err.stack || '');
    });
});

wssAndroidClients.on('error', (err) => {
    console.error(`[Receiver] PID: ${process.pid} --- Public WebSocket Server Error: ${err.message}`, err.stack || '');
});


// --- Server for Internal Data from binance_listener_modified.js ---
const wssListenerSource = new WebSocket.Server({ port: INTERNAL_LISTENER_PORT });
console.log(`[Receiver] PID: ${process.pid} --- Internal WebSocket server for binance_listener.js started on port ${INTERNAL_LISTENER_PORT}`);

wssListenerSource.on('connection', (wsListener, req) => {
    const listenerIp = req.socket.remoteAddress;
    console.log(`[Receiver] PID: ${process.pid} --- Connection attempt to internal listener port from IP: ${listenerIp}.`);
    // Security: Only allow connections from localhost for this internal port
    if (listenerIp !== '127.0.0.1' && listenerIp !== '::1' && listenerIp !== '::ffff:127.0.0.1') {
        console.warn(`[Receiver] PID: ${process.pid} --- UNAUTHORIZED connection attempt to internal listener port from IP: ${listenerIp}. Closing connection.`);
        wsListener.close();
        return;
    }
    console.log(`[Receiver] PID: ${process.pid} --- binance_listener.js connected internally from ${listenerIp}`);

    wsListener.on('message', (message) => {
        console.log(`[Receiver] PID: ${process.pid} --- STEP 1: Entered wsListener.on("message")`);
        try {
            console.log(`[Receiver] PID: ${process.pid} --- STEP 2: Raw message type: ${typeof message}, isBuffer: ${Buffer.isBuffer(message)}`);
            if (Buffer.isBuffer(message)) {
                console.log(`[Receiver] PID: ${process.pid} --- STEP 2.1: Buffer length: ${message.length}`);
            }

            const messageString = message.toString();
            console.log(`[Receiver] PID: ${process.pid} --- STEP 3: Message converted to string. Length: ${messageString.length}. Preview: ${messageString.substring(0, 30)}...`);

            console.log(`[Receiver] PID: ${process.pid} --- STEP 4: Checking wssAndroidClients.clients...`);
            if (!wssAndroidClients || !wssAndroidClients.clients) {
                console.error(`[Receiver] PID: ${process.pid} --- FATAL PRE-BROADCAST: wssAndroidClients or wssAndroidClients.clients is null/undefined!`);
                process.exit(1); // Force exit to see if PM2 logs this as error
                return; // Should not be reached if exit works
            }
            console.log(`[Receiver] PID: ${process.pid} --- STEP 5: wssAndroidClients.clients type: ${typeof wssAndroidClients.clients}, size: ${wssAndroidClients.clients.size}`);

            if (wssAndroidClients.clients.size === 0) {
                console.log(`[Receiver] PID: ${process.pid} --- STEP 6: No Android clients connected. Skipping broadcast.`);
            } else {
                console.log(`[Receiver] PID: ${process.pid} --- STEP 6: Iterating ${wssAndroidClients.clients.size} Android clients for broadcast...`);
                let clientIndex = 0;
                wssAndroidClients.clients.forEach(androidClient => {
                    clientIndex++;
                    let androidClientIdentifier = (androidClient.clientId || `unknown-android-${clientIndex}`);
                    console.log(`[Receiver] PID: ${process.pid} --- STEP 6.${clientIndex}.1: Processing Android client: ${androidClientIdentifier}`);
                    if (androidClient.readyState === WebSocket.OPEN) {
                        try {
                            console.log(`[Receiver] PID: ${process.pid} --- STEP 6.${clientIndex}.2: Sending to ${androidClientIdentifier}`);
                            androidClient.send(messageString);
                            console.log(`[Receiver] PID: ${process.pid} --- STEP 6.${clientIndex}.3: Send successful to ${androidClientIdentifier}`);
                        } catch (sendError) {
                            console.error(`[Receiver] PID: ${process.pid} --- STEP 6.${clientIndex}.E: CRITICAL Send Error to ${androidClientIdentifier}: ${sendError.message}`, sendError.stack);
                        }
                    } else {
                        console.log(`[Receiver] PID: ${process.pid} --- STEP 6.${clientIndex}.S: Android client ${androidClientIdentifier} not OPEN (state: ${androidClient.readyState}). Skipping.`);
                    }
                });
            }
            console.log(`[Receiver] PID: ${process.pid} --- STEP 7: Exiting wsListener.on("message") normally.`);
        } catch (e) {
            console.error(`[Receiver] PID: ${process.pid} --- FATAL CATCH BLOCK in wsListener.on("message"): ${e.message}`, e.stack);
            process.exit(1); // Force exit to make PM2 log it as an error
        }
    });

    wsListener.on('close', (code, reason) => {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Receiver] PID: ${process.pid} --- binance_listener.js disconnected internally from ${listenerIp}. Code: ${code}, Reason: ${reasonStr}`);
    });

    wsListener.on('error', (err) => {
        console.error(`[Receiver] PID: ${process.pid} --- Error with internal binance_listener.js connection from ${listenerIp}: ${err.message}`, err.stack || '');
    });
});

wssListenerSource.on('error', (err) => {
    console.error(`[Receiver] PID: ${process.pid} --- Internal WebSocket Server Error: ${err.message}`, err.stack || '');
});


// Graceful shutdown
process.on('SIGINT', () => {
    console.log(`\n[Receiver] PID: ${process.pid} --- SIGINT received. Shutting down servers...`);
    wssAndroidClients.close(() => {
        console.log(`[Receiver] PID: ${process.pid} --- Public server for Android clients closed.`);
    });
    wssListenerSource.close(() => {
        console.log(`[Receiver] PID: ${process.pid} --- Internal server for binance_listener closed.`);
        setTimeout(() => process.exit(0), 500); // Allow time for logs to flush
    });
});

process.on('SIGTERM', () => {
    console.log(`\n[Receiver] PID: ${process.pid} --- SIGTERM received. Shutting down servers...`);
    wssAndroidClients.close(() => {
        console.log(`[Receiver] PID: ${process.pid} --- Public server for Android clients closed.`);
    });
    wssListenerSource.close(() => {
        console.log(`[Receiver] PID: ${process.pid} --- Internal server for binance_listener closed.`);
        setTimeout(() => process.exit(0), 500); // Allow time for logs to flush
    });
});

// Global Error Handlers
process.on('unhandledRejection', (reason, promise) => {
  console.error(`[Receiver] PID: ${process.pid} --- Unhandled Rejection at:`, promise, 'reason:', reason instanceof Error ? reason.stack : reason);
  // No process.exit(1) here as per Node.js recommendation for unhandledRejection,
  // but be aware the application might be in an inconsistent state.
});

process.on('uncaughtException', (error) => {
  console.error(`[Receiver] PID: ${process.pid} --- Uncaught Exception:`, error.stack || error);
  // For uncaughtException, it's critical to exit as the app is in an undefined state.
  // Try to close servers gracefully if possible, but it might be too late.
  console.error(`[Receiver] PID: ${process.pid} --- Uncaught exception detected. Forcing shutdown...`);
    wssAndroidClients.close(() => {
        console.log(`[Receiver] PID: ${process.pid} --- Public server (on uncaughtException) closed attempt.`);
    });
    wssListenerSource.close(() => {
        console.log(`[Receiver] PID: ${process.pid} --- Internal server (on uncaughtException) closed attempt.`);
        process.exit(1); // Exit with error code
    });
    // Fallback exit if servers don't close quickly
    setTimeout(() => {
        console.error(`[Receiver] PID: ${process.pid} --- Timeout waiting for servers to close on uncaughtException. Forcing exit.`);
        process.exit(1);
    }, 2000);
});

console.log(`[Receiver] PID: ${process.pid} --- data_receiver_server.js script initialized. Waiting for connections.`);
