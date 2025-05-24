// File: data_receiver_server.js
const WebSocket = require('ws');

// Port for Android clients (and other external clients) to connect to
const PUBLIC_PORT = 8081; // Android app connects here: ws://YOUR_VPS_IP:8081
// Port for binance_listener_modified.js to send data to (internal only)
const INTERNAL_LISTENER_PORT = 8082; // binance_listener_modified.js connects here: ws://localhost:8082

// --- Global Error Handlers (Place at top) ---
process.on('unhandledRejection', (reason, promise) => {
  console.error(`[Receiver] PID: ${process.pid} --- Unhandled Rejection at:`, promise, 'reason:', reason instanceof Error ? reason.stack : reason);
  // Node.js recommendation is not to exit on unhandledRejection by default
});

process.on('uncaughtException', (error) => {
  console.error(`[Receiver] PID: ${process.pid} --- Uncaught Exception:`, error.stack || error);
  console.error(`[Receiver] PID: ${process.pid} --- Uncaught exception detected. Attempting graceful shutdown then forcing exit...`);
  // Try to close servers gracefully
  let closedCount = 0;
  const totalServers = 2;
  const tryExit = () => {
    closedCount++;
    if (closedCount === totalServers) {
      console.error(`[Receiver] PID: ${process.pid} --- Servers closed on uncaughtException. Exiting.`);
      process.exit(1);
    }
  };

  if (wssAndroidClients) {
    wssAndroidClients.close(() => {
      console.log(`[Receiver] PID: ${process.pid} --- Public server (on uncaughtException) closed.`);
      tryExit();
    });
  } else {
      tryExit(); // Decrement totalServers or handle if not initialized
  }

  if (wssListenerSource) {
    wssListenerSource.close(() => {
      console.log(`[Receiver] PID: ${process.pid} --- Internal server (on uncaughtException) closed.`);
      tryExit();
    });
  } else {
      tryExit();
  }

  // Fallback exit if servers don't close quickly
  setTimeout(() => {
    console.error(`[Receiver] PID: ${process.pid} --- Timeout waiting for servers to close on uncaughtException. Forcing exit.`);
    process.exit(1);
  }, 3000).unref(); // unref to not keep process alive if it would exit otherwise
});


// --- Server for Android Clients ---
const wssAndroidClients = new WebSocket.Server({ port: PUBLIC_PORT });
console.log(`[Receiver] PID: ${process.pid} --- Public WebSocket server for Android clients started on port ${PUBLIC_PORT}`);

let androidClientCounter = 0;

wssAndroidClients.on('connection', (ws, req) => {
    ws.clientId = `android-${androidClientCounter++}`;
    const clientIp = req.socket.remoteAddress || req.headers['x-forwarded-for'];
    console.log(`[Receiver] PID: ${process.pid} --- Android client ${ws.clientId} (IP: ${clientIp}) connected.`);

    ws.on('message', (message) => {
        // console.log(`[Receiver] PID: ${process.pid} --- Received message from Android client ${ws.clientId} (IP: ${clientIp}): ${message.toString().substring(0,100)}...`); // Infrequent, but generally not expected for this server
        // Handle messages from Android if needed
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


// --- Server for Internal Data from binance_listener.js ---
const wssListenerSource = new WebSocket.Server({ port: INTERNAL_LISTENER_PORT });
console.log(`[Receiver] PID: ${process.pid} --- Internal WebSocket server for binance_listener.js started on port ${INTERNAL_LISTENER_PORT}`);

wssListenerSource.on('connection', (wsListener, req) => {
    const listenerIp = req.socket.remoteAddress;
    // console.log(`[Receiver] PID: ${process.pid} --- Connection attempt to internal listener port from IP: ${listenerIp}.`); // Reduced verbosity

    if (listenerIp !== '127.0.0.1' && listenerIp !== '::1' && listenerIp !== '::ffff:127.0.0.1') {
        console.warn(`[Receiver] PID: ${process.pid} --- UNAUTHORIZED connection attempt to internal listener port from IP: ${listenerIp}. Closing connection.`);
        wsListener.close();
        return;
    }
    console.log(`[Receiver] PID: ${process.pid} --- binance_listener.js connected internally from ${listenerIp}`);

    wsListener.on('message', (message) => {
        // console.log(`[Receiver] PID: ${process.pid} --- STEP 1: Entered wsListener.on("message")`); // Removed
        try {
            // console.log(`[Receiver] PID: ${process.pid} --- STEP 2: Raw message type: ${typeof message}, isBuffer: ${Buffer.isBuffer(message)}`); // Removed
            // if (Buffer.isBuffer(message)) {
            //     console.log(`[Receiver] PID: ${process.pid} --- STEP 2.1: Buffer length: ${message.length}`); // Removed
            // }

            const messageString = message.toString();
            // console.log(`[Receiver] PID: ${process.pid} --- STEP 3: Message converted to string. Length: ${messageString.length}. Preview: ${messageString.substring(0, 30)}...`); // Removed

            // console.log(`[Receiver] PID: ${process.pid} --- STEP 4: Checking wssAndroidClients.clients...`); // Removed
            if (!wssAndroidClients || !wssAndroidClients.clients) {
                console.error(`[Receiver] PID: ${process.pid} --- FATAL PRE-BROADCAST: wssAndroidClients or wssAndroidClients.clients is null/undefined!`);
                process.exit(1);
                return;
            }
            // console.log(`[Receiver] PID: ${process.pid} --- STEP 5: wssAndroidClients.clients type: ${typeof wssAndroidClients.clients}, size: ${wssAndroidClients.clients.size}`); // Removed

            const numAndroidClients = wssAndroidClients.clients.size;
            if (numAndroidClients === 0) {
                // console.log(`[Receiver] PID: ${process.pid} --- No Android clients connected. Skipping broadcast.`); // Can be frequent if no clients. Keep commented unless debugging.
            } else {
                // console.log(`[Receiver] PID: ${process.pid} --- Broadcasting to ${numAndroidClients} Android client(s)...`); // Optional: log once per message broadcast
                let successfullySentCount = 0;
                wssAndroidClients.clients.forEach(androidClient => {
                    // console.log(`[Receiver] PID: ${process.pid} --- STEP 6.${clientIndex}.1: Processing Android client: ${androidClientIdentifier}`); // Removed
                    if (androidClient.readyState === WebSocket.OPEN) {
                        try {
                            // console.log(`[Receiver] PID: ${process.pid} --- STEP 6.${clientIndex}.2: Sending to ${androidClientIdentifier}`); // Removed
                            androidClient.send(messageString);
                            successfullySentCount++;
                            // console.log(`[Receiver] PID: ${process.pid} --- STEP 6.${clientIndex}.3: Send successful to ${androidClientIdentifier}`); // Removed
                        } catch (sendError) {
                            console.error(`[Receiver] PID: ${process.pid} --- Send Error to Android client ${androidClient.clientId || 'unknown'}: ${sendError.message}`, sendError.stack || '');
                        }
                    } else {
                        // console.log(`[Receiver] PID: ${process.pid} --- Android client ${androidClient.clientId || 'unknown'} not OPEN (state: ${androidClient.readyState}). Skipping.`); // Can be frequent.
                    }
                });
                // if (successfullySentCount > 0 && successfullySentCount < numAndroidClients) {
                //    console.log(`[Receiver] PID: ${process.pid} --- Broadcasted message to ${successfullySentCount}/${numAndroidClients} open Android clients.`);
                // } else if (successfullySentCount === 0 && numAndroidClients > 0) {
                //    console.warn(`[Receiver] PID: ${process.pid} --- No open Android clients to broadcast message to (out of ${numAndroidClients} total).`);
                // }
            }
            // console.log(`[Receiver] PID: ${process.pid} --- STEP 7: Exiting wsListener.on("message") normally.`); // Removed
        } catch (e) {
            console.error(`[Receiver] PID: ${process.pid} --- FATAL CATCH BLOCK in wsListener.on("message"): ${e.message}`, e.stack);
            process.exit(1);
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
const gracefulShutdown = (signal) => {
    console.log(`\n[Receiver] PID: ${process.pid} --- ${signal} received. Shutting down servers...`);
    let closedCount = 0;
    const totalServers = 2; // Adjust if you have more/less servers to close

    const onServerClose = (serverName) => {
        console.log(`[Receiver] PID: ${process.pid} --- ${serverName} closed.`);
        closedCount++;
        if (closedCount === totalServers) {
            console.log(`[Receiver] PID: ${process.pid} --- All servers closed. Exiting.`);
            setTimeout(() => process.exit(0), 200); // Allow time for logs to flush
        }
    };

    wssAndroidClients.close(() => onServerClose('Public server for Android clients'));
    wssListenerSource.close(() => onServerClose('Internal server for binance_listener'));

    // Force exit if graceful shutdown takes too long
    setTimeout(() => {
        console.error(`[Receiver] PID: ${process.pid} --- Graceful shutdown timeout. Forcing exit.`);
        process.exit(1);
    }, 5000).unref();
};

process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

console.log(`[Receiver] PID: ${process.pid} --- data_receiver_server.js script initialized. Waiting for connections.`);
