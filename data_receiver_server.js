
// File: data_receiver_server.js
const WebSocket = require('ws');

// Port for Android clients (and other external clients) to connect to
const PUBLIC_PORT = 8081; // Android app connects here: ws://YOUR_VPS_IP:8081
// Port for binance_listener.js to send data to (internal only)
const INTERNAL_LISTENER_PORT = 8082; // binance_listener.js connects here: ws://localhost:8082

// --- Global Error Handlers (Place at top) ---
process.on('unhandledRejection', (reason, promise) => {
  console.error(`[Receiver] PID: ${process.pid} --- Unhandled Rejection at:`, promise, 'reason:', reason instanceof Error ? reason.stack : reason);
  // Node.js recommendation is not to exit on unhandledRejection by default
});

process.on('uncaughtException', (error) => {
  console.error(`[Receiver] PID: ${process.pid} --- Uncaught Exception:`, error.stack || error);
  console.error(`[Receiver] PID: ${process.pid} --- Uncaught exception detected. Attempting graceful shutdown then forcing exit...`);

  let serversClosed = 0;
  const totalServersToClose = 2; // wssAndroidClients, wssListenerSource

  const attemptExit = () => {
    serversClosed++;
    if (serversClosed >= totalServersToClose) {
      console.error(`[Receiver] PID: ${process.pid} --- All servers attempted to close on uncaughtException. Forcing exit.`);
      process.exit(1);
    }
  };

  const closeServer = (server, serverName) => {
    if (server) {
      server.close(() => {
        console.log(`[Receiver] PID: ${process.pid} --- ${serverName} (on uncaughtException) closed.`);
        attemptExit();
      });
    } else {
      console.log(`[Receiver] PID: ${process.pid} --- ${serverName} (on uncaughtException) was not initialized or already null.`);
      attemptExit(); // Still count it as "handled" for exit logic
    }
  };

  closeServer(wssAndroidClients, 'Public server');
  closeServer(wssListenerSource, 'Internal server');

  // Fallback exit if servers don't close quickly
  setTimeout(() => {
    console.error(`[Receiver] PID: ${process.pid} --- Timeout waiting for servers to close on uncaughtException. Forcing exit.`);
    process.exit(1);
  }, 5000).unref();
});


// --- Server for Android Clients ---
let wssAndroidClients; // Declare here for access in uncaughtException
try {
    wssAndroidClients = new WebSocket.Server({ port: PUBLIC_PORT });
    console.log(`[Receiver] PID: ${process.pid} --- Public WebSocket server for Android clients started on port ${PUBLIC_PORT}`);

    let androidClientCounter = 0;

    wssAndroidClients.on('connection', (ws, req) => {
        ws.clientId = `android-${androidClientCounter++}`;
        const clientIp = req.socket.remoteAddress || req.headers['x-forwarded-for'];
        console.log(`[Receiver] PID: ${process.pid} --- Android client ${ws.clientId} (IP: ${clientIp}) connected.`);

        ws.on('message', (message) => {
            // console.log(`[Receiver] PID: ${process.pid} --- Received message from Android client ${ws.clientId} (IP: ${clientIp}): ${message.toString().substring(0,100)}...`);
            // Handle messages from Android if needed (e.g., subscription requests)
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

} catch (e) {
    console.error(`[Receiver] PID: ${process.pid} --- FATAL ERROR starting Public WebSocket server: ${e.message}`, e.stack || '');
    process.exit(1);
}


// --- Server for Internal Data from binance_listener.js ---
let wssListenerSource; // Declare here for access in uncaughtException
try {
    wssListenerSource = new WebSocket.Server({ port: INTERNAL_LISTENER_PORT });
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
            // All "STEP" logs are now commented out or removed
            // console.log(`[Receiver] PID: ${process.pid} --- STEP 1: Entered wsListener.on("message")`);
            try {
                // console.log(`[Receiver] PID: ${process.pid} --- STEP 2: Raw message type: ${typeof message}, isBuffer: ${Buffer.isBuffer(message)}`);
                // if (Buffer.isBuffer(message)) {
                //     console.log(`[Receiver] PID: ${process.pid} --- STEP 2.1: Buffer length: ${message.length}`);
                // }

                const messageString = message.toString();
                // console.log(`[Receiver] PID: ${process.pid} --- STEP 3: Message converted to string. Length: ${messageString.length}. Preview: ${messageString.substring(0, 30)}...`);

                // console.log(`[Receiver] PID: ${process.pid} --- STEP 4: Checking wssAndroidClients.clients...`);
                if (!wssAndroidClients || !wssAndroidClients.clients) {
                    console.error(`[Receiver] PID: ${process.pid} --- CRITICAL PRE-BROADCAST: wssAndroidClients or wssAndroidClients.clients is null/undefined! Data not broadcasted.`);
                    // No process.exit here; log the error and continue. The app might still be partially functional or recover.
                    return;
                }
                // console.log(`[Receiver] PID: ${process.pid} --- STEP 5: wssAndroidClients.clients type: ${typeof wssAndroidClients.clients}, size: ${wssAndroidClients.clients.size}`);

                const numAndroidClients = wssAndroidClients.clients.size;
                if (numAndroidClients === 0) {
                    // console.log(`[Receiver] PID: ${process.pid} --- STEP 6: No Android clients connected. Skipping broadcast.`); // This can be frequent
                } else {
                    // console.log(`[Receiver] PID: ${process.pid} --- STEP 6: Iterating ${numAndroidClients} Android clients for broadcast...`);
                    // let clientIndex = 0;
                    wssAndroidClients.clients.forEach(androidClient => {
                        // clientIndex++;
                        // let androidClientIdentifier = (androidClient.clientId || `unknown-android-${clientIndex}`);
                        // console.log(`[Receiver] PID: ${process.pid} --- STEP 6.${clientIndex}.1: Processing Android client: ${androidClientIdentifier}`);
                        if (androidClient.readyState === WebSocket.OPEN) {
                            try {
                                // console.log(`[Receiver] PID: ${process.pid} --- STEP 6.${clientIndex}.2: Sending to ${androidClientIdentifier}`);
                                androidClient.send(messageString);
                                // console.log(`[Receiver] PID: ${process.pid} --- STEP 6.${clientIndex}.3: Send successful to ${androidClientIdentifier}`);
                            } catch (sendError) {
                                console.error(`[Receiver] PID: ${process.pid} --- Send Error to Android client ${androidClient.clientId || 'unknown'}: ${sendError.message}`, sendError.stack || '');
                            }
                        } else {
                            // console.log(`[Receiver] PID: ${process.pid} --- STEP 6.${clientIndex}.S: Android client ${androidClientIdentifier} not OPEN (state: ${androidClient.readyState}). Skipping.`);
                        }
                    });
                }
                // console.log(`[Receiver] PID: ${process.pid} --- STEP 7: Exiting wsListener.on("message") normally.`);
            } catch (e) {
                console.error(`[Receiver] PID: ${process.pid} --- ERROR in wsListener.on("message") processing: ${e.message}`, e.stack);
                // Avoid process.exit(1) directly in message handler for robustness unless absolutely necessary.
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

} catch (e) {
    console.error(`[Receiver] PID: ${process.pid} --- FATAL ERROR starting Internal WebSocket server: ${e.message}`, e.stack || '');
    process.exit(1);
}


// Graceful shutdown
const gracefulShutdown = (signal) => {
    console.log(`\n[Receiver] PID: ${process.pid} --- ${signal} received. Shutting down servers...`);
    let serversClosed = 0;
    const totalServersToClose = 2;

    const onServerClose = (serverName) => {
        console.log(`[Receiver] PID: ${process.pid} --- ${serverName} closed.`);
        serversClosed++;
        if (serversClosed >= totalServersToClose) {
            console.log(`[Receiver] PID: ${process.pid} --- All servers closed. Exiting.`);
            setTimeout(() => process.exit(0), 500); // Allow time for logs to flush
        }
    };

    if (wssAndroidClients) {
        wssAndroidClients.close(() => onServerClose('Public server for Android clients'));
    } else {
        onServerClose('Public server for Android clients (was not initialized)');
    }

    if (wssListenerSource) {
        wssListenerSource.close(() => onServerClose('Internal server for binance_listener'));
    } else {
        onServerClose('Internal server for binance_listener (was not initialized)');
    }


    // Force exit if graceful shutdown takes too long
    setTimeout(() => {
        console.error(`[Receiver] PID: ${process.pid} --- Graceful shutdown timeout. Forcing exit.`);
        process.exit(signal === 'SIGINT' ? 0 : 1); // Exit 0 for SIGINT, 1 for SIGTERM if timeout
    }, 5000).unref();
};

process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

console.log(`[Receiver] PID: ${process.pid} --- data_receiver_server.js script initialized. Waiting for connections.`) 
