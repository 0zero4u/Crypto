// data_receiver_server.js (Effectively unchanged, handles new fields by passthrough)

const WebSocket = require('ws');
// const msgpack = require('msgpack-lite'); // No longer needed by this script

const PUBLIC_PORT = 8081;
const INTERNAL_LISTENER_PORT = 8082;

process.on('unhandledRejection', (reason, promise) => {
  console.error(`[Receiver] PID: ${process.pid} --- Unhandled Rejection at:`, promise, 'reason:', reason instanceof Error ? reason.stack : reason);
});

process.on('uncaughtException', (error) => {
  console.error(`[Receiver] PID: ${process.pid} --- FATAL: Uncaught Exception:`, error.stack || error);
  console.error(`[Receiver] PID: ${process.pid} --- Attempting graceful shutdown then forcing exit...`);

  let serversClosed = 0;
  const totalServersToClose = 2; // Assuming both servers might be up

  const attemptExit = () => {
    serversClosed++;
    if (serversClosed >= totalServersToClose) {
      console.error(`[Receiver] PID: ${process.pid} --- All servers attempted to close. Forcing exit.`);
      process.exit(1); // Force exit after attempting to close servers
    }
  };

  const closeServer = (server, serverName) => {
    if (server) {
      console.log(`[Receiver] PID: ${process.pid} --- Attempting to close ${serverName} (on uncaughtException)...`);
      server.close((err) => {
        if (err) {
          console.error(`[Receiver] PID: ${process.pid} --- Error closing ${serverName} (on uncaughtException):`, err.message);
        } else {
          console.log(`[Receiver] PID: ${process.pid} --- ${serverName} (on uncaughtException) closed.`);
        }
        attemptExit();
      });
      // Terminate clients forcefully as server.close() might wait for them
      if (server.clients && typeof server.clients.forEach === 'function') {
        server.clients.forEach(client => {
          if (client.readyState === WebSocket.OPEN) client.terminate();
        });
      }
    } else {
      console.log(`[Receiver] PID: ${process.pid} --- ${serverName} (on uncaughtException) was not initialized or already null.`);
      attemptExit(); // Still count it as "handled" for exit logic
    }
  };

  closeServer(wssAndroidClients, 'Public server');
  closeServer(wssListenerSource, 'Internal server');

  // Force exit after a timeout if graceful shutdown hangs
  setTimeout(() => {
    console.error(`[Receiver] PID: ${process.pid} --- Timeout waiting for servers to close on uncaughtException. Forcing exit.`);
    process.exit(1);
  }, 5000).unref(); // .unref() allows the program to exit if this is the only timer
});


let wssAndroidClients = null; // Initialize to null
try {
    wssAndroidClients = new WebSocket.Server({ port: PUBLIC_PORT });
    console.log(`[Receiver] PID: ${process.pid} --- Public WebSocket server for Android clients started on port ${PUBLIC_PORT}`);

    let androidClientCounter = 0;

    wssAndroidClients.on('connection', (ws, req) => {
        ws.clientId = `android-${androidClientCounter++}`;
        const clientIp = req.socket.remoteAddress || (req.headers['x-forwarded-for'] ? req.headers['x-forwarded-for'].split(',').shift() : null);
        console.log(`[Receiver] PID: ${process.pid} --- Android client ${ws.clientId} (IP: ${clientIp || 'unknown'}) connected.`);

        ws.on('message', (message) => {
            // console.log(`[Receiver] PID: ${process.pid} --- Received message from Android ${ws.clientId}: ${message.toString().substring(0,100)}...`);
            // Android clients might send messages back, e.g., acknowledgements or requests.
            // For now, we just log them if needed.
        });

        ws.on('close', (code, reason) => {
            const reasonStr = reason ? reason.toString() : 'N/A';
            console.log(`[Receiver] PID: ${process.pid} --- Android client ${ws.clientId} (IP: ${clientIp || 'unknown'}) disconnected. Code: ${code}, Reason: ${reasonStr}`);
        });

        ws.on('error', (err) => {
            console.error(`[Receiver] PID: ${process.pid} --- Android client ${ws.clientId} (IP: ${clientIp || 'unknown'}) error: ${err.message}`);
        });
    });

    wssAndroidClients.on('error', (err) => {
        console.error(`[Receiver] PID: ${process.pid} --- Public WebSocket Server Error: ${err.message}`, err.stack || '');
        // Depending on the error, you might want to attempt a restart or exit
    });

} catch (e) {
    console.error(`[Receiver] PID: ${process.pid} --- FATAL ERROR starting Public WebSocket server: ${e.message}`, e.stack || '');
    process.exit(1); // Exit if public server can't start
}


let wssListenerSource = null; // Initialize to null
try {
    wssListenerSource = new WebSocket.Server({ port: INTERNAL_LISTENER_PORT });
    console.log(`[Receiver] PID: ${process.pid} --- Internal WebSocket server (expecting minimal JSON strings) started on port ${INTERNAL_LISTENER_PORT}`);

    wssListenerSource.on('connection', (wsListener, req) => {
        const listenerIp = req.socket.remoteAddress;

        // Simple IP check for the internal listener
        if (listenerIp !== '127.0.0.1' && listenerIp !== '::1' && listenerIp !== '::ffff:127.0.0.1') {
            console.warn(`[Receiver] PID: ${process.pid} --- UNAUTHORIZED connection attempt to internal listener port from IP: ${listenerIp}. Closing connection.`);
            wsListener.terminate();
            return;
        }
        console.log(`[Receiver] PID: ${process.pid} --- binance_listener.js connected internally from ${listenerIp}`);

        wsListener.on('message', (message) => { // message here is a Buffer (containing a minimal JSON string from listener)
            try {
                // --- Convert buffer to string (which is our minimal JSON string) ---
                const minimalJsonStringFromListener = message.toString();
                // --- This string is now passed directly to Android clients. ---
                // --- It might be '{"e":"trade", "p":"..."}' OR '{"e":"trade", "p":"...", "s":"BUY", "src":"F1"}' ---

                if (!wssAndroidClients || !wssAndroidClients.clients) {
                    // This check is important, especially if the public server failed to start or crashed.
                    console.error(`[Receiver] PID: ${process.pid} --- CRITICAL PRE-BROADCAST: wssAndroidClients or clients set is null/undefined! Data not broadcasted: ${minimalJsonStringFromListener.substring(0,100)}`);
                    return;
                }

                const numAndroidClients = wssAndroidClients.clients.size;
                if (numAndroidClients > 0) {
                    // console.log(`[Receiver] Broadcasting to ${numAndroidClients} Android client(s): ${minimalJsonStringFromListener.substring(0,100)}...`);
                    wssAndroidClients.clients.forEach(androidClient => {
                        if (androidClient.readyState === WebSocket.OPEN) {
                            try {
                                // --- Send the already prepared minimal JSON string ---
                                androidClient.send(minimalJsonStringFromListener);
                                // High-frequency log (usually commented out):
                                // console.log(`[Receiver] PID: ${process.pid} --- Sent to Android ${androidClient.clientId}: ${minimalJsonStringFromListener.substring(0,60)}`);
                            } catch (sendError) {
                                console.error(`[Receiver] PID: ${process.pid} --- Send Error to Android client ${androidClient.clientId || 'unknown'}: ${sendError.message}`);
                                // Consider removing client if send consistently fails, or client.terminate()
                            }
                        }
                    });
                }
            } catch (e) {
                 // toString() on a buffer should not typically throw errors unless it's an encoding issue,
                 // but keeping a generic catch for unexpected issues during message handling.
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
        // Depending on the error, you might want to attempt a restart or exit
    });

} catch (e) {
    console.error(`[Receiver] PID: ${process.pid} --- FATAL ERROR starting Internal WebSocket server: ${e.message}`, e.stack || '');
    process.exit(1); // Exit if internal server can't start
}


const gracefulShutdown = (signal) => {
    console.log(`\n[Receiver] PID: ${process.pid} --- ${signal} received. Shutting down servers...`);
    let serversClosed = 0;
    const totalServersToClose = 2; // We have two WebSocket servers

    const onServerClose = (serverName) => {
        console.log(`[Receiver] PID: ${process.pid} --- ${serverName} closed.`);
        serversClosed++;
        if (serversClosed >= totalServersToClose) {
            console.log(`[Receiver] PID: ${process.pid} --- All servers closed. Exiting.`);
            // Allow a very brief moment for final logs to flush before exiting
            setTimeout(() => process.exit(signal === 'SIGINT' ? 0 : 1), 100);
        }
    };
    
    const closeAndTerminate = (server, serverName) => {
        if (server) {
            console.log(`[Receiver] PID: ${process.pid} --- Closing ${serverName}...`);
            // First, terminate all clients to speed up server.close()
            if (server.clients && typeof server.clients.forEach === 'function') {
                server.clients.forEach(client => {
                    if (client.readyState === WebSocket.OPEN) {
                        // console.log(`[Receiver] Terminating client on ${serverName}`);
                        client.terminate(); // Force close client connections
                    }
                });
            }
            // Now close the server
            server.close((err) => {
                if (err) console.error(`[Receiver] PID: ${process.pid} --- Error closing ${serverName}: ${err.message}`);
                onServerClose(serverName);
            });
        } else {
            // If server was null (e.g., failed to start), still count it as "closed" for shutdown logic
            onServerClose(`${serverName} (was not initialized or already null)`);
        }
    };

    closeAndTerminate(wssAndroidClients, 'Public server for Android clients');
    closeAndTerminate(wssListenerSource, 'Internal server for binance_listener');

    // Fallback timeout for graceful shutdown
    setTimeout(() => {
        console.error(`[Receiver] PID: ${process.pid} --- Graceful shutdown timeout. Forcing exit.`);
        process.exit(1);
    }, 5000).unref(); // .unref() so this timer doesn't keep Node.js alive if everything else finishes
};

process.on('SIGINT', () => gracefulShutdown('SIGINT')); // Ctrl+C
process.on('SIGTERM', () => gracefulShutdown('SIGTERM')); // kill command

console.log(`[Receiver] PID: ${process.pid} --- data_receiver_server.js script initialized. (Minimal JSON Passthrough Mode)`);
