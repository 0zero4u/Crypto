
// data_receiver_server.js (Modified for minimal JSON string from listener)

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
  const totalServersToClose = 2;

  const attemptExit = () => {
    serversClosed++;
    if (serversClosed >= totalServersToClose) {
      console.error(`[Receiver] PID: ${process.pid} --- All servers attempted to close. Forcing exit.`);
      process.exit(1);
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
      if (server.clients && typeof server.clients.forEach === 'function') {
        server.clients.forEach(client => {
          if (client.readyState === WebSocket.OPEN) client.terminate();
        });
      }
    } else {
      console.log(`[Receiver] PID: ${process.pid} --- ${serverName} (on uncaughtException) was not initialized or already null.`);
      attemptExit();
    }
  };

  closeServer(wssAndroidClients, 'Public server');
  closeServer(wssListenerSource, 'Internal server');

  setTimeout(() => {
    console.error(`[Receiver] PID: ${process.pid} --- Timeout waiting for servers to close on uncaughtException. Forcing exit.`);
    process.exit(1);
  }, 5000).unref();
});


let wssAndroidClients;
try {
    wssAndroidClients = new WebSocket.Server({ port: PUBLIC_PORT });
    console.log(`[Receiver] PID: ${process.pid} --- Public WebSocket server for Android clients started on port ${PUBLIC_PORT}`);

    let androidClientCounter = 0;

    wssAndroidClients.on('connection', (ws, req) => {
        ws.clientId = `android-${androidClientCounter++}`;
        const clientIp = req.socket.remoteAddress || req.headers['x-forwarded-for'];
        console.log(`[Receiver] PID: ${process.pid} --- Android client ${ws.clientId} (IP: ${clientIp}) connected.`);

        ws.on('message', (message) => {
            // console.log(`[Receiver] PID: ${process.pid} --- Received message from Android ${ws.clientId}: ${message.toString().substring(0,100)}...`);
        });

        ws.on('close', (code, reason) => {
            const reasonStr = reason ? reason.toString() : 'N/A';
            console.log(`[Receiver] PID: ${process.pid} --- Android client ${ws.clientId} (IP: ${clientIp}) disconnected. Code: ${code}, Reason: ${reasonStr}`);
        });

        ws.on('error', (err) => {
            console.error(`[Receiver] PID: ${process.pid} --- Android client ${ws.clientId} (IP: ${clientIp}) error: ${err.message}`);
        });
    });

    wssAndroidClients.on('error', (err) => {
        console.error(`[Receiver] PID: ${process.pid} --- Public WebSocket Server Error: ${err.message}`, err.stack || '');
    });

} catch (e) {
    console.error(`[Receiver] PID: ${process.pid} --- FATAL ERROR starting Public WebSocket server: ${e.message}`, e.stack || '');
    process.exit(1);
}


let wssListenerSource;
try {
    wssListenerSource = new WebSocket.Server({ port: INTERNAL_LISTENER_PORT });
    console.log(`[Receiver] PID: ${process.pid} --- Internal WebSocket server (expecting minimal JSON strings) started on port ${INTERNAL_LISTENER_PORT}`);

    wssListenerSource.on('connection', (wsListener, req) => {
        const listenerIp = req.socket.remoteAddress;

        if (listenerIp !== '127.0.0.1' && listenerIp !== '::1' && listenerIp !== '::ffff:127.0.0.1') {
            console.warn(`[Receiver] PID: ${process.pid} --- UNAUTHORIZED connection attempt to internal listener port from IP: ${listenerIp}. Closing connection.`);
            wsListener.terminate();
            return;
        }
        console.log(`[Receiver] PID: ${process.pid} --- binance_listener.js connected internally from ${listenerIp}`);

        wsListener.on('message', (message) => { // message here is a Buffer (containing a minimal JSON string from listener)
            try {
                // --- MODIFICATION: Convert buffer to string (which is our minimal JSON string) ---
                const minimalJsonStringFromListener = message.toString();
                // --- END MODIFICATION ---

                if (!wssAndroidClients || !wssAndroidClients.clients) {
                    console.error(`[Receiver] PID: ${process.pid} --- CRITICAL PRE-BROADCAST: wssAndroidClients or clients set is null/undefined! Data not broadcasted.`);
                    return;
                }

                const numAndroidClients = wssAndroidClients.clients.size;
                if (numAndroidClients > 0) {
                    wssAndroidClients.clients.forEach(androidClient => {
                        if (androidClient.readyState === WebSocket.OPEN) {
                            try {
                                // --- MODIFICATION: Send the already prepared minimal JSON string ---
                                androidClient.send(minimalJsonStringFromListener);
                                // --- END MODIFICATION ---
                                // High-frequency log: // console.log(`[Receiver] PID: ${process.pid} --- Sent minimal JSON data to Android client ${androidClient.clientId}`);
                            } catch (sendError) {
                                console.error(`[Receiver] PID: ${process.pid} --- Send Error to Android client ${androidClient.clientId || 'unknown'}: ${sendError.message}`);
                            }
                        }
                    });
                }
            } catch (e) {
                 // toString() on a buffer should not typically throw errors unless it's an encoding issue,
                 // but keeping a generic catch.
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
    });

} catch (e) {
    console.error(`[Receiver] PID: ${process.pid} --- FATAL ERROR starting Internal WebSocket server: ${e.message}`, e.stack || '');
    process.exit(1);
}


const gracefulShutdown = (signal) => {
    console.log(`\n[Receiver] PID: ${process.pid} --- ${signal} received. Shutting down servers...`);
    let serversClosed = 0;
    const totalServersToClose = 2;

    const onServerClose = (serverName) => {
        console.log(`[Receiver] PID: ${process.pid} --- ${serverName} closed.`);
        serversClosed++;
        if (serversClosed >= totalServersToClose) {
            console.log(`[Receiver] PID: ${process.pid} --- All servers closed. Exiting.`);
            setTimeout(() => process.exit(signal === 'SIGINT' ? 0 : 1), 500);
        }
    };
    
    const closeAndTerminate = (server, serverName) => {
        if (server) {
            console.log(`[Receiver] PID: ${process.pid} --- Closing ${serverName}...`);
            if (server.clients && typeof server.clients.forEach === 'function') {
                server.clients.forEach(client => {
                    if (client.readyState === WebSocket.OPEN) client.terminate();
                });
            }
            server.close((err) => {
                if (err) console.error(`[Receiver] PID: ${process.pid} --- Error closing ${serverName}: ${err.message}`);
                onServerClose(serverName);
            });
        } else {
            onServerClose(`${serverName} (was not initialized)`);
        }
    };

    closeAndTerminate(wssAndroidClients, 'Public server for Android clients');
    closeAndTerminate(wssListenerSource, 'Internal server for binance_listener');

    setTimeout(() => {
        console.error(`[Receiver] PID: ${process.pid} --- Graceful shutdown timeout. Forcing exit.`);
        process.exit(1);
    }, 5000).unref();
};

process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

console.log(`[Receiver] PID: ${process.pid} --- data_receiver_server.js script initialized. (Minimal JSON Mode)`);
