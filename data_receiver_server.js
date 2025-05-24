
const WebSocket = require('ws');

const PUBLIC_PORT = 8081;
const INTERNAL_LISTENER_PORT = 8082;

process.on('unhandledRejection', (reason, promise) => {
  console.error(`[Receiver] PID: ${process.pid} --- Unhandled Rejection at:`, promise, 'reason:', reason instanceof Error ? reason.stack : reason);
});

process.on('uncaughtException', (error) => {
  console.error(`[Receiver] PID: ${process.pid} --- Uncaught Exception:`, error.stack || error);
  console.error(`[Receiver] PID: ${process.pid} --- Uncaught exception detected. Attempting graceful shutdown then forcing exit...`);

  let serversClosed = 0;
  const totalServersToClose = 2;

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
      // This log is kept as it's part of a critical error handling path
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
    // console.log(`[Receiver] PID: ${process.pid} --- Public WebSocket server for Android clients started on port ${PUBLIC_PORT}`);

    let androidClientCounter = 0;

    wssAndroidClients.on('connection', (ws, req) => {
        ws.clientId = `android-${androidClientCounter++}`;
        const clientIp = req.socket.remoteAddress || req.headers['x-forwarded-for'];
        // console.log(`[Receiver] PID: ${process.pid} --- Android client ${ws.clientId} (IP: ${clientIp}) connected.`);

        ws.on('message', (message) => {
            // Message logging from Android clients typically verbose, keep commented or remove if not needed for debugging.
            // console.log(`[Receiver] PID: ${process.pid} --- Received message from Android client ${ws.clientId} (IP: ${clientIp}): ${message.toString().substring(0,100)}...`);
        });

        ws.on('close', (code, reason) => {
            const reasonStr = reason ? reason.toString() : 'N/A';
            // console.log(`[Receiver] PID: ${process.pid} --- Android client ${ws.clientId} (IP: ${clientIp}) disconnected. Code: ${code}, Reason: ${reasonStr}`);
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


let wssListenerSource;
try {
    wssListenerSource = new WebSocket.Server({ port: INTERNAL_LISTENER_PORT });
    // console.log(`[Receiver] PID: ${process.pid} --- Internal WebSocket server for binance_listener.js started on port ${INTERNAL_LISTENER_PORT}`);

    wssListenerSource.on('connection', (wsListener, req) => {
        const listenerIp = req.socket.remoteAddress;

        if (listenerIp !== '127.0.0.1' && listenerIp !== '::1' && listenerIp !== '::ffff:127.0.0.1') {
            console.warn(`[Receiver] PID: ${process.pid} --- UNAUTHORIZED connection attempt to internal listener port from IP: ${listenerIp}. Closing connection.`);
            wsListener.close();
            return;
        }
        // console.log(`[Receiver] PID: ${process.pid} --- binance_listener.js connected internally from ${listenerIp}`);

        wsListener.on('message', (message) => {
            try {
                const messageString = message.toString();

                if (!wssAndroidClients || !wssAndroidClients.clients) {
                    console.error(`[Receiver] PID: ${process.pid} --- CRITICAL PRE-BROADCAST: wssAndroidClients or wssAndroidClients.clients is null/undefined! Data not broadcasted.`);
                    return;
                }

                const numAndroidClients = wssAndroidClients.clients.size;
                if (numAndroidClients > 0) {
                    wssAndroidClients.clients.forEach(androidClient => {
                        if (androidClient.readyState === WebSocket.OPEN) {
                            try {
                                androidClient.send(messageString);
                            } catch (sendError) {
                                console.error(`[Receiver] PID: ${process.pid} --- Send Error to Android client ${androidClient.clientId || 'unknown'}: ${sendError.message}`, sendError.stack || '');
                            }
                        } else {
                            // Client not open state is usually not an error, but a state. Logging can be very verbose.
                            // console.log(`[Receiver] PID: ${process.pid} --- Android client ${androidClient.clientId || 'unknown'} not OPEN (state: ${androidClient.readyState}). Skipping.`);
                        }
                    });
                } else {
                    // No clients connected is a normal state, not an error.
                    // console.log(`[Receiver] PID: ${process.pid} --- No Android clients connected. Skipping broadcast.`);
                }
            } catch (e) {
                console.error(`[Receiver] PID: ${process.pid} --- ERROR in wsListener.on("message") processing: ${e.message}`, e.stack);
            }
        });

        wsListener.on('close', (code, reason) => {
            const reasonStr = reason ? reason.toString() : 'N/A';
            // console.log(`[Receiver] PID: ${process.pid} --- binance_listener.js disconnected internally from ${listenerIp}. Code: ${code}, Reason: ${reasonStr}`);
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


const gracefulShutdown = (signal) => {
    console.log(`\n[Receiver] PID: ${process.pid} --- ${signal} received. Shutting down servers...`);
    let serversClosed = 0;
    const totalServersToClose = 2;

    const onServerClose = (serverName) => {
        console.log(`[Receiver] PID: ${process.pid} --- ${serverName} closed.`);
        serversClosed++;
        if (serversClosed >= totalServersToClose) {
            console.log(`[Receiver] PID: ${process.pid} --- All servers closed. Exiting.`);
            setTimeout(() => process.exit(0), 500); // Give a bit of time for logs to flush
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

    setTimeout(() => {
        console.error(`[Receiver] PID: ${process.pid} --- Graceful shutdown timeout. Forcing exit.`);
        process.exit(signal === 'SIGINT' ? 0 : 1); // Exit with 0 on SIGINT, 1 on SIGTERM if timeout
    }, 5000).unref();
};

process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

// console.log(`[Receiver] PID: ${process.pid} --- data_receiver_server.js script initialized. Waiting for connections.`);
