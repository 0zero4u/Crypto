// File: binance_listener.js
const WebSocket = require('ws');

// --- Global Error Handlers ---
// These should be at the very top to catch any synchronous errors during script initialization
// or asynchronous errors not caught elsewhere.
process.on('uncaughtException', (err, origin) => {
    console.error(`[Listener] FATAL: UNCAUGHT EXCEPTION`);
    console.error(err.stack || err);
    console.error(`Exception origin: ${origin}`);
    console.error(`[Listener] Exiting due to uncaught exception...`);
    // Forcing an exit is often recommended for uncaught exceptions
    // as the application is in an undefined state.
    // Give a small delay for logs to flush if possible
    setTimeout(() => process.exit(1), 1000).unref(); // unref() so it doesn't keep node alive
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('[Listener] FATAL: UNHANDLED PROMISE REJECTION');
    console.error('Unhandled Rejection at:', promise);
    console.error('Reason:', reason.stack || reason);
    // Depending on the application, you might choose to exit or log and continue
    // console.error('[Listener] Exiting due to unhandled rejection...');
    // setTimeout(() => process.exit(1), 1000).unref();
});

// --- Configuration ---
const binanceStreamUrl = 'wss://stream.binance.com:9443/ws/btcusdt@depth5@100ms';
const internalReceiverUrl = 'ws://localhost:8082'; // Sends data TO data_receiver_server.js's internal port
const RECONNECT_INTERVAL = 5000; // 5 seconds
const BINANCE_PING_INTERVAL_MS = 3 * 60 * 1000; // 3 minutes

// --- State Variables ---
let binanceWsClient;
let internalWsClient;
let binancePingIntervalId;

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    console.log(`[Listener] Attempting to connect to internal data receiver: ${internalReceiverUrl}`);

    // Prevent multiple connection attempts if already connecting or connected
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) {
        console.log('[Listener] Already connected or attempting to connect to internal receiver.');
        return;
    }

    internalWsClient = new WebSocket(internalReceiverUrl);

    internalWsClient.on('open', () => {
        console.log('[Listener] SUCCESS: Connected to internal data receiver.');
    });

    internalWsClient.on('error', (err) => {
        console.error('[Listener] Internal receiver WebSocket error:', err.message);
        // The 'close' event will also be triggered, handling reconnection.
    });

    internalWsClient.on('close', (code, reason) => {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] Internal receiver WebSocket closed. Code: ${code}, Reason: ${reasonStr}`);
        internalWsClient = null; // Clear the client instance
        console.log(`[Listener] Attempting to reconnect to internal receiver in ${RECONNECT_INTERVAL / 1000} seconds...`);
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL);
    });
}

// --- Binance Stream Connection ---
function connectToBinance() {
    console.log(`[Listener] Attempting to connect to Binance stream: ${binanceStreamUrl}`);

    // Prevent multiple connection attempts if already connecting or connected
    if (binanceWsClient && (binanceWsClient.readyState === WebSocket.OPEN || binanceWsClient.readyState === WebSocket.CONNECTING)) {
        console.log('[Listener] Already connected or attempting to connect to Binance.');
        return;
    }

    binanceWsClient = new WebSocket(binanceStreamUrl);

    binanceWsClient.on('open', function open() {
        console.log('[Listener] SUCCESS: Connected to Binance stream (btcusdt@depth5@100ms).');
        // Clear any existing ping interval before starting a new one
        if (binancePingIntervalId) clearInterval(binancePingIntervalId);
        binancePingIntervalId = setInterval(() => {
            if (binanceWsClient && binanceWsClient.readyState === WebSocket.OPEN) {
                // console.log('[Listener] Sending PING to Binance'); // Can be verbose
                binanceWsClient.ping(() => {
                    // Optional: callback after ping is sent
                });
            }
        }, BINANCE_PING_INTERVAL_MS);
    });

    binanceWsClient.on('message', function incoming(data) {
        const messageString = data.toString();
        // console.log('[Listener] Received raw from Binance:', messageString.substring(0,100)); // Can be very verbose

        try {
            const depthData = JSON.parse(messageString);

            // Basic validation of expected structure
            if (depthData && depthData.bids && depthData.asks && depthData.lastUpdateId !== undefined) {
                const extractedData = {
                    lastUpdateId: depthData.lastUpdateId,
                    bids: depthData.bids,
                    asks: depthData.asks,
                    timestamp: Date.now() // Add a server-side timestamp
                };

                if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                    internalWsClient.send(JSON.stringify(extractedData));
                    // console.log('[Listener] Sent data to internal receiver.'); // Can be very verbose
                } else {
                    console.warn('[Listener] Internal receiver not connected or not open. Data from Binance not sent.');
                }
            } else {
                console.warn('[Listener] Received unexpected data structure from Binance:', messageString.substring(0,200));
            }
        } catch (error) {
            console.error('[Listener] Error parsing Binance message or processing data:', error.message);
            console.error('[Listener] Problematic Binance message snippet:', messageString.substring(0, 200));
        }
    });

    binanceWsClient.on('pong', () => {
        // console.log('[Listener] Received PONG from Binance. Connection healthy.'); // Can be verbose
    });

    binanceWsClient.on('error', function error(err) {
        console.error('[Listener] Binance WebSocket error:', err.message);
        console.error('[Listener] Binance WebSocket error object:', err); // Log the full error object
        // The 'close' event will also be triggered, handling reconnection.
    });

    binanceWsClient.on('close', function close(code, reason) {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] Binance WebSocket closed. Code: ${code}, Reason: ${reasonStr}`);
        if (binancePingIntervalId) {
            clearInterval(binancePingIntervalId);
            binancePingIntervalId = null;
        }
        binanceWsClient = null; // Clear the client instance
        console.log(`[Listener] Attempting to reconnect to Binance in ${RECONNECT_INTERVAL / 1000} seconds...`);
        setTimeout(connectToBinance, RECONNECT_INTERVAL);
    });
}

// --- Start the connections ---
console.log(`[Listener] PID: ${process.pid} --- Binance listener script starting...`);
connectToBinance();
connectToInternalReceiver();

console.log(`[Listener] PID: ${process.pid} --- Initial connection attempts initiated.`);
