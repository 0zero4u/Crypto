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
    setTimeout(() => process.exit(1), 1000).unref();
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('[Listener] FATAL: UNHANDLED PROMISE REJECTION');
    console.error('Unhandled Rejection at:', promise);
    console.error('Reason:', reason.stack || reason);
    // setTimeout(() => process.exit(1), 1000).unref(); // Optionally exit
});

// --- Configuration ---
const binanceStreamUrl = 'wss://stream.binance.com:9443/ws/btcusdt@depth5@100ms';
const internalReceiverUrl = 'ws://localhost:8082';
const RECONNECT_INTERVAL = 5000;
const BINANCE_PING_INTERVAL_MS = 3 * 60 * 1000;

// --- State Variables ---
let binanceWsClient;
let internalWsClient;
let binancePingIntervalId;

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    // Prevent multiple connection attempts if already connecting or connected
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) {
        // console.log('[Listener] Already connected or attempting to connect to internal receiver.'); // Reduced verbosity
        return;
    }
    console.log(`[Listener] Attempting to connect to internal data receiver: ${internalReceiverUrl}`);

    internalWsClient = new WebSocket(internalReceiverUrl);

    internalWsClient.on('open', () => {
        console.log('[Listener] SUCCESS: Connected to internal data receiver.');
    });

    internalWsClient.on('error', (err) => {
        console.error('[Listener] Internal receiver WebSocket error:', err.message);
    });

    internalWsClient.on('close', (code, reason) => {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] Internal receiver WebSocket closed. Code: ${code}, Reason: ${reasonStr}`);
        internalWsClient = null;
        console.log(`[Listener] Attempting to reconnect to internal receiver in ${RECONNECT_INTERVAL / 1000} seconds...`);
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL);
    });
}

// --- Binance Stream Connection ---
function connectToBinance() {
    // Prevent multiple connection attempts if already connecting or connected
    if (binanceWsClient && (binanceWsClient.readyState === WebSocket.OPEN || binanceWsClient.readyState === WebSocket.CONNECTING)) {
        // console.log('[Listener] Already connected or attempting to connect to Binance.'); // Reduced verbosity
        return;
    }
    console.log(`[Listener] Attempting to connect to Binance stream: ${binanceStreamUrl}`);

    binanceWsClient = new WebSocket(binanceStreamUrl);

    binanceWsClient.on('open', function open() {
        console.log('[Listener] SUCCESS: Connected to Binance stream (btcusdt@depth5@100ms).');
        if (binancePingIntervalId) clearInterval(binancePingIntervalId);
        binancePingIntervalId = setInterval(() => {
            if (binanceWsClient && binanceWsClient.readyState === WebSocket.OPEN) {
                // console.log('[Listener] Sending PING to Binance'); // Verbose, keep commented
                binanceWsClient.ping(() => {});
            }
        }, BINANCE_PING_INTERVAL_MS);
    });

    binanceWsClient.on('message', function incoming(data) {
        const messageString = data.toString();
        // console.log('[Listener] Received raw from Binance:', messageString.substring(0,100)); // Very verbose

        try {
            const depthData = JSON.parse(messageString);

            if (depthData && depthData.bids && depthData.asks && depthData.lastUpdateId !== undefined) {
                const extractedData = {
                    lastUpdateId: depthData.lastUpdateId,
                    bids: depthData.bids,
                    asks: depthData.asks,
                    timestamp: Date.now()
                };

                if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                    internalWsClient.send(JSON.stringify(extractedData));
                    // console.log('[Listener] Sent data to internal receiver.'); // Very verbose
                } else {
                    console.warn('[Listener] Internal receiver not connected/open. Data from Binance NOT sent.');
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
        // console.log('[Listener] Received PONG from Binance. Connection healthy.'); // Verbose, keep commented
    });

    binanceWsClient.on('error', function error(err) {
        console.error('[Listener] Binance WebSocket error:', err.message);
        // console.error('[Listener] Binance WebSocket error object:', err); // Can be very verbose, enable for deep debugging
    });

    binanceWsClient.on('close', function close(code, reason) {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] Binance WebSocket closed. Code: ${code}, Reason: ${reasonStr}`);
        if (binancePingIntervalId) {
            clearInterval(binancePingIntervalId);
            binancePingIntervalId = null;
        }
        binanceWsClient = null;
        console.log(`[Listener] Attempting to reconnect to Binance in ${RECONNECT_INTERVAL / 1000} seconds...`);
        setTimeout(connectToBinance, RECONNECT_INTERVAL);
    });
}

// --- Start the connections ---
console.log(`[Listener] PID: ${process.pid} --- Binance listener script starting...`);
connectToBinance();
connectToInternalReceiver();

console.log(`[Listener] PID: ${process.pid} --- Initial connection attempts initiated.`);
