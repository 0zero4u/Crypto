const WebSocket = require('ws');
const msgpack = require('msgpack-lite');

// --- Global Error Handlers ---
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
    // Optionally exit for unhandled rejections if they are always critical
    // setTimeout(() => process.exit(1), 1000).unref();
});

// --- Configuration ---
const binanceStreamUrl = 'wss://stream.binance.com:9443/ws/btcusdt@depth5@100ms';
const internalReceiverUrl = 'ws://localhost:8082';
const RECONNECT_INTERVAL = 5000; // ms
const BINANCE_PING_INTERVAL_MS = 3 * 60 * 1000; // 3 minutes

// --- State Variables ---
let binanceWsClient;
let internalWsClient;
let binancePingIntervalId;

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) {
        return; // Already connected or connecting
    }
    console.log(`[Listener] Attempting to connect to internal data receiver: ${internalReceiverUrl}`);
    internalWsClient = new WebSocket(internalReceiverUrl);

    internalWsClient.on('open', () => {
        console.log('[Listener] SUCCESS: Connected to internal data receiver.');
    });

    internalWsClient.on('error', (err) => {
        // Log errors, but not necessarily every failed connection attempt during retries.
        // Consider adding a counter for retries if logging becomes too noisy.
        console.error('[Listener] Internal receiver WebSocket error:', err.message);
    });

    internalWsClient.on('close', (code, reason) => {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] Internal receiver WebSocket closed. Code: ${code}, Reason: ${reasonStr}. Reconnecting in ${RECONNECT_INTERVAL / 1000}s...`);
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL);
    });
}

/**
 * WARNING: This manual parser is highly optimized for a specific, stable JSON format
 * and assumes minimal whitespace. It is very fragile and will break if the
 * Binance stream format for 'btcusdt@depth5@100ms' changes even slightly.
 */
function manualParseBinanceDepthSnapshot(messageString) {
    try {
        let lastUpdateIdNum = null;
        let bestBidPriceStr = null;

        const lastUpdateIdKey = '"lastUpdateId":';
        let currentIndex = messageString.indexOf(lastUpdateIdKey);
        if (currentIndex === -1) return null;
        currentIndex += lastUpdateIdKey.length;
        let valueEndIndex = currentIndex;
        while (valueEndIndex < messageString.length && messageString[valueEndIndex] >= '0' && messageString[valueEndIndex] <= '9') {
            valueEndIndex++;
        }
        if (currentIndex === valueEndIndex) return null;
        lastUpdateIdNum = parseInt(messageString.substring(currentIndex, valueEndIndex), 10);
        if (isNaN(lastUpdateIdNum)) return null;

        const bidsKeyAndOpening = '"bids":[["';
        currentIndex = messageString.indexOf(bidsKeyAndOpening, valueEndIndex);
        if (currentIndex === -1) return null;
        currentIndex += bidsKeyAndOpening.length;
        valueEndIndex = messageString.indexOf('"', currentIndex);
        if (valueEndIndex === -1) return null;
        bestBidPriceStr = messageString.substring(currentIndex, valueEndIndex);
        if (bestBidPriceStr.length === 0 || isNaN(parseFloat(bestBidPriceStr))) return null;

        return {
            lastUpdateId: lastUpdateIdNum,
            bestBidPrice: bestBidPriceStr,
            timestamp: Date.now()
        };
    } catch (error) {
        // This could be frequent if messages vary, keep it commented unless debugging parsing issues.
        // console.error('[Listener] Exception in manualParseBinanceDepthSnapshot:', error.message);
        return null;
    }
}

// --- Binance Stream Connection ---
function connectToBinance() {
    if (binanceWsClient && (binanceWsClient.readyState === WebSocket.OPEN || binanceWsClient.readyState === WebSocket.CONNECTING)) {
        return; // Already connected or connecting
    }
    console.log(`[Listener] Attempting to connect to Binance stream: ${binanceStreamUrl}`);
    binanceWsClient = new WebSocket(binanceStreamUrl);

    binanceWsClient.on('open', function open() {
        console.log('[Listener] SUCCESS: Connected to Binance stream (btcusdt@depth5@100ms).');
        if (binancePingIntervalId) clearInterval(binancePingIntervalId);
        binancePingIntervalId = setInterval(() => {
            if (binanceWsClient && binanceWsClient.readyState === WebSocket.OPEN) {
                binanceWsClient.ping(() => {}); // No log for ping send
            }
        }, BINANCE_PING_INTERVAL_MS);
    });

    binanceWsClient.on('message', function incoming(data) {
        const messageString = data.toString();
        const extractedData = manualParseBinanceDepthSnapshot(messageString);

        if (extractedData) {
            if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                try {
                    const packedData = msgpack.encode(extractedData);
                    internalWsClient.send(packedData);
                    // High-frequency log: // console.log('[Listener] Sent data (MessagePack) to internal receiver.');
                } catch (encodeError) {
                     console.error('[Listener] CRITICAL: Error encoding data to MessagePack:', encodeError.message, encodeError.stack);
                     // Potentially add logic to handle repeated encoding errors, e.g., stop or alert.
                }
            } else {
                // This log can be very frequent if the internal receiver is down.
                // Consider rate-limiting this log or having a state to log only once per disconnection.
                // For now, kept for visibility during disconnections.
                console.warn('[Listener] Internal receiver not connected/open. Data from Binance NOT sent.');
            }
        } else {
            // Log only if it's not a known non-data message (like WebSocket protocol pings/pongs if they come as messages)
            // and the message isn't empty.
            if (messageString && !messageString.includes('"ping"') && !messageString.includes('"pong"')) {
                 // This log indicates a potential issue with the manual parser or an unexpected message format.
                 // Can be frequent if the stream changes. Keep an eye on it.
                 console.warn('[Listener] Failed to manually parse Binance message or data was unexpected. Snippet:', messageString.substring(0, 100));
            }
        }
    });

    binanceWsClient.on('pong', () => {
        // High-frequency log: // console.log('[Listener] Received PONG from Binance.');
    });

    binanceWsClient.on('error', function error(err) {
        console.error('[Listener] Binance WebSocket error:', err.message);
    });

    binanceWsClient.on('close', function close(code, reason) {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] Binance WebSocket closed. Code: ${code}, Reason: ${reasonStr}. Reconnecting in ${RECONNECT_INTERVAL / 1000}s...`);
        if (binancePingIntervalId) { clearInterval(binancePingIntervalId); binancePingIntervalId = null; }
        binanceWsClient = null;
        setTimeout(connectToBinance, RECONNECT_INTERVAL);
    });
}

// --- Start the connections ---
console.log(`[Listener] PID: ${process.pid} --- Binance listener script starting...`);
connectToBinance();
connectToInternalReceiver();
console.log(`[Listener] PID: ${process.pid} --- Initial connection attempts initiated.`);
