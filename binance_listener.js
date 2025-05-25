//
const WebSocket = require('ws');

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
});

// --- Configuration ---
const binanceStreamUrl = 'wss://stream.binance.com:9443/ws/btcusdt@trade';
const internalReceiverUrl = 'ws://localhost:8082';
const RECONNECT_INTERVAL = 5000; // ms
const BINANCE_PING_INTERVAL_MS = 3 * 60 * 1000; // 3 minutes
const PRICE_CHANGE_THRESHOLD = 1.2; // Send data if price changes by this much or more

// --- State Variables ---
let binanceWsClient;
let internalWsClient;
let binancePingIntervalId;
let lastSentPrice = null; // Stores the numeric value of the last price sent

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    console.log(`[Listener] Connecting to internal receiver: ${internalReceiverUrl}`);
    internalWsClient = new WebSocket(internalReceiverUrl);

    internalWsClient.on('open', () => {
        console.log('[Listener] Connected to internal receiver.');
        lastSentPrice = null; // Reset on new connection to ensure receiver gets fresh data
    });

    internalWsClient.on('error', (err) => {
        console.error('[Listener] Internal receiver WebSocket error:', err.message);
    });

    internalWsClient.on('close', (code, reason) => {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] Internal receiver closed. Code: ${code}, Reason: ${reasonStr}. Reconnecting in ${RECONNECT_INTERVAL / 1000}s...`);
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL);
    });
}

/**
 * Returns an object like: { e: string, E: number, p: string } or null.
 */
function manualExtractMinimalData(messageString) {
    try {
        let eventTypeStr = null;
        let eventTimeNum = null;
        let priceStr = null;

        const eventTypeKey = '"e":"';
        let currentIndex = messageString.indexOf(eventTypeKey);
        if (currentIndex === -1) return null;
        currentIndex += eventTypeKey.length;
        let valueEndIndex = messageString.indexOf('"', currentIndex);
        if (valueEndIndex === -1) return null;
        eventTypeStr = messageString.substring(currentIndex, valueEndIndex);

        if (eventTypeStr !== 'trade') {
            return null;
        }

        const eventTimeKey = '"E":';
        currentIndex = messageString.indexOf(eventTimeKey, valueEndIndex);
        if (currentIndex === -1) return null;
        currentIndex += eventTimeKey.length;
        valueEndIndex = currentIndex;
        while (valueEndIndex < messageString.length && messageString[valueEndIndex] >= '0' && messageString[valueEndIndex] <= '9') {
            valueEndIndex++;
        }
        if (currentIndex === valueEndIndex) return null;
        eventTimeNum = parseInt(messageString.substring(currentIndex, valueEndIndex), 10);
        if (isNaN(eventTimeNum)) return null;

        const priceKey = '"p":"';
        currentIndex = messageString.indexOf(priceKey, valueEndIndex);
        if (currentIndex === -1) return null;
        currentIndex += priceKey.length;
        valueEndIndex = messageString.indexOf('"', currentIndex);
        if (valueEndIndex === -1) return null;
        priceStr = messageString.substring(currentIndex, valueEndIndex);
        if (priceStr.length === 0 || isNaN(parseFloat(priceStr))) return null;

        return {
            e: eventTypeStr,
            E: eventTimeNum,
            p: priceStr
        };
    } catch (error) {
        return null; // Keep this silent, main loop handles warnings for unparseable data
    }
}

// --- Binance Stream Connection ---
function connectToBinance() {
    if (binanceWsClient && (binanceWsClient.readyState === WebSocket.OPEN || binanceWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    console.log(`[Listener] Connecting to Binance: ${binanceStreamUrl}`);
    binanceWsClient = new WebSocket(binanceStreamUrl);

    binanceWsClient.on('open', function open() {
        console.log('[Listener] Connected to Binance stream (btcusdt@trade).');
        lastSentPrice = null; // Reset on new connection for fresh baseline

        if (binancePingIntervalId) clearInterval(binancePingIntervalId);
        binancePingIntervalId = setInterval(() => {
            if (binanceWsClient && binanceWsClient.readyState === WebSocket.OPEN) {
                binanceWsClient.ping(() => {}); // Send ping, no log needed on success
            }
        }, BINANCE_PING_INTERVAL_MS);
    });

    binanceWsClient.on('message', function incoming(data) {
        const messageString = data.toString();
        const minimalData = manualExtractMinimalData(messageString);

        if (minimalData) {
            const currentPrice = parseFloat(minimalData.p);
            if (isNaN(currentPrice)) {
                // This case should ideally be caught by manualExtractMinimalData's parseFloat check,
                // but an extra safety net if manualExtractMinimalData changes.
                console.warn('[Listener] Invalid price in trade data:', minimalData.p);
                return;
            }

            let shouldSendData = false;

            if (lastSentPrice === null) {
                shouldSendData = true;
            } else {
                const priceDifference = Math.abs(currentPrice - lastSentPrice);
                if (priceDifference >= PRICE_CHANGE_THRESHOLD) {
                    shouldSendData = true;
                }
            }

            if (shouldSendData) {
                if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                    try {
                        const minimalJsonString = JSON.stringify(minimalData);
                        internalWsClient.send(minimalJsonString);
                        lastSentPrice = currentPrice;
                    } catch (stringifyError) {
                         console.error('[Listener] CRITICAL: Error stringifying minimal data:', stringifyError.message, stringifyError.stack);
                    }
                } else {
                    // Log only if we intended to send but couldn't
                    console.warn('[Listener] Internal receiver not open. Qualified data NOT sent.');
                }
            }
        } else {
            // Log only if data is unparseable and not a known ping/pong
            if (messageString && !messageString.includes('"ping"') && !messageString.includes('"pong"')) {
                 console.warn('[Listener] Failed to extract data or unexpected format. Snippet:', messageString.substring(0, 100));
            }
        }
    });

    binanceWsClient.on('pong', () => {
        // Pong received, no log needed by default, confirms connection is alive
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
console.log(`[Listener] PID: ${process.pid} --- Binance listener starting (btcusdt@trade, Price Threshold: ${PRICE_CHANGE_THRESHOLD})`);
connectToBinance();
connectToInternalReceiver();
console.log(`[Listener] PID: ${process.pid} --- Initial connection attempts initiated.`);
