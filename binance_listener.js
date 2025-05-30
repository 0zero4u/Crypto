// binance_listener.js (Modified for AggTrade Only)

const WebSocket = require('ws');

// --- Global Error Handlers ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[AggTrade-Listener] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`);
    console.error(err.stack || err);
    console.error(`[AggTrade-Listener] Exception origin: ${origin}`);
    console.error(`[AggTrade-Listener] PID: ${process.pid} --- Exiting due to uncaught exception...`);
    setTimeout(() => {
        if (pushIntervalId) { try { clearInterval(pushIntervalId); } catch(e) { /* ignore */ } }
        if (internalWsClient && typeof internalWsClient.terminate === 'function') {
            try { internalWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (binanceWsClient && typeof binanceWsClient.terminate === 'function') {
            try { binanceWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (binancePingIntervalId) { try { clearInterval(binancePingIntervalId); } catch(e) { /* ignore */ } }
        process.exit(1);
    }, 1000).unref();
});

process.on('unhandledRejection', (reason, promise) => {
    console.error(`[AggTrade-Listener] PID: ${process.pid} --- FATAL: UNHANDLED PROMISE REJECTION`);
    console.error('[AggTrade-Listener] Unhandled Rejection at:', promise);
    console.error('[AggTrade-Listener] Reason:', reason instanceof Error ? reason.stack : reason);
    console.error(`[AggTrade-Listener] PID: ${process.pid} --- Exiting due to unhandled promise rejection...`);
    setTimeout(() => {
        if (pushIntervalId) { try { clearInterval(pushIntervalId); } catch(e) { /* ignore */ } }
        if (internalWsClient && typeof internalWsClient.terminate === 'function') {
            try { internalWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (binanceWsClient && typeof binanceWsClient.terminate === 'function') {
            try { binanceWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (binancePingIntervalId) { try { clearInterval(binancePingIntervalId); } catch(e) { /* ignore */ } }
        process.exit(1);
    }, 1000).unref();
});

// --- AggTrade Listener Configuration ---
const binanceStreamUrl = 'wss://stream.binance.com:9443/ws/btcusdt@aggTrade';
const internalReceiverUrl = 'ws://localhost:8082'; // Your internal client address
const RECONNECT_INTERVAL_MS = 5000;
const BINANCE_PING_INTERVAL_MS = 3 * 60 * 1000;
const PRICE_CHANGE_THRESHOLD = 1.2; // For aggTrade listener push to internal client
const PUSH_INTERVAL_MS = 25.0; // Fixed push interval for aggTrade data

// --- AggTrade Listener State Variables ---
let binanceWsClient = null;
let internalWsClient = null;
let binancePingIntervalId = null;
let pushIntervalId = null;
let lastPriceMeetingThreshold = null;
let latestSignificantDataToPush = null;
let lastPushedDataByInterval = null;


// --- Internal Receiver Connection (for aggTrade) ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    console.log(`[AggTrade-Listener] PID: ${process.pid} --- Connecting to internal receiver at ${internalReceiverUrl}...`);
    internalWsClient = new WebSocket(internalReceiverUrl);

    internalWsClient.on('open', () => {
        console.log(`[AggTrade-Listener] PID: ${process.pid} --- Connected to internal receiver.`);
        lastPushedDataByInterval = null;
        latestSignificantDataToPush = null;
        startPushInterval();
    });

    internalWsClient.on('error', (err) => {
        console.error(`[AggTrade-Listener] PID: ${process.pid} --- Internal receiver WebSocket error:`, err.message);
    });

    internalWsClient.on('close', (code, reason) => {
        console.log(`[AggTrade-Listener] PID: ${process.pid} --- Disconnected from internal receiver. Code: ${code}, Reason: ${reason ? reason.toString() : 'N/A'}`);
        if (pushIntervalId) {
            clearInterval(pushIntervalId);
            pushIntervalId = null;
        }
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
}

/**
 * Manually extracts minimal data from an aggTrade message string.
 */
function manualExtractMinimalData(messageString) {
    try {
        const outputEventTypeStr = "trade"; // To match what was sent by the original script
        let eventTimeNum = null;
        let priceStr = null;

        const aggTradeEventTypeKey = '"e":"aggTrade"';
        let currentIndex = messageString.indexOf(aggTradeEventTypeKey);
        if (currentIndex === -1) return null;
        let searchStartIndex = currentIndex + aggTradeEventTypeKey.length;

        const eventTimeKey = '"E":';
        currentIndex = messageString.indexOf(eventTimeKey, searchStartIndex);
        if (currentIndex === -1) return null;
        currentIndex += eventTimeKey.length;
        let valueEndIndex = currentIndex;
        while (valueEndIndex < messageString.length && messageString[valueEndIndex] >= '0' && messageString[valueEndIndex] <= '9') {
            valueEndIndex++;
        }
        if (currentIndex === valueEndIndex) return null;
        eventTimeNum = parseInt(messageString.substring(currentIndex, valueEndIndex), 10);
        if (isNaN(eventTimeNum)) return null;

        const priceKey = '"p":"';
        let priceStartIndex = messageString.indexOf(priceKey, valueEndIndex);
        if (priceStartIndex === -1) return null;
        priceStartIndex += priceKey.length;
        let priceEndIndex = messageString.indexOf('"', priceStartIndex);
        if (priceEndIndex === -1) return null;
        priceStr = messageString.substring(priceStartIndex, priceEndIndex);
        if (priceStr.length === 0 || isNaN(parseFloat(priceStr))) return null;

        return { e: outputEventTypeStr, E: eventTimeNum, p: priceStr };
    } catch (error) {
        return null;
    }
}

// --- AggTrade Push Interval Logic ---
function startPushInterval() {
    if (pushIntervalId) {
        clearInterval(pushIntervalId);
        pushIntervalId = null;
    }

    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        pushIntervalId = setInterval(() => {
            if (!(internalWsClient && internalWsClient.readyState === WebSocket.OPEN)) {
                if (pushIntervalId) {
                    clearInterval(pushIntervalId);
                    pushIntervalId = null;
                }
                return;
            }
            if (!latestSignificantDataToPush) return;
            if (latestSignificantDataToPush === lastPushedDataByInterval) return;

            const dataToPush = latestSignificantDataToPush;
            let minimalJsonString;
            try {
                minimalJsonString = JSON.stringify(dataToPush);
            } catch (stringifyError) {
                console.error(`[AggTrade-Listener] PID: ${process.pid} --- CRITICAL: Error stringifying aggTrade data for interval push:`, stringifyError.message);
                return;
            }
            try {
                internalWsClient.send(minimalJsonString);
                lastPushedDataByInterval = dataToPush;
            } catch (sendError) {
                console.error(`[AggTrade-Listener] PID: ${process.pid} --- Error sending aggTrade data to internal receiver via interval:`, sendError.message);
            }
        }, PUSH_INTERVAL_MS);
    }
}

// --- Binance AggTrade Stream Connection ---
function connectToBinanceAggTrade() {
    if (binanceWsClient && (binanceWsClient.readyState === WebSocket.OPEN || binanceWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    console.log(`[AggTrade-Listener] PID: ${process.pid} --- Connecting to Binance AggTrade stream...`);
    binanceWsClient = new WebSocket(binanceStreamUrl);

    binanceWsClient.on('open', function open() {
        console.log(`[AggTrade-Listener] PID: ${process.pid} --- Connected to Binance AggTrade stream.`);
        lastPriceMeetingThreshold = null;
        latestSignificantDataToPush = null;

        if (binancePingIntervalId) clearInterval(binancePingIntervalId);
        binancePingIntervalId = setInterval(() => {
            if (binanceWsClient && binanceWsClient.readyState === WebSocket.OPEN) {
                try {
                    binanceWsClient.ping(() => {});
                } catch (pingError) {
                    console.error(`[AggTrade-Listener] PID: ${process.pid} --- Error sending ping to Binance AggTrade:`, pingError.message);
                }
            }
        }, BINANCE_PING_INTERVAL_MS);
    });

    binanceWsClient.on('message', function incoming(data) {
        try {
            const messageString = data.toString();
            const minimalData = manualExtractMinimalData(messageString);

            if (minimalData) {
                const currentPrice = parseFloat(minimalData.p);
                if (isNaN(currentPrice)) {
                    return;
                }
                let isSignificantChange = false;
                if (lastPriceMeetingThreshold === null) {
                    isSignificantChange = true;
                } else {
                    const priceDifference = Math.abs(currentPrice - lastPriceMeetingThreshold);
                    if (priceDifference >= PRICE_CHANGE_THRESHOLD) {
                        isSignificantChange = true;
                    }
                }
                if (isSignificantChange) {
                    latestSignificantDataToPush = minimalData;
                    lastPriceMeetingThreshold = currentPrice;
                }
            }
        } catch (e) {
            console.error(`[AggTrade-Listener] PID: ${process.pid} --- CRITICAL ERROR in Binance AggTrade message handler:`, e.message, e.stack);
        }
    });

    binanceWsClient.on('pong', () => { });

    binanceWsClient.on('error', function error(err) {
        console.error(`[AggTrade-Listener] PID: ${process.pid} --- Binance AggTrade WebSocket error:`, err.message);
    });

    binanceWsClient.on('close', function close(code, reason) {
        console.log(`[AggTrade-Listener] PID: ${process.pid} --- Binance AggTrade WebSocket closed. Code: ${code}, Reason: ${reason ? reason.toString() : 'N/A'}`);
        if (binancePingIntervalId) { clearInterval(binancePingIntervalId); binancePingIntervalId = null; }
        binanceWsClient = null;
        setTimeout(connectToBinanceAggTrade, RECONNECT_INTERVAL_MS);
    });
}

// --- Start the connections ---
console.log(`[AggTrade-Listener] PID: ${process.pid} --- Binance AggTrade Listener starting...`);

connectToInternalReceiver();
connectToBinanceAggTrade();
