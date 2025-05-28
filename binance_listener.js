
const WebSocket = require('ws');

// --- Global Error Handlers ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`);
    console.error(err.stack || err);
    console.error(`[Listener] Exception origin: ${origin}`);
    console.error(`[Listener] PID: ${process.pid} --- Exiting due to uncaught exception...`);
    setTimeout(() => {
        if (internalWsClient && typeof internalWsClient.terminate === 'function') {
            try { internalWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (binanceAggTradeWsClient && typeof binanceAggTradeWsClient.terminate === 'function') {
            try { binanceAggTradeWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (binanceDepthWsClient && typeof binanceDepthWsClient.terminate === 'function') {
            try { binanceDepthWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        process.exit(1);
    }, 1000).unref();
});

process.on('unhandledRejection', (reason, promise) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNHANDLED PROMISE REJECTION`);
    console.error('[Listener] Unhandled Rejection at:', promise);
    console.error('[Listener] Reason:', reason instanceof Error ? reason.stack : reason);
    console.error(`[Listener] PID: ${process.pid} --- Exiting due to unhandled promise rejection...`);
    setTimeout(() => {
        if (internalWsClient && typeof internalWsClient.terminate === 'function') {
            try { internalWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (binanceAggTradeWsClient && typeof binanceAggTradeWsClient.terminate === 'function') {
            try { binanceAggTradeWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (binanceDepthWsClient && typeof binanceDepthWsClient.terminate === 'function') {
            try { binanceDepthWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        process.exit(1);
    }, 1000).unref();
});

// --- Configuration ---
const binanceAggTradeStreamUrl = 'wss://stream.binance.com:9443/ws/btcusdt@aggTrade';
const binanceDepthStreamUrl = 'wss://fstream.binance.com/ws/btcusdt@depth5@0ms'; // Futures stream
const internalReceiverUrl = 'ws://localhost:8082';
const RECONNECT_INTERVAL_MS = 5000;
const BINANCE_SPOT_PING_INTERVAL_MS = 3 * 60 * 1000; // 3 minutes for Binance official spot stream
const BINANCE_FUTURES_PING_INTERVAL_MS = 3 * 60 * 1000; // 3 minutes for Binance Futures stream
const AGG_TRADE_PRICE_CHANGE_THRESHOLD = 1.2;
const DEPTH_PRICE_CHANGE_THRESHOLD = 15.0;

// --- State Variables ---
let binanceAggTradeWsClient = null;
let binanceDepthWsClient = null;
let internalWsClient = null;

let binanceAggTradePingIntervalId = null;
let binanceDepthPingIntervalId = null;

let lastSentAggTradePrice = null; // Stores the numeric value of the last aggTrade price sent
let lastSentBestBidPrice = null;  // Stores the numeric value of the last best bid price sent

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) {
        console.log('[Listener] Internal receiver connection attempt skipped: already open or connecting.');
        return;
    }
    console.log(`[Listener] PID: ${process.pid} --- Connecting to internal receiver: ${internalReceiverUrl}`);
    internalWsClient = new WebSocket(internalReceiverUrl);

    internalWsClient.on('open', () => {
        console.log(`[Listener] PID: ${process.pid} --- Connected to internal receiver.`);
        lastSentAggTradePrice = null; // Reset on new connection to ensure fresh data
        lastSentBestBidPrice = null;  // Reset on new connection to ensure fresh data
    });

    internalWsClient.on('error', (err) => {
        console.error(`[Listener] PID: ${process.pid} --- Internal receiver WebSocket error:`, err.message);
    });

    internalWsClient.on('close', (code, reason) => {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] PID: ${process.pid} --- Internal receiver closed. Code: ${code}, Reason: ${reasonStr}. Reconnecting in ${RECONNECT_INTERVAL_MS / 1000}s...`);
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
}

/**
 * Manually extracts minimal data from an aggTrade message string and transforms it
 * into the format: { e: "trade", E: number, p: string } or null.
 */
function manualExtractMinimalData(messageString) {
    try {
        const outputEventTypeStr = "trade";
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
        // console.error('[Listener] Error in manualExtractMinimalData:', error.message); // Keep silent by default
        return null;
    }
}

// --- Binance AggTrade Stream Connection ---
function connectToBinanceAggTrade() {
    if (binanceAggTradeWsClient && (binanceAggTradeWsClient.readyState === WebSocket.OPEN || binanceAggTradeWsClient.readyState === WebSocket.CONNECTING)) {
        console.log('[Listener] Binance AggTrade connection attempt skipped: already open or connecting.');
        return;
    }
    console.log(`[Listener] PID: ${process.pid} --- Connecting to Binance (AggTrade): ${binanceAggTradeStreamUrl}`);
    binanceAggTradeWsClient = new WebSocket(binanceAggTradeStreamUrl);

    binanceAggTradeWsClient.on('open', function open() {
        console.log(`[Listener] PID: ${process.pid} --- Connected to Binance stream (btcusdt@aggTrade).`);
        lastSentAggTradePrice = null; // Reset on new connection

        if (binanceAggTradePingIntervalId) clearInterval(binanceAggTradePingIntervalId);
        binanceAggTradePingIntervalId = setInterval(() => {
            if (binanceAggTradeWsClient && binanceAggTradeWsClient.readyState === WebSocket.OPEN) {
                try {
                    binanceAggTradeWsClient.ping(() => {});
                } catch (pingError) {
                    console.error(`[Listener] PID: ${process.pid} --- Error sending ping to Binance (AggTrade):`, pingError.message);
                }
            }
        }, BINANCE_SPOT_PING_INTERVAL_MS);
    });

    binanceAggTradeWsClient.on('message', function incoming(data) {
        try {
            const messageString = data.toString();
            const minimalData = manualExtractMinimalData(messageString);

            if (minimalData) {
                const currentPrice = parseFloat(minimalData.p);
                if (isNaN(currentPrice)) {
                    console.warn(`[Listener] PID: ${process.pid} --- Invalid price in transformed aggTrade data:`, minimalData.p);
                    return;
                }

                let shouldSendData = false;
                if (lastSentAggTradePrice === null) {
                    shouldSendData = true;
                } else {
                    const priceDifference = Math.abs(currentPrice - lastSentAggTradePrice);
                    if (priceDifference >= AGG_TRADE_PRICE_CHANGE_THRESHOLD) {
                        shouldSendData = true;
                    }
                }

                if (shouldSendData) {
                    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                        let minimalJsonString;
                        try {
                            minimalJsonString = JSON.stringify(minimalData);
                        } catch (stringifyError) {
                            console.error(`[Listener] PID: ${process.pid} --- CRITICAL: Error stringifying aggTrade minimal data:`, stringifyError.message, stringifyError.stack);
                            return;
                        }
                        
                        try {
                            internalWsClient.send(minimalJsonString);
                            lastSentAggTradePrice = currentPrice;
                        } catch (sendError) {
                            console.error(`[Listener] PID: ${process.pid} --- Error sending aggTrade data to internal receiver:`, sendError.message, sendError.stack);
                        }
                    }
                }
            } else {
                if (messageString && !messageString.includes('"e":"pong"')) {
                    let isPotentiallyJson = false;
                    try { JSON.parse(messageString); isPotentiallyJson = true; } catch (e) { /* not json */ }

                    if (isPotentiallyJson && !messageString.includes('"e":"aggTrade"')) {
                         console.warn(`[Listener] PID: ${process.pid} --- Received non-aggTrade JSON or unexpected message from Binance (AggTrade). Snippet:`, messageString.substring(0, 150));
                    } else if (!isPotentiallyJson) {
                         console.warn(`[Listener] PID: ${process.pid} --- Received non-JSON message from Binance (AggTrade) (or parsing failed in check). Snippet:`, messageString.substring(0, 150));
                    }
                }
            }
        } catch (e) {
            console.error(`[Listener] PID: ${process.pid} --- CRITICAL ERROR in Binance (AggTrade) message handler:`, e.message, e.stack);
        }
    });

    binanceAggTradeWsClient.on('pong', () => { /* console.log('[Listener] Pong received from Binance (AggTrade).'); */ });

    binanceAggTradeWsClient.on('error', function error(err) {
        console.error(`[Listener] PID: ${process.pid} --- Binance (AggTrade) WebSocket error:`, err.message);
    });

    binanceAggTradeWsClient.on('close', function close(code, reason) {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] PID: ${process.pid} --- Binance (AggTrade) WebSocket closed. Code: ${code}, Reason: ${reasonStr}. Reconnecting in ${RECONNECT_INTERVAL_MS / 1000}s...`);
        if (binanceAggTradePingIntervalId) { clearInterval(binanceAggTradePingIntervalId); binanceAggTradePingIntervalId = null; }
        binanceAggTradeWsClient = null;
        setTimeout(connectToBinanceAggTrade, RECONNECT_INTERVAL_MS);
    });
}

// --- Binance Depth Stream Connection ---
function connectToBinanceDepth() {
    if (binanceDepthWsClient && (binanceDepthWsClient.readyState === WebSocket.OPEN || binanceDepthWsClient.readyState === WebSocket.CONNECTING)) {
        console.log('[Listener] Binance Depth connection attempt skipped: already open or connecting.');
        return;
    }
    console.log(`[Listener] PID: ${process.pid} --- Connecting to Binance (Depth@0ms Futures): ${binanceDepthStreamUrl}`);
    binanceDepthWsClient = new WebSocket(binanceDepthStreamUrl);

    binanceDepthWsClient.on('open', function open() {
        console.log(`[Listener] PID: ${process.pid} --- Connected to Binance stream (btcusdt@depth5@0ms Futures).`);
        lastSentBestBidPrice = null; // Reset on new connection

        if (binanceDepthPingIntervalId) clearInterval(binanceDepthPingIntervalId);
        binanceDepthPingIntervalId = setInterval(() => {
            if (binanceDepthWsClient && binanceDepthWsClient.readyState === WebSocket.OPEN) {
                try {
                    binanceDepthWsClient.ping(() => {});
                } catch (pingError) {
                    console.error(`[Listener] PID: ${process.pid} --- Error sending ping to Binance (Depth Futures):`, pingError.message);
                }
            }
        }, BINANCE_FUTURES_PING_INTERVAL_MS);
    });

    binanceDepthWsClient.on('message', function incoming(data) {
        try {
            const messageString = data.toString();
            let parsedMessage;
            try {
                parsedMessage = JSON.parse(messageString);
            } catch (parseError) {
                console.warn(`[Listener] PID: ${process.pid} --- Failed to parse JSON from Binance (Depth Futures). Snippet:`, messageString.substring(0,150));
                return;
            }

            // Expected structure: { "lastUpdateId": ..., "E": ..., "T": ..., "bids": [["price", "qty"], ...], "asks": [...] }
            if (parsedMessage && parsedMessage.bids && Array.isArray(parsedMessage.bids) && parsedMessage.bids.length > 0 &&
                Array.isArray(parsedMessage.bids[0]) && parsedMessage.bids[0].length > 0 && typeof parsedMessage.bids[0][0] === 'string' &&
                typeof parsedMessage.E === 'number') {

                const bestBidPriceString = parsedMessage.bids[0][0];
                const eventTime = parsedMessage.E;
                const currentBestBidPrice = parseFloat(bestBidPriceString);

                if (isNaN(currentBestBidPrice)) {
                    console.warn(`[Listener] PID: ${process.pid} --- Invalid best bid price in depth data:`, bestBidPriceString);
                    return;
                }

                let shouldSendData = false;
                if (lastSentBestBidPrice === null) {
                    shouldSendData = true;
                } else {
                    const priceDifference = Math.abs(currentBestBidPrice - lastSentBestBidPrice);
                    if (priceDifference >= DEPTH_PRICE_CHANGE_THRESHOLD) {
                        shouldSendData = true;
                    }
                }

                if (shouldSendData) {
                    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                        const payload = {
                            e: "depthBestBid", // Event type for client differentiation
                            E: eventTime,      // Event time from depth message
                            p: bestBidPriceString // Best bid price string
                        };
                        let payloadJsonString;
                        try {
                            payloadJsonString = JSON.stringify(payload);
                        } catch (stringifyError) {
                            console.error(`[Listener] PID: ${process.pid} --- CRITICAL: Error stringifying depth best bid data:`, stringifyError.message, stringifyError.stack);
                            return;
                        }

                        try {
                            internalWsClient.send(payloadJsonString);
                            lastSentBestBidPrice = currentBestBidPrice;
                        } catch (sendError) {
                            console.error(`[Listener] PID: ${process.pid} --- Error sending depth best bid data to internal receiver:`, sendError.message, sendError.stack);
                        }
                    }
                }
            } else {
                 // It's common for fstream to send pongs as JSON objects like {"e":"pong","T":1679498687278}
                 // Or just `pong` text message for PING requests from `ws` library (which sends text PING)
                if (parsedMessage && parsedMessage.e === 'pong') {
                    // This is a server-sent pong (often in response to a client ping if client sends JSON ping)
                    // Or sometimes sent proactively. If our library handles PING/PONG automatically (text based), this might be ignored.
                    // console.log('[Listener] JSON Pong received from Binance (Depth Futures).');
                } else if (messageString.trim().toLowerCase() === 'pong') {
                    // This is a text pong, likely in response to our ws library's text ping.
                    // console.log('[Listener] Text Pong received from Binance (Depth Futures).');
                }
                else {
                    console.warn(`[Listener] PID: ${process.pid} --- Received unexpected data structure from Binance (Depth Futures). Snippet:`, messageString.substring(0, 250));
                }
            }
        } catch (e) {
            console.error(`[Listener] PID: ${process.pid} --- CRITICAL ERROR in Binance (Depth Futures) message handler:`, e.message, e.stack);
        }
    });

    binanceDepthWsClient.on('pong', () => {
        // This handles pong frames in response to our ping frames.
        // console.log('[Listener] Pong frame received from Binance (Depth Futures).');
    });

    binanceDepthWsClient.on('error', function error(err) {
        console.error(`[Listener] PID: ${process.pid} --- Binance (Depth Futures) WebSocket error:`, err.message);
    });

    binanceDepthWsClient.on('close', function close(code, reason) {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] PID: ${process.pid} --- Binance (Depth Futures) WebSocket closed. Code: ${code}, Reason: ${reasonStr}. Reconnecting in ${RECONNECT_INTERVAL_MS / 1000}s...`);
        if (binanceDepthPingIntervalId) { clearInterval(binanceDepthPingIntervalId); binanceDepthPingIntervalId = null; }
        binanceDepthWsClient = null;
        setTimeout(connectToBinanceDepth, RECONNECT_INTERVAL_MS);
    });
}


// --- Start the connections ---
console.log(`[Listener] PID: ${process.pid} --- Binance listener starting...`);
console.log(`[Listener]   AggTrade Stream: btcusdt@aggTrade, Threshold: ${AGG_TRADE_PRICE_CHANGE_THRESHOLD}`);
console.log(`[Listener]   Depth Stream (Futures): btcusdt@depth5@0ms, Best Bid Threshold: ${DEPTH_PRICE_CHANGE_THRESHOLD}`);
connectToBinanceAggTrade();
connectToBinanceDepth();
connectToInternalReceiver();
console.log(`[Listener] PID: ${process.pid} --- Initial connection attempts initiated.`);
