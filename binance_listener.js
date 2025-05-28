// binance_listener.js

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
        if (binanceBookTickerWsClient && typeof binanceBookTickerWsClient.terminate === 'function') {
            try { binanceBookTickerWsClient.terminate(); } catch (e) { /* ignore */ }
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
        if (binanceBookTickerWsClient && typeof binanceBookTickerWsClient.terminate === 'function') {
            try { binanceBookTickerWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        process.exit(1);
    }, 1000).unref();
});

// --- Configuration ---
const binanceBookTickerStreamUrl = 'wss://stream.binance.com:9443/ws/btcusdt@bookTicker'; // Spot BookTicker stream
const internalReceiverUrl = 'ws://localhost:8082'; // URL of your data_receiver_server.js
const RECONNECT_INTERVAL_MS = 5000;
const BINANCE_SPOT_PING_INTERVAL_MS = 3 * 60 * 1000; // 3 minutes for Binance official spot stream (used for BookTicker)
const BOOK_TICKER_BID_PRICE_CHANGE_THRESHOLD = 1.2; // Example: Send if price changes by at least 1.2 USDT

// --- State Variables ---
let binanceBookTickerWsClient = null;
let internalWsClient = null;
let binanceBookTickerPingIntervalId = null;
let lastSentBookTickerBidPrice = null; // Stores the numeric value of the last BookTicker best bid price sent

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
        lastSentBookTickerBidPrice = null; // Reset on new connection to ensure fresh data
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

// --- Binance BookTicker Stream Connection (Spot) ---
function connectToBinanceBookTicker() {
    if (binanceBookTickerWsClient && (binanceBookTickerWsClient.readyState === WebSocket.OPEN || binanceBookTickerWsClient.readyState === WebSocket.CONNECTING)) {
        console.log('[Listener] Binance BookTicker connection attempt skipped: already open or connecting.');
        return;
    }
    console.log(`[Listener] PID: ${process.pid} --- Connecting to Binance (BookTicker Spot): ${binanceBookTickerStreamUrl}`);
    binanceBookTickerWsClient = new WebSocket(binanceBookTickerStreamUrl);

    binanceBookTickerWsClient.on('open', function open() {
        console.log(`[Listener] PID: ${process.pid} --- Connected to Binance stream (btcusdt@bookTicker Spot).`);
        lastSentBookTickerBidPrice = null; // Reset on new connection

        if (binanceBookTickerPingIntervalId) clearInterval(binanceBookTickerPingIntervalId);
        binanceBookTickerPingIntervalId = setInterval(() => {
            if (binanceBookTickerWsClient && binanceBookTickerWsClient.readyState === WebSocket.OPEN) {
                try {
                    binanceBookTickerWsClient.ping(() => {});
                    // console.log('[Listener] Ping sent to Binance (BookTicker Spot).') // Optional: for debugging
                } catch (pingError) {
                    console.error(`[Listener] PID: ${process.pid} --- Error sending ping to Binance (BookTicker Spot):`, pingError.message);
                }
            }
        }, BINANCE_SPOT_PING_INTERVAL_MS);
    });

    binanceBookTickerWsClient.on('message', function incoming(data) {
        try {
            const messageString = data.toString();
            let parsedMessage;
            try {
                parsedMessage = JSON.parse(messageString);
            } catch (parseError) {
                 // Binance spot stream can send text 'pong' in response to library's text 'ping'
                 if (messageString.includes("pong") || messageString.trim().toLowerCase() === 'pong') {
                    // console.log('[Listener] Text Pong received from Binance (BookTicker Spot).'); // Optional: for debugging
                    return; // Handled by 'pong' event usually, but good to be safe.
                }
                console.warn(`[Listener] PID: ${process.pid} --- Failed to parse JSON from Binance (BookTicker Spot). Snippet:`, messageString.substring(0,150));
                return;
            }

            // Expected BookTicker fields: "s" (symbol), "b" (best bid price)
            // Stream: btcusdt@bookTicker, so symbol "s" should be "BTCUSDT"
            // Message format: {"u":updateId,"s":"BTCUSDT","b":"bestBidPrice","B":"bestBidQty","a":"bestAskPrice","A":"bestAskQty"}
            if (parsedMessage && typeof parsedMessage.b === 'string' && parsedMessage.s === 'BTCUSDT') {
                const currentBestBidPriceString = parsedMessage.b;
                const currentPrice = parseFloat(currentBestBidPriceString);

                if (isNaN(currentPrice)) {
                    console.warn(`[Listener] PID: ${process.pid} --- Invalid price in BookTicker data (Spot):`, currentBestBidPriceString);
                    return;
                }

                let shouldSendData = false;
                if (lastSentBookTickerBidPrice === null) {
                    shouldSendData = true;
                } else {
                    const priceDifference = Math.abs(currentPrice - lastSentBookTickerBidPrice);
                    if (priceDifference >= BOOK_TICKER_BID_PRICE_CHANGE_THRESHOLD) {
                        shouldSendData = true;
                    }
                }

                if (shouldSendData) {
                    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                        const payload = {
                            p: currentBestBidPriceString // Best bid price
                        };
                        let payloadJsonString;
                        try {
                            payloadJsonString = JSON.stringify(payload);
                        } catch (stringifyError) {
                            console.error(`[Listener] PID: ${process.pid} --- CRITICAL: Error stringifying BookTicker data (Spot):`, stringifyError.message, stringifyError.stack);
                            return;
                        }
                        
                        try {
                            internalWsClient.send(payloadJsonString);
                            lastSentBookTickerBidPrice = currentPrice;
                            // console.log(`[Listener] Sent to internal: ${payloadJsonString}`); // Optional: for debugging
                        } catch (sendError) {
                            console.error(`[Listener] PID: ${process.pid} --- Error sending BookTicker data (Spot) to internal receiver:`, sendError.message, sendError.stack);
                        }
                    }
                }
            } else {
                // Handle other JSON messages (e.g., subscription confirmation like {"result":null,"id":1}, JSON pongs)
                if (parsedMessage && parsedMessage.result === null && parsedMessage.id !== undefined) {
                    console.log(`[Listener] PID: ${process.pid} --- Received subscription confirmation/response from Binance (BookTicker Spot):`, messageString.substring(0, 150));
                } else if (parsedMessage && parsedMessage.e === 'pong') { // Binance spot stream can also send JSON pongs
                    // console.log('[Listener] JSON Pong received from Binance (BookTicker Spot).'); // Optional: for debugging
                } else {
                    // Log if it's not a known non-data message
                    console.warn(`[Listener] PID: ${process.pid} --- Received unexpected JSON structure or non-BookTicker event from Binance (BookTicker Spot). Snippet:`, messageString.substring(0, 250));
                }
            }
        } catch (e) {
            console.error(`[Listener] PID: ${process.pid} --- CRITICAL ERROR in Binance (BookTicker Spot) message handler:`, e.message, e.stack);
        }
    });

    binanceBookTickerWsClient.on('pong', () => {
        // This handles pong frames in response to our ping frames (sent by ws library).
        // console.log('[Listener] Pong frame received from Binance (BookTicker Spot).'); // Optional: for debugging
    });

    binanceBookTickerWsClient.on('error', function error(err) {
        console.error(`[Listener] PID: ${process.pid} --- Binance (BookTicker Spot) WebSocket error:`, err.message);
    });

    binanceBookTickerWsClient.on('close', function close(code, reason) {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] PID: ${process.pid} --- Binance (BookTicker Spot) WebSocket closed. Code: ${code}, Reason: ${reasonStr}. Reconnecting in ${RECONNECT_INTERVAL_MS / 1000}s...`);
        if (binanceBookTickerPingIntervalId) { clearInterval(binanceBookTickerPingIntervalId); binanceBookTickerPingIntervalId = null; }
        binanceBookTickerWsClient = null;
        setTimeout(connectToBinanceBookTicker, RECONNECT_INTERVAL_MS);
    });
}

// --- Initial Connections ---
// To run this script, you would typically call these functions.
// For example, at the end of the file or in a main startup function:

function startListeners() {
    console.log(`[Listener] PID: ${process.pid} --- Starting listeners...`);
    connectToInternalReceiver();
    connectToBinanceBookTicker();
}

startListeners(); // Call to start the process

// Keep the process alive (useful if running standalone)
// setInterval(() => {}, 1 << 30); // This is a common trick, but not strictly necessary if websockets keep it alive.
// A more explicit way for a long-running script without a specific end:
// console.log("[Listener] Script initialized and running. Press Ctrl+C to exit.");
