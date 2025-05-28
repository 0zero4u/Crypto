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
const BOOK_TICKER_PROCESSING_INTERVAL_MS = 40; // Process data every 40ms

// --- State Variables ---
let binanceBookTickerWsClient = null;
let internalWsClient = null;
let binanceBookTickerPingIntervalId = null;
let bookTickerProcessingIntervalId = null; // Interval ID for the 40ms processing

let lastSentBookTickerBidPrice = null;      // Stores the numeric value of the last BookTicker best bid price SENT to internal receiver
let latestBookTickerBidPriceString = null;  // Stores the latest best bid price string RECEIVED from Binance
let latestBookTickerBidPriceNumeric = null; // Stores the latest best bid price numeric value RECEIVED from Binance

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
        // Reset lastSentBookTickerBidPrice on new internal connection.
        // This ensures the current price (if available and meeting criteria) is sent to the newly connected receiver,
        // effectively resynchronizing it.
        lastSentBookTickerBidPrice = null;
    });

    internalWsClient.on('error', (err) => {
        console.error(`[Listener] PID: ${process.pid} --- Internal receiver WebSocket error:`, err.message);
    });

    internalWsClient.on('close', (code, reason) => {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] PID: ${process.pid} --- Internal receiver closed. Code: ${code}, Reason: ${reasonStr}. Reconnecting in ${RECONNECT_INTERVAL_MS / 1000}s...`);
        internalWsClient = null; // Important to set to null so a new object is created on reconnect attempt
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
}

// --- Binance BookTicker Data Processing (called by 40ms interval) ---
function processBookTickerData() {
    try {
        // Only process if we have actually received data from Binance since the last relevant reset
        if (latestBookTickerBidPriceNumeric === null || latestBookTickerBidPriceString === null) {
            // No data from Binance has been received yet (e.g., post-connection, before first message)
            return; 
        }

        const currentPriceToProcess = latestBookTickerBidPriceNumeric;
        const currentPriceStringToProcess = latestBookTickerBidPriceString;

        let shouldSendData = false;
        if (lastSentBookTickerBidPrice === null) {
            // This condition is met if:
            // 1. It's the first time processing after the listener started.
            // 2. The Binance WebSocket reconnected (lastSentBookTickerBidPrice reset in 'open').
            // 3. The Internal Receiver WebSocket reconnected (lastSentBookTickerBidPrice reset in 'open').
            // In these cases, we should send the current price to establish/re-establish a baseline.
            shouldSendData = true;
        } else {
            const priceDifference = Math.abs(currentPriceToProcess - lastSentBookTickerBidPrice);
            if (priceDifference >= BOOK_TICKER_BID_PRICE_CHANGE_THRESHOLD) {
                shouldSendData = true;
            }
        }

        if (shouldSendData) {
            if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                const payload = {
                    p: currentPriceStringToProcess // Send the original string representation for precision
                };
                let payloadJsonString;
                try {
                    payloadJsonString = JSON.stringify(payload);
                } catch (stringifyError) {
                    console.error(`[Listener] PID: ${process.pid} --- CRITICAL: Error stringifying BookTicker data for internal send:`, stringifyError.message, stringifyError.stack);
                    return; // Don't proceed if payload can't be created
                }
                
                try {
                    internalWsClient.send(payloadJsonString);
                    lastSentBookTickerBidPrice = currentPriceToProcess; // Update the last SENT price
                    // console.log(`[Listener] Sent to internal (interval): ${payloadJsonString}`); // Optional: for debugging
                } catch (sendError) {
                    console.error(`[Listener] PID: ${process.pid} --- Error sending BookTicker data to internal receiver:`, sendError.message, sendError.stack);
                    // If send fails, lastSentBookTickerBidPrice is NOT updated.
                    // This means in the next interval, we will re-evaluate sending this price data (or a newer one)
                    // against the same old lastSentBookTickerBidPrice.
                }
            }
            // If internalWsClient is not open, we don't send, and lastSentBookTickerBidPrice is not updated.
            // The next interval will re-evaluate sending this price (or a newer one).
        }
    } catch (e) {
        // Catch any unexpected errors within this interval function to prevent it from crashing the interval timer.
        console.error(`[Listener] PID: ${process.pid} --- CRITICAL ERROR in processBookTickerData interval:`, e.message, e.stack);
    }
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
        // Reset states related to received data and what was last sent.
        // Resetting lastSentBookTickerBidPrice ensures that after a Binance reconnect,
        // the first piece of new data processed by the interval will be sent to the internal receiver.
        lastSentBookTickerBidPrice = null;
        latestBookTickerBidPriceString = null;  // New data from Binance will populate this
        latestBookTickerBidPriceNumeric = null; // New data from Binance will populate this

        // Clear any existing ping interval and start a new one
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

        // Clear any existing processing interval and start a new one
        if (bookTickerProcessingIntervalId) clearInterval(bookTickerProcessingIntervalId);
        bookTickerProcessingIntervalId = setInterval(processBookTickerData, BOOK_TICKER_PROCESSING_INTERVAL_MS);
    });

    binanceBookTickerWsClient.on('message', function incoming(data) {
        try {
            const messageString = data.toString();
            let parsedMessage;
            try {
                parsedMessage = JSON.parse(messageString);
            } catch (parseError) {
                 if (messageString.includes("pong") || messageString.trim().toLowerCase() === 'pong') {
                    // console.log('[Listener] Text Pong received from Binance (BookTicker Spot).'); // Optional: for debugging
                    return; 
                }
                console.warn(`[Listener] PID: ${process.pid} --- Failed to parse JSON from Binance (BookTicker Spot). Snippet:`, messageString.substring(0,150));
                return;
            }

            // Expected BookTicker fields: "s" (symbol), "b" (best bid price)
            if (parsedMessage && typeof parsedMessage.b === 'string' && parsedMessage.s === 'BTCUSDT') {
                const currentBestBidPriceString = parsedMessage.b;
                const currentPrice = parseFloat(currentBestBidPriceString);

                if (isNaN(currentPrice)) {
                    console.warn(`[Listener] PID: ${process.pid} --- Invalid price in BookTicker data (Spot):`, currentBestBidPriceString);
                    return;
                }

                // Store the latest received price. The 40ms interval (processBookTickerData) will use this.
                latestBookTickerBidPriceString = currentBestBidPriceString;
                latestBookTickerBidPriceNumeric = currentPrice;
                // The decision to send is handled by processBookTickerData on its fixed interval.
                
            } else {
                if (parsedMessage && parsedMessage.result === null && parsedMessage.id !== undefined) {
                    console.log(`[Listener] PID: ${process.pid} --- Received subscription confirmation/response from Binance (BookTicker Spot):`, messageString.substring(0, 150));
                } else if (parsedMessage && parsedMessage.e === 'pong') { 
                    // console.log('[Listener] JSON Pong received from Binance (BookTicker Spot).'); // Optional: for debugging
                } else {
                    console.warn(`[Listener] PID: ${process.pid} --- Received unexpected JSON structure or non-BookTicker event from Binance (BookTicker Spot). Snippet:`, messageString.substring(0, 250));
                }
            }
        } catch (e) {
            console.error(`[Listener] PID: ${process.pid} --- CRITICAL ERROR in Binance (BookTicker Spot) message handler:`, e.message, e.stack);
        }
    });

    binanceBookTickerWsClient.on('pong', () => {
        // console.log('[Listener] Pong frame received from Binance (BookTicker Spot).'); // Optional: for debugging
    });

    binanceBookTickerWsClient.on('error', function error(err) {
        console.error(`[Listener] PID: ${process.pid} --- Binance (BookTicker Spot) WebSocket error:`, err.message);
        // Note: 'close' will usually follow 'error' and handle cleanup.
    });

    binanceBookTickerWsClient.on('close', function close(code, reason) {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] PID: ${process.pid} --- Binance (BookTicker Spot) WebSocket closed. Code: ${code}, Reason: ${reasonStr}. Reconnecting in ${RECONNECT_INTERVAL_MS / 1000}s...`);
        
        // Clear intervals associated with this specific WebSocket instance
        if (binanceBookTickerPingIntervalId) { clearInterval(binanceBookTickerPingIntervalId); binanceBookTickerPingIntervalId = null; }
        if (bookTickerProcessingIntervalId) { clearInterval(bookTickerProcessingIntervalId); bookTickerProcessingIntervalId = null; } // Stop processing data if Binance source is down
        
        binanceBookTickerWsClient = null; // Important to set to null
        // latestBookTickerBidPriceString/Numeric are not reset here; they hold the last known value.
        // They will be appropriately reset if/when the 'open' event fires on a successful reconnection.
        // lastSentBookTickerBidPrice is also not reset here; it reflects what was last sent.
        // It's reset by the 'open' events of either Binance or Internal WS.

        setTimeout(connectToBinanceBookTicker, RECONNECT_INTERVAL_MS);
    });
}

// --- Initial Connections ---
function startListeners() {
    console.log(`[Listener] PID: ${process.pid} --- Starting listeners...`);
    connectToInternalReceiver(); // Connect to internal system first (or concurrently)
    connectToBinanceBookTicker(); // Then connect to Binance
}

startListeners(); // Call to start the process

// console.log("[Listener] Script initialized and running. Press Ctrl+C to exit."); // Optional, for standalone execution
