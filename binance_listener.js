
// binance_listener (60).js
const WebSocket = require('ws');

// --- Process-wide Error Handling ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`, err.stack || err);
    cleanupAndExit(1);
});
process.on('unhandledRejection', (reason, promise) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNHANDLED PROMISE REJECTION`, reason);
    cleanupAndExit(1);
});

/**
 * Gracefully terminates WebSocket clients and exits the process.
 * @param {number} [exitCode=1] - The exit code to use.
 */
function cleanupAndExit(exitCode = 1) {
    // MODIFIED: Removed the futuresBookTickerClient from cleanup
    const clientsToTerminate = [internalWsClient, spotBookTickerClient, futuresTradeClient];
    console.error('[Listener] Initiating cleanup...');
    clientsToTerminate.forEach(client => {
        if (client && (client.readyState === WebSocket.OPEN || client.readyState === WebSocket.CONNECTING)) {
            try {
                client.terminate();
            } catch (e) {
                console.error(`[Listener] Error during WebSocket termination: ${e.message}`);
            }
        }
    });
    // Allow time for cleanup before force-exiting
    setTimeout(() => {
        console.error(`[Listener] Exiting with code ${exitCode}.`);
        process.exit(exitCode);
    }, 1000).unref();
}

// --- Configuration ---
const SYMBOL = 'btcusdt'; // Binance uses lowercase for streams
const RECONNECT_INTERVAL_MS = 5000;

// --- Standard Behaviour Config ---
const MINIMUM_TICK_SIZE = 0.2;

// --- Imbalance Logic Config ---
const IMBALANCE_THRESHOLD_UPPER = 0.8; // 80% for buy-side imbalance
const IMBALANCE_THRESHOLD_LOWER = 0.2; // 20% for sell-side imbalance
const FAKE_PRICE_OFFSET = 1.0;

// --- URLS ---
const internalReceiverUrl = 'ws://instance-20250627-040948.asia-south2-a.c.ace-server-460719-b7.internal:8082/internal';
// MODIFIED: Removed FUTURES_BOOKTICKER_URL
const FUTURES_TRADE_URL = `wss://fstream.binance.com/ws/${SYMBOL}@trade`;         // For trade-triggered logic
const SPOT_BOOKTICKER_URL = `wss://stream.binance.com:9443/ws/${SYMBOL}@bookTicker`; // For imbalance AND standard tick logic

// --- WebSocket Clients and State ---
// MODIFIED: Removed futuresBookTickerClient
let internalWsClient, spotBookTickerClient, futuresTradeClient;
let last_sent_price = null;

// State for imbalance logic
let latestSpotData = { b: null, B: null, a: null, A: null }; // best bid P, Q, best ask P, Q

// Optimization: Reusable payload object to prevent GC pressure.
const payload_to_send = { type: 'S', p: 0.0 };

/**
 * Establishes and maintains the connection to the internal WebSocket receiver.
 */
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) return;

    internalWsClient = new WebSocket(internalReceiverUrl);

    internalWsClient.on('open', () => console.log('[Listener] Connected to internal receiver.'));
    internalWsClient.on('close', () => {
        console.log('[Listener] Disconnected from internal receiver. Reconnecting...');
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
    internalWsClient.on('error', (err) => console.error('[Listener] Internal receiver connection error:', err.message));
}

/**
 * Sends a payload to the internal WebSocket client.
 * @param {object} payload - The data to send.
 */
function sendToInternalClient(payload) {
    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        try {
            internalWsClient.send(JSON.stringify(payload));
        } catch (e) {
            // Error logging eliminated for performance
        }
    }
}

/**
 * Connects to the Spot BookTicker stream.
 * This function now handles BOTH the standard price tick logic AND provides data for the imbalance check.
 */
function connectToSpotBookTicker() {
    spotBookTickerClient = new WebSocket(SPOT_BOOKTICKER_URL);

    spotBookTickerClient.on('open', () => {
        console.log('[Listener] Connected to Spot BookTicker Stream.');
        last_sent_price = null; // Reset on new connection
    });

    spotBookTickerClient.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());
            
            // Ensure message is valid
            if (!message || !message.b || !message.B || !message.a || !message.A) return;

            // --- 1. HANDLE STANDARD TICK LOGIC (Formerly done by Futures Ticker) ---
            const bestBidPrice = parseFloat(message.b);
            if (!isNaN(bestBidPrice)) {
                const shouldSendPrice = (last_sent_price === null) || (Math.abs(bestBidPrice - last_sent_price) >= MINIMUM_TICK_SIZE);
                if (shouldSendPrice) {
                    payload_to_send.p = bestBidPrice;
                    sendToInternalClient(payload_to_send);
                    last_sent_price = bestBidPrice;
                }
            }

            // --- 2. UPDATE LATEST SPOT DATA for imbalance logic ---
            latestSpotData = message;

        } catch (e) {
            // Error logging eliminated for performance
        }
    });

    spotBookTickerClient.on('close', () => {
        console.log('[Listener] Disconnected from Spot BookTicker. Reconnecting...');
        spotBookTickerClient = null;
        setTimeout(connectToSpotBookTicker, RECONNECT_INTERVAL_MS);
    });
    spotBookTickerClient.on('error', (err) => console.error('[Listener] Spot BookTicker error:', err.message));
}

/**
 * Connects to the Futures Trade stream which triggers the imbalance check.
 */
function connectToFuturesTrade() {
    futuresTradeClient = new WebSocket(FUTURES_TRADE_URL);

    futuresTradeClient.on('open', () => console.log('[Listener] Connected to Futures Trade Stream.'));

    futuresTradeClient.on('message', (data) => {
        try {
            const trade = JSON.parse(data.toString());

            if (!latestSpotData.B || !latestSpotData.A) return;

            const bidQty = parseFloat(latestSpotData.B);
            const askQty = parseFloat(latestSpotData.A);
            const totalQty = bidQty + askQty;

            if (totalQty === 0) return;

            const imbalance = bidQty / totalQty;
            const isSellTrade = trade.m; // true if buyer is maker (SELL)

            if (!isSellTrade) { // BUY trade
                if (imbalance >= IMBALANCE_THRESHOLD_UPPER) {
                    const currentBestBid = parseFloat(latestSpotData.b);
                    if (!isNaN(currentBestBid)) {
                        const fakePrice = currentBestBid + FAKE_PRICE_OFFSET;
                        payload_to_send.p = fakePrice;
                        sendToInternalClient(payload_to_send);
                        console.log(`[Listener] FAKE PRICE SENT (BUY): ${fakePrice}`);
                    }
                }
            } else { // SELL trade
                if (imbalance <= IMBALANCE_THRESHOLD_LOWER) {
                    const currentBestAsk = parseFloat(latestSpotData.a);
                    if (!isNaN(currentBestAsk)) {
                        const fakePrice = currentBestAsk - FAKE_PRICE_OFFSET;
                        payload_to_send.p = fakePrice;
                        sendToInternalClient(payload_to_send);
                        console.log(`[Listener] FAKE PRICE SENT (SELL): ${fakePrice}`);
                    }
                }
            }
        } catch (e) {
            // Error logging eliminated for performance
        }
    });

    futuresTradeClient.on('close', () => {
        console.log('[Listener] Disconnected from Futures Trade Stream. Reconnecting...');
        futuresTradeClient = null;
        setTimeout(connectToFuturesTrade, RECONNECT_INTERVAL_MS);
    });
    futuresTradeClient.on('error', (err) => console.error('[Listener] Futures Trade Stream error:', err.message));
}

// --- Script Entry Point ---
connectToInternalReceiver();
connectToSpotBookTicker(); // Provides data for BOTH logics
connectToFuturesTrade();   // Triggers the imbalance logic
