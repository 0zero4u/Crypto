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
    setTimeout(() => {
        console.error(`[Listener] Exiting with code ${exitCode}.`);
        process.exit(exitCode);
    }, 1000).unref();
}

// --- Configuration ---
const SYMBOL = 'btcusdt';
const RECONNECT_INTERVAL_MS = 5000;
const MINIMUM_TICK_SIZE = 0.2;
const IMBALANCE_THRESHOLD_UPPER = 0.9;
const IMBALANCE_THRESHOLD_LOWER = 0.1;
const FAKE_PRICE_OFFSET = 1.0;

// --- URLS ---
const internalReceiverUrl = 'ws://instance-20250627-040948.asia-south2-a.c.ace-server-460719-b7.internal:8082/internal';
const FUTURES_TRADE_URL = `wss://fstream.binance.com/ws/${SYMBOL}@trade`;
const SPOT_BOOKTICKER_URL = `wss://stream.binance.com:9443/ws/${SYMBOL}@bookTicker`;

// --- WebSocket Clients and State ---
let internalWsClient, spotBookTickerClient, futuresTradeClient;
let last_sent_price = null;
let latestSpotData = { b: null, B: null, a: null, A: null };
let last_fake_buy_price_sent = null;
let last_fake_sell_price_sent = null;

// Reusable payload object. The 'f' key will be added/removed as needed.
const payload_to_send = { type: 'S', p: 0.0 };

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

function sendToInternalClient(payload) {
    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        try {
            internalWsClient.send(JSON.stringify(payload));
        } catch (e) { /* Performance */ }
    }
}

function connectToSpotBookTicker() {
    spotBookTickerClient = new WebSocket(SPOT_BOOKTICKER_URL);

    spotBookTickerClient.on('open', () => {
        console.log('[Listener] Connected to Spot BookTicker Stream.');
        last_sent_price = null;
        last_fake_buy_price_sent = null;
        last_fake_sell_price_sent = null;
    });

    spotBookTickerClient.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());
            if (!message || !message.b || !message.B || !message.a || !message.A) return;

            const bestBidPrice = parseFloat(message.b);
            if (!isNaN(bestBidPrice)) {
                const shouldSendPrice = (last_sent_price === null) || (Math.abs(bestBidPrice - last_sent_price) >= MINIMUM_TICK_SIZE);
                if (shouldSendPrice) {
                    payload_to_send.p = bestBidPrice;
                    // Standard ticks do not have the 'f' flag.
                    sendToInternalClient(payload_to_send);
                    last_sent_price = bestBidPrice;
                }
            }
            latestSpotData = message;
        } catch (e) { /* Performance */ }
    });

    spotBookTickerClient.on('close', () => {
        console.log('[Listener] Disconnected from Spot BookTicker. Reconnecting...');
        spotBookTickerClient = null;
        setTimeout(connectToSpotBookTicker, RECONNECT_INTERVAL_MS);
    });
    spotBookTickerClient.on('error', (err) => console.error('[Listener] Spot BookTicker error:', err.message));
}

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
            const isSellTrade = trade.m;

            if (!isSellTrade) { // BUY trade
                if (imbalance >= IMBALANCE_THRESHOLD_UPPER) {
                    const currentBestBid = parseFloat(latestSpotData.b);
                    if (!isNaN(currentBestBid)) {
                        const fakePrice = currentBestBid + FAKE_PRICE_OFFSET;
                        if (fakePrice !== last_fake_buy_price_sent) {
                            payload_to_send.p = fakePrice;
                            payload_to_send.f = true; // --- MODIFIED: Add the fake flag ---
                            sendToInternalClient(payload_to_send);
                            delete payload_to_send.f; // --- MODIFIED: Remove flag immediately after sending ---
                            last_fake_buy_price_sent = fakePrice;
                            console.log(`[Listener] FAKE PRICE SENT (BUY): ${JSON.stringify({p: fakePrice, f: true})}`);
                        }
                    }
                }
            } else { // SELL trade
                if (imbalance <= IMBALANCE_THRESHOLD_LOWER) {
                    const currentBestAsk = parseFloat(latestSpotData.a);
                    if (!isNaN(currentBestAsk)) {
                        const fakePrice = currentBestAsk - FAKE_PRICE_OFFSET;
                        if (fakePrice !== last_fake_sell_price_sent) {
                            payload_to_send.p = fakePrice;
                            payload_to_send.f = true; // --- MODIFIED: Add the fake flag ---
                            sendToInternalClient(payload_to_send);
                            delete payload_to_send.f; // --- MODIFIED: Remove flag immediately after sending ---
                            last_fake_sell_price_sent = fakePrice;
                            console.log(`[Listener] FAKE PRICE SENT (SELL): ${JSON.stringify({p: fakePrice, f: true})}`);
                        }
                    }
                }
            }
        } catch (e) { /* Performance */ }
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
connectToSpotBookTicker();
connectToFuturesTrade();
