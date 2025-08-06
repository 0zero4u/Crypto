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
    // MODIFIED: Client name updated
    const clientsToTerminate = [internalWsClient, spotBookTickerClient, futuresBookTickerClient];
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
const SPOT_BOOKTICKER_URL = `wss://stream.binance.com:9443/ws/${SYMBOL}@bookTicker`;
// MODIFIED: Replaced @trade with @bookTicker for the trigger
const FUTURES_BOOKTICKER_URL = `wss://fstream.binance.com/ws/${SYMBOL}@bookTicker`;

// --- WebSocket Clients and State ---
let internalWsClient, spotBookTickerClient, futuresBookTickerClient;
let last_sent_price = null;
let latestSpotData = { b: null, B: null, a: null, A: null };
let last_fake_buy_price_sent = null;
let last_fake_sell_price_sent = null;

// NEW: State to track previous futures prices to detect tick direction
let lastFuturesTick = { b: null, a: null };

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
                if ((last_sent_price === null) || (Math.abs(bestBidPrice - last_sent_price) >= MINIMUM_TICK_SIZE)) {
                    payload_to_send.p = bestBidPrice;
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

// --- MODIFIED: This function now connects to the Futures BookTicker and infers tick direction ---
function connectToFuturesBookTicker() {
    futuresBookTickerClient = new WebSocket(FUTURES_BOOKTICKER_URL);

    futuresBookTickerClient.on('open', () => {
        console.log('[Listener] Connected to Futures BookTicker Stream (Trigger).');
        // Reset last known futures prices on connect
        lastFuturesTick = { b: null, a: null };
    });

    futuresBookTickerClient.on('message', (data) => {
        try {
            const f_message = JSON.parse(data.toString());
            if (!f_message || !f_message.b || !f_message.a) return;

            const newFuturesBid = parseFloat(f_message.b);
            const newFuturesAsk = parseFloat(f_message.a);

            // On first message, just store the prices and wait for the next tick
            if (lastFuturesTick.b === null) {
                lastFuturesTick = { b: newFuturesBid, a: newFuturesAsk };
                return;
            }

            // --- Check for Spot Imbalance ---
            if (!latestSpotData.B || !latestSpotData.A) return;
            const bidQty = parseFloat(latestSpotData.B);
            const askQty = parseFloat(latestSpotData.A);
            const totalQty = bidQty + askQty;
            if (totalQty === 0) return;
            const spotImbalance = bidQty / totalQty;

            // --- Logic for BUY-side tick ---
            // If futures bid price has increased, it's an UP-tick.
            if (newFuturesBid > lastFuturesTick.b) {
                if (spotImbalance >= IMBALANCE_THRESHOLD_UPPER) {
                    const currentSpotBid = parseFloat(latestSpotData.b);
                    if (!isNaN(currentSpotBid)) {
                        const fakePrice = currentSpotBid + FAKE_PRICE_OFFSET;
                        if (fakePrice !== last_fake_buy_price_sent) {
                            payload_to_send.p = fakePrice;
                            payload_to_send.f = true;
                            sendToInternalClient(payload_to_send);
                            delete payload_to_send.f;
                            last_fake_buy_price_sent = fakePrice;
                            console.log(`[Listener] FAKE PRICE SENT (BUY): ${JSON.stringify({p: fakePrice, f: true})}`);
                        }
                    }
                }
            }
            // --- Logic for SELL-side tick ---
            // If futures ask price has decreased, it's a DOWN-tick.
            else if (newFuturesAsk < lastFuturesTick.a) {
                if (spotImbalance <= IMBALANCE_THRESHOLD_LOWER) {
                    const currentSpotAsk = parseFloat(latestSpotData.a);
                    if (!isNaN(currentSpotAsk)) {
                        const fakePrice = currentSpotAsk - FAKE_PRICE_OFFSET;
                        if (fakePrice !== last_fake_sell_price_sent) {
                            payload_to_send.p = fakePrice;
                            payload_to_send.f = true;
                            sendToInternalClient(payload_to_send);
                            delete payload_to_send.f;
                            last_fake_sell_price_sent = fakePrice;
                            console.log(`[Listener] FAKE PRICE SENT (SELL): ${JSON.stringify({p: fakePrice, f: true})}`);
                        }
                    }
                }
            }

            // Update the last known futures prices for the next comparison
            lastFuturesTick = { b: newFuturesBid, a: newFuturesAsk };

        } catch (e) { /* Performance */ }
    });

    futuresBookTickerClient.on('close', () => {
        console.log('[Listener] Disconnected from Futures BookTicker. Reconnecting...');
        futuresBookTickerClient = null;
        setTimeout(connectToFuturesBookTicker, RECONNECT_INTERVAL_MS);
    });
    futuresBookTickerClient.on('error', (err) => console.error('[Listener] Futures BookTicker error:', err.message));
}


// --- Script Entry Point ---
connectToInternalReceiver();
connectToSpotBookTicker(); // For standard ticks and providing spot data
connectToFuturesBookTicker(); // As the trigger for the fake offset logic
