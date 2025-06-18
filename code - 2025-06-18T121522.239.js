const WebSocket = require('ws');

// --- Global Error Handlers ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`);
    console.error(err.stack || err);
    cleanupAndExit(1);
});
process.on('unhandledRejection', (reason, promise) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNHANDLED PROMISE REJECTION`);
    console.error('[Listener] Unhandled Rejection at:', promise);
    console.error('[Listener] Reason:', reason instanceof Error ? reason.stack : reason);
    cleanupAndExit(1);
});

// --- State Management ---
function cleanupAndExit(exitCode = 1) {
    if (pending_futures_confirmation?.timeoutId) {
        clearTimeout(pending_futures_confirmation.timeoutId);
    }
    const clientsToTerminate = [internalWsClient, spotWsClient, futuresWsClient];
    clientsToTerminate.forEach(client => {
        if (client && typeof client.terminate === 'function') {
            try { client.terminate(); } catch (e) { console.error(`[Listener] Error terminating a WebSocket client: ${e.message}`); }
        }
    });
    setTimeout(() => { process.exit(exitCode); }, 1000).unref();
}

// --- Listener Configuration ---
const SYMBOL = 'btcusdt';
const RECONNECT_INTERVAL_MS = 5000;
const MINIMUM_TICK_SIZE = 0.1;
const IMBALANCE_THRESHOLD = 0.6;
const MONITORING_WINDOW_MS = 100;

// --- Connection URLs ---
const internalReceiverUrl = 'ws://localhost:8082';
const SPOT_STREAM_URL = `wss://stream.binance.com:9443/ws/${SYMBOL}@bookTicker`;
const FUTURES_STREAM_URL = `wss://fstream.binance.com/ws/${SYMBOL}@bookTicker`;

// --- Listener State Variables ---
let internalWsClient = null;
let spotWsClient = null;
let futuresWsClient = null;

let last_sent_spot_price = null;
let last_sent_futures_price = null;
let current_spot_book_ticker = null;

// This object holds the state for a pending confirmation. It's null when not monitoring.
let pending_futures_confirmation = null;

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) return;
    internalWsClient = new WebSocket(internalReceiverUrl);
    internalWsClient.on('error', (err) => console.error(`[Internal] WebSocket error: ${err.message}`));
    internalWsClient.on('close', () => {
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
}

// --- Data Forwarding ---
function sendToInternalClient(payload) {
    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        try {
            internalWsClient.send(JSON.stringify(payload));
        } catch (e) { console.error(`[Internal] Failed to send message: ${e.message}`); }
    } else {
        console.error('[Internal] Cannot send message, receiver not connected.');
    }
}

// --- Imbalance Calculation & Checking ---
function calculateSpotImbalance() {
    if (!current_spot_book_ticker) return null;
    const bid_qty = parseFloat(current_spot_book_ticker.B);
    const ask_qty = parseFloat(current_spot_book_ticker.A);
    const total_volume = bid_qty + ask_qty;
    return total_volume > 0 ? (bid_qty / total_volume) : null;
}

function isSpotImbalanceFavorable(is_tick_up) {
    const imbalance_ratio = calculateSpotImbalance();
    if (imbalance_ratio === null) return false;
    if (is_tick_up) return imbalance_ratio >= IMBALANCE_THRESHOLD;
    else return imbalance_ratio <= (1 - IMBALANCE_THRESHOLD);
}

// --- Spot Exchange Connection ---
function connectToSpot() {
    spotWsClient = new WebSocket(SPOT_STREAM_URL);
    spotWsClient.on('open', () => {
        last_sent_spot_price = null;
        current_spot_book_ticker = null;
    });
    spotWsClient.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());
            current_spot_book_ticker = message; // Always keep state fresh

            // --- Handler 1: Independent Spot Tick Logic ---
            const current_spot_price = parseFloat(message.b);
            if (current_spot_price && (last_sent_spot_price === null || Math.abs(current_spot_price - last_sent_spot_price) >= MINIMUM_TICK_SIZE)) {
                sendToInternalClient({ type: 'S', p: current_spot_price });
                last_sent_spot_price = current_spot_price;
            }

            // --- Handler 2: Check for Pending Futures Confirmation ---
            if (pending_futures_confirmation && isSpotImbalanceFavorable(pending_futures_confirmation.is_tick_up)) {
                // Confirmation received! Send the payload.
                const final_imbalance = calculateSpotImbalance();
                const payload = { type: 'F', p: pending_futures_confirmation.futures_price, imb: final_imbalance };
                sendToInternalClient(payload);
                
                last_sent_futures_price = pending_futures_confirmation.futures_price;

                // Disarm the trigger
                clearTimeout(pending_futures_confirmation.timeoutId);
                pending_futures_confirmation = null;
            }
        } catch (e) { /* Ignore */ }
    });
    spotWsClient.on('error', (err) => console.error('[Spot] Connection error:', err.message));
    spotWsClient.on('close', () => {
        spotWsClient = null;
        setTimeout(connectToSpot, RECONNECT_INTERVAL_MS);
    });
}

// --- Futures (Leader) Exchange Connection ---
function connectToFutures() {
    futuresWsClient = new WebSocket(FUTURES_STREAM_URL);
    futuresWsClient.on('open', () => { last_sent_futures_price = null; });
    futuresWsClient.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());
            const current_futures_price = parseFloat(message.b);
            if (!current_futures_price) return;
            
            if (last_sent_futures_price === null) {
                last_sent_futures_price = current_futures_price; // Set baseline on first tick
                return;
            }

            const price_difference = current_futures_price - last_sent_futures_price;
            if (Math.abs(price_difference) >= MINIMUM_TICK_SIZE) {
                // A valid tick occurred. Arm the confirmation trigger.
                if (pending_futures_confirmation?.timeoutId) {
                    clearTimeout(pending_futures_confirmation.timeoutId); // Invalidate previous pending tick
                }

                pending_futures_confirmation = {
                    futures_price: current_futures_price,
                    is_tick_up: price_difference > 0,
                    // Set a timeout to cancel this confirmation if it's not met in time
                    timeoutId: setTimeout(() => {
                        pending_futures_confirmation = null;
                    }, MONITORING_WINDOW_MS)
                };
            }
        } catch (e) { /* Ignore */ }
    });
    futuresWsClient.on('error', (err) => console.error('[Futures] Connection error:', err.message));
    futuresWsClient.on('close', () => {
        futuresWsClient = null;
        setTimeout(connectToFutures, RECONNECT_INTERVAL_MS);
    });
}

// --- Start all connections ---
console.log(`[Listener] Starting Event-Driven Hybrid Monitor.`);
console.log(`-- Config: Tick Size >= ${MINIMUM_TICK_SIZE}, Imbalance >= ${IMBALANCE_THRESHOLD*100}%, Window: ${MONITORING_WINDOW_MS}ms`);
connectToInternalReceiver();
connectToSpot();
connectToFutures();