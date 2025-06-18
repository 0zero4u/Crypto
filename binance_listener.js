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

// --- State Management (ROBUST VERSION) ---
function cleanupAndExit(exitCode = 1) {
    if (pending_futures_confirmation?.timeoutId) {
        clearTimeout(pending_futures_confirmation.timeoutId);
    }
    const clientsToTerminate = [internalWsClient, spotWsClient, futuresWsClient];
    
    console.error('[Listener] Initiating cleanup...');
    clientsToTerminate.forEach(client => {
        if (client && (client.readyState === WebSocket.OPEN || client.readyState === WebSocket.CONNECTING)) {
            try { client.terminate(); } catch (e) { console.error(`[Listener] Error during WebSocket termination: ${e.message}`); }
        }
    });
    
    setTimeout(() => {
        console.error(`[Listener] Exiting with code ${exitCode}.`);
        process.exit(exitCode);
    }, 1000).unref();
}

// --- Listener Configuration ---
const SYMBOL = 'btcusdt';
const RECONNECT_INTERVAL_MS = 5000;
const MINIMUM_TICK_SIZE = 0.1;
const MINIMUM_SCORE_THRESHOLD = 87;  // Final check for signal strength (applied to the average score)
const MONITORING_WINDOW_MS = 100; // Timeout for a spot confirmation to arrive

// --- Connection URLs ---
const internalReceiverUrl = 'ws://localhost:8082';
const SPOT_STREAM_URL = `wss://stream.binance.com:9443/ws/${SYMBOL}@bookTicker`;
const FUTURES_STREAM_URL = `wss://fstream.binance.com/ws/${SYMBOL}@bookTicker`;

// --- Listener State Variables ---
let internalWsClient, spotWsClient, futuresWsClient;
let last_sent_spot_price, last_sent_futures_price, current_spot_book_ticker, pending_futures_confirmation;
let last_successful_f_score = null; // NEW: Stores the score of the last successful 'F' signal

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
    }
}

// --- Imbalance Logic ---
function calculateSpotImbalanceRatio() {
    if (!current_spot_book_ticker) return null;
    const bid_qty = parseFloat(current_spot_book_ticker.B);
    const ask_qty = parseFloat(current_spot_book_ticker.A);
    const total_volume = bid_qty + ask_qty;
    return total_volume > 0 ? (bid_qty / total_volume) : null;
}

function calculateImbalanceScore(imbalance_ratio) {
    if (imbalance_ratio === null) return null;
    const score = (imbalance_ratio - 0.5) * 200;
    return Math.round(score);
}

// --- Spot Exchange Connection ---
function connectToSpot() {
    spotWsClient = new WebSocket(SPOT_STREAM_URL);
    spotWsClient.on('open', () => { [last_sent_spot_price, current_spot_book_ticker] = [null, null]; });
    spotWsClient.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());
            current_spot_book_ticker = message;

            // Handler 1: Independent Spot Tick
            const current_spot_price = parseFloat(message.b);
            if (current_spot_price && (last_sent_spot_price === null || Math.abs(current_spot_price - last_sent_spot_price) >= MINIMUM_TICK_SIZE)) {
                sendToInternalClient({ type: 'S', p: current_spot_price });
                last_sent_spot_price = current_spot_price;
            }

            // Handler 2: One-shot check for a pending futures signal (MODIFIED LOGIC)
            if (pending_futures_confirmation) {
                const current_pure_score = calculateImbalanceScore(calculateSpotImbalanceRatio());
                if (current_pure_score === null) return; // Not enough data to score, wait for next tick or timeout

                // Calculate average using the last successful score
                const average_score = last_successful_f_score === null
                    ? current_pure_score
                    : Math.round((current_pure_score + last_successful_f_score) / 2);
                
                // Check 1: Is the average score strong enough?
                const is_strong_enough = Math.abs(average_score) >= MINIMUM_SCORE_THRESHOLD;
                // Check 2: Does the score direction match the futures tick?
                const is_direction_correct = pending_futures_confirmation.is_tick_up ? average_score > 0 : average_score < 0;

                if (is_strong_enough && is_direction_correct) {
                    const signal = pending_futures_confirmation.is_tick_up ? 'buy' : 'sell';
                    const payload = {
                        type: 'F',
                        p: pending_futures_confirmation.futures_price,
                        s: current_pure_score,
                        s_avg: average_score,
                        sig: signal
                    };
                    sendToInternalClient(payload);

                    // CRITICAL: Update state only on successful signal
                    last_sent_futures_price = pending_futures_confirmation.futures_price;
                    last_successful_f_score = current_pure_score;
                }
                
                // IMPORTANT: Reset state after this one-shot check, whether signal was sent or not.
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
                last_sent_futures_price = current_futures_price;
                return;
            }

            const price_difference = current_futures_price - last_sent_futures_price;
            // Initiate a pending confirmation ONLY if one isn't already active
            if (Math.abs(price_difference) >= MINIMUM_TICK_SIZE && !pending_futures_confirmation) {
                pending_futures_confirmation = {
                    futures_price: current_futures_price,
                    is_tick_up: price_difference > 0,
                    // This timeout is a failsafe to clear the pending state if no spot update arrives
                    timeoutId: setTimeout(() => { pending_futures_confirmation = null; }, MONITORING_WINDOW_MS)
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
console.log(`[Listener] Starting High-Conviction Signal Monitor.`);
console.log(`-- Config: Averaging current score with the last successful signal's score.`);
console.log(`-- Config: Requires average score magnitude >= ${MINIMUM_SCORE_THRESHOLD}`);
connectToInternalReceiver();
connectToSpot();
connectToFutures();
