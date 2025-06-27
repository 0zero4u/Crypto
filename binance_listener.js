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
const IMBALANCE_THRESHOLD = 0.8; // Initial check for favorable direction
const MINIMUM_SCORE_THRESHOLD = 87;  // Final check for signal strength
const MONITORING_WINDOW_MS = 100;

// --- Trend Score Configuration ---
const TREND_SCORE_INITIAL = 200;  // Neutral score
const TREND_SCORE_INCREMENT = 5;   // Points to add/subtract per signal
const TREND_SCORE_MAX = 5000;      // Upper bound (strong buy trend)
const TREND_SCORE_MIN = 1;        // Lower bound (strong sell trend)

// --- Rate Limiting Configuration ---
const RATE_LIMIT_MAX_MESSAGES = 3;
const RATE_LIMIT_WINDOW_MS = 250;

// --- Connection URLs ---
const internalReceiverUrl = 'ws://instance-20250627-040948.asia-south2-a.c.ace-server-460719-b7.internal:8082';
const SPOT_STREAM_URL = `wss://stream.binance.com:9443/ws/${SYMBOL}@bookTicker`;
const FUTURES_STREAM_URL = `wss://fstream.binance.com/ws/${SYMBOL}@bookTicker`;

// --- Listener State Variables ---
let internalWsClient, spotWsClient, futuresWsClient;
let last_sent_futures_price, current_spot_book_ticker, pending_futures_confirmation;
let trend_score = TREND_SCORE_INITIAL;

// --- Rate Limiting State ---
let rate_limit_state = {
    count: 0,
    window_start_time: null,
    last_signal_direction: null
};

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

function isSpotImbalanceFavorable(is_tick_up) {
    const imbalance_ratio = calculateSpotImbalanceRatio();
    if (imbalance_ratio === null) return false;
    if (is_tick_up) return imbalance_ratio >= IMBALANCE_THRESHOLD;
    else return imbalance_ratio <= (1 - IMBALANCE_THRESHOLD);
}

// --- Spot Exchange Connection ---
function connectToSpot() {
    spotWsClient = new WebSocket(SPOT_STREAM_URL);
    spotWsClient.on('open', () => { current_spot_book_ticker = null; });
    spotWsClient.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());
            current_spot_book_ticker = message;

            if (pending_futures_confirmation && isSpotImbalanceFavorable(pending_futures_confirmation.is_tick_up)) {
                const final_imbalance_ratio = calculateSpotImbalanceRatio();
                const final_score = calculateImbalanceScore(final_imbalance_ratio);

                if (final_score !== null && Math.abs(final_score) >= MINIMUM_SCORE_THRESHOLD) {
                    const signal = pending_futures_confirmation.is_tick_up ? 'buy' : 'sell';

                    // --- RATE LIMITING LOGIC ---
                    const now = Date.now();
                    const is_adverse_signal = rate_limit_state.last_signal_direction && signal !== rate_limit_state.last_signal_direction;
                    const is_window_expired = rate_limit_state.window_start_time && (now - rate_limit_state.window_start_time > RATE_LIMIT_WINDOW_MS);

                    if (is_adverse_signal || is_window_expired) {
                        rate_limit_state = { count: 0, window_start_time: null, last_signal_direction: null };
                    }
                    
                    if (rate_limit_state.count < RATE_LIMIT_MAX_MESSAGES) {
                        if (rate_limit_state.count === 0) {
                           rate_limit_state.window_start_time = now;
                        }
                        rate_limit_state.count++;
                        rate_limit_state.last_signal_direction = signal;
                        
                        if (signal === 'buy') {
                            trend_score += TREND_SCORE_INCREMENT;
                        } else {
                            trend_score -= TREND_SCORE_INCREMENT;
                        }
                        trend_score = Math.max(TREND_SCORE_MIN, Math.min(TREND_SCORE_MAX, trend_score));
                        
                        const payload = { type: 'F', p: trend_score };
                        sendToInternalClient(payload);
                        
                        last_sent_futures_price = pending_futures_confirmation.futures_price;
                    }
                    
                    clearTimeout(pending_futures_confirmation.timeoutId);
                    pending_futures_confirmation = null;
                }
            }
        } catch (e) { /* Ignore parsing errors */ }
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
            if (Math.abs(price_difference) >= MINIMUM_TICK_SIZE) {
                if (pending_futures_confirmation?.timeoutId) {
                    clearTimeout(pending_futures_confirmation.timeoutId);
                }
                pending_futures_confirmation = {
                    futures_price: current_futures_price,
                    is_tick_up: price_difference > 0,
                    timeoutId: setTimeout(() => { pending_futures_confirmation = null; }, MONITORING_WINDOW_MS)
                };
            }
        } catch (e) { /* Ignore parsing errors */ }
    });
    futuresWsClient.on('error', (err) => console.error('[Futures] Connection error:', err.message));
    futuresWsClient.on('close', () => {
        futuresWsClient = null;
        setTimeout(connectToFutures, RECONNECT_INTERVAL_MS);
    });
}

// --- Start all connections ---
connectToInternalReceiver();
connectToSpot();
connectToFutures();
