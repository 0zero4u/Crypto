// --- START OF FILE predictive_listener_advanced.js ---

const WebSocket = require('ws');

// --- Global Error Handlers (unchanged) ---
process.on('uncaughtException', (err, origin) => { /* ... */ });
process.on('unhandledRejection', (reason, promise) => { /* ... */ });
function cleanupAndExit(exitCode = 1) { /* ... */ }


// --- Configuration ---
const SYMBOL = 'BTCUSDT';
const internalReceiverUrl = 'ws://localhost:8082';
const RECONNECT_INTERVAL_MS = 5000;

const STREAM_NAMES = [`${SYMBOL.toLowerCase()}@bookTicker`, `${SYMBOL.toLowerCase()}@trade`];
const BINANCE_COMBINED_STREAM_URL = `wss://stream.binance.com:9443/stream?streams=${STREAM_NAMES.join('/')}`;

// --- UPDATED: Predictive Model Parameters ---

// Weights (α, β, γ, δ) - Tuned for new components
const WEIGHT_IMBALANCE = 0.30;           // α: Static imbalance is slightly less important now
const WEIGHT_IMBALANCE_MOMENTUM = 0.30;  // β: Momentum is a key indicator of urgency
const WEIGHT_AGGRESSIVE_TRADE = 0.30;    // γ: EMA-based trade score
const WEIGHT_DISAPPEARANCE_RATE = 0.10;  // δ: Remains a useful indicator of large player actions

// EMA and Momentum Parameters
const TRADE_EMA_PERIOD = 20; // Lookback period for trade aggression EMA (fades older trades)
const IMBALANCE_SHORT_WINDOW = 5;  // Short-term average for imbalance (last 5 updates)
const IMBALANCE_LONG_WINDOW = 15; // Long-term average for imbalance (last 15 updates)
const IMBALANCE_HISTORY_SIZE = IMBALANCE_LONG_WINDOW; // We only need to store enough for the longest window

// Thresholds
const DISAPPEARANCE_QTY_DROP_THRESHOLD = 0.15; // Lowered for more sensitivity
const PREDICT_SCORE_BUY_THRESHOLD = 0.5;   // Adjusted for new score distribution
const PREDICT_SCORE_SELL_THRESHOLD = -0.5; // Adjusted for new score distribution
const HYSTERESIS_BUFFER = 0.08;


// --- State Variables ---
let binanceWsClient = null;
let internalWsClient = null;
let lastSentSignal = 'Neutral';

let state = {
    bestBidPrice: 0, bestBidQty: 0, bestAskPrice: 0, bestAskQty: 0,
    imbalanceHistory: [],
    // Component scores
    imbalancePercent: 0,
    imbalanceMomentum: 0, // NEW: Replaces deltaImbalance
    aggressiveTradeScore: 0, // Now calculated via EMA
    orderDisappearanceRate: 0,
    // EMA state for trade aggression
    emaAggression: 0,
    emaAbsTradeSize: 0,
};

// --- Internal Receiver Connection (unchanged) ---
function connectToInternalReceiver() { /* ... */ }

// --- Predictive Score Calculation ---

function updateImbalanceAndMomentum() {
    const { bestBidQty, bestAskQty } = state;
    const totalQty = bestBidQty + bestAskQty;

    // 1. Calculate current Imbalance%
    state.imbalancePercent = totalQty > 0 ? (bestBidQty - bestAskQty) / totalQty : 0;

    // 2. Update history buffer
    state.imbalanceHistory.push(state.imbalancePercent);
    if (state.imbalanceHistory.length > IMBALANCE_HISTORY_SIZE) {
        state.imbalanceHistory.shift();
    }
    
    // 3. Calculate Imbalance Momentum (Short Avg vs Long Avg)
    if (state.imbalanceHistory.length >= IMBALANCE_LONG_WINDOW) {
        const short_slice = state.imbalanceHistory.slice(-IMBALANCE_SHORT_WINDOW);
        const long_slice = state.imbalanceHistory; // Already at max size

        const short_avg = short_slice.reduce((a, b) => a + b, 0) / short_slice.length;
        const long_avg = long_slice.reduce((a, b) => a + b, 0) / long_slice.length;
        
        // The difference shows the "urgency" or momentum
        state.imbalanceMomentum = short_avg - long_avg;
    }
}

// REWRITTEN: To use EMA for decaying importance
function processNewTrade(trade) {
    const { bestBidPrice, bestAskPrice } = state;
    const tradePrice = parseFloat(trade.p);
    const tradeQty = parseFloat(trade.q);
    const alpha = 2 / (TRADE_EMA_PERIOD + 1);

    let tradeAggression = 0;
    if (bestBidPrice > 0 && tradePrice <= bestBidPrice) tradeAggression = -tradeQty; // Market sell
    else if (bestAskPrice > 0 && tradePrice >= bestAskPrice) tradeAggression = +tradeQty; // Market buy
    else return; // Ignore trades that aren't clearly aggressive

    // Update the EMA for net aggression (buys are positive, sells are negative)
    state.emaAggression = (tradeAggression * alpha) + (state.emaAggression * (1 - alpha));

    // Update the EMA for average absolute trade size (for normalization)
    state.emaAbsTradeSize = (tradeQty * alpha) + (state.emaAbsTradeSize * (1 - alpha));

    // The final score is the normalized EMA of aggression
    if (state.emaAbsTradeSize > 0) {
        state.aggressiveTradeScore = state.emaAggression / state.emaAbsTradeSize;
        state.aggressiveTradeScore = Math.max(-1, Math.min(1, state.aggressiveTradeScore)); // Clamp
    }
}

// Order disappearance logic is conceptually fine, no major changes needed
function updateOrderDisappearanceRate(prevBidQty, prevAskQty) { /* ... same as before ... */ }

// UPDATED: Uses new components and weights
function calculateAndSendPredictScore() {
    const { imbalancePercent, imbalanceMomentum, aggressiveTradeScore, orderDisappearanceRate } = state;

    // The new formula, emphasizing momentum and recent trades
    const score = (WEIGHT_IMBALANCE * imbalancePercent) +
                  (WEIGHT_IMBALANCE_MOMENTUM * imbalanceMomentum) +
                  (WEIGHT_AGGRESSIVE_TRADE * aggressiveTradeScore) +
                  (WEIGHT_DISAPPEARANCE_RATE * orderDisappearanceRate);
    
    let newSignal = lastSentSignal;

    if (lastSentSignal === 'Neutral') {
        if (score > PREDICT_SCORE_BUY_THRESHOLD) newSignal = 'Strong Buy';
        else if (score < PREDICT_SCORE_SELL_THRESHOLD) newSignal = 'Strong Sell';
    } else if (lastSentSignal === 'Strong Buy') {
        if (score < PREDICT_SCORE_BUY_THRESHOLD - HYSTERESIS_BUFFER) newSignal = 'Neutral';
    } else if (lastSentSignal === 'Strong Sell') {
        if (score > PREDICT_SCORE_SELL_THRESHOLD + HYSTERESIS_BUFFER) newSignal = 'Neutral';
    }

    if (newSignal !== lastSentSignal) {
        const payload = {
            signal: newSignal,
            score: parseFloat(score.toFixed(4)),
            components: {
                imbalance: parseFloat(imbalancePercent.toFixed(4)),
                momentum: parseFloat(imbalanceMomentum.toFixed(4)), // Renamed from deltaImbalance
                aggression: parseFloat(aggressiveTradeScore.toFixed(4)),
                disappearance: parseFloat(orderDisappearanceRate.toFixed(4))
            },
            timestamp: Date.now()
        };
        sendToInternalClient(payload);
        lastSentSignal = newSignal;
        console.log(`[Predictor] SIGNAL CHANGE: ${newSignal.padEnd(12)}| Score: ${payload.score.toFixed(4)} | Momentum: ${payload.components.momentum}`);
    }
}

function sendToInternalClient(payload) { /* ... same as before ... */ }


// --- Single Binance Stream Connection ---
function connectToBinanceStreams() {
    // ... same as before ...
    binanceWsClient.on('message', (data) => {
        try {
            const wrappedMessage = JSON.parse(data.toString());
            const streamName = wrappedMessage.stream;
            const eventData = wrappedMessage.data;

            if (streamName === STREAM_NAMES[0]) { // bookTicker
                const prevBidQty = state.bestBidQty;
                const prevAskQty = state.bestAskQty;
                state.bestBidPrice = parseFloat(eventData.b); state.bestBidQty = parseFloat(eventData.B);
                state.bestAskPrice = parseFloat(eventData.a); state.bestAskQty = parseFloat(eventData.A);
                
                updateImbalanceAndMomentum(); // UPDATED function call
                updateOrderDisappearanceRate(prevBidQty, prevAskQty);
                calculateAndSendPredictScore();

            } else if (streamName === STREAM_NAMES[1]) { // trade
                if (eventData.p && eventData.q) {
                    processNewTrade(eventData);
                }
            }
        } catch (e) {
            console.error(`[Predictor] CRITICAL ERROR in Combined Stream handler: ${e.message}`, e.stack);
        }
    });
    // ... rest of function is the same ...
}

// --- Start the application (unchanged) ---
console.log(`[Predictor] Starting up Advanced Model for symbol: ${SYMBOL}...`);
connectToInternalReceiver();
connectToBinanceStreams();
// --- END OF FILE predictive_listener_advanced.js ---
