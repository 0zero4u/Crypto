// --- START OF FILE predictive_listener_combined.js ---

const WebSocket = require('ws');

// --- Global Error Handlers ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[Predictor] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`);
    console.error(err.stack || err);
    console.error(`[Predictor] Exception origin: ${origin}`);
    console.error(`[Predictor] PID: ${process.pid} --- Exiting due to uncaught exception...`);
    cleanupAndExit(1);
});

process.on('unhandledRejection', (reason, promise) => {
    console.error(`[Predictor] PID: ${process.pid} --- FATAL: UNHANDLED PROMISE REJECTION`);
    console.error('[Predictor] Unhandled Rejection at:', promise);
    console.error('[Predictor] Reason:', reason instanceof Error ? reason.stack : reason);
    console.error(`[Predictor] PID: ${process.pid} --- Exiting due to unhandled promise rejection...`);
    cleanupAndExit(1);
});

function cleanupAndExit(exitCode = 1) {
    console.log('[Predictor] Initiating graceful shutdown...');
    const clientsToTerminate = [internalWsClient, binanceWsClient];
    clientsToTerminate.forEach(client => {
        if (client && typeof client.terminate === 'function') {
            try { client.terminate(); } catch (e) { console.error(`[Predictor] Error terminating a WebSocket client: ${e.message}`); }
        }
    });

    setTimeout(() => {
        console.log(`[Predictor] Exiting with code ${exitCode}.`);
        process.exit(exitCode);
    }, 1000).unref();
}

// --- Configuration ---
const SYMBOL = 'BTCUSDT';
const internalReceiverUrl = 'ws://localhost:8082';
const RECONNECT_INTERVAL_MS = 5000;

// --- Binance Combined WebSocket URL ---
const STREAM_NAMES = [
    `${SYMBOL.toLowerCase()}@bookTicker`,
    `${SYMBOL.toLowerCase()}@trade`
];
const BINANCE_COMBINED_STREAM_URL = `wss://stream.binance.com:9443/stream?streams=${STREAM_NAMES.join('/')}`;

// --- Predictive Model Parameters ---
const WEIGHT_IMBALANCE = 0.35;
const WEIGHT_DELTA_IMBALANCE = 0.20;
const WEIGHT_AGGRESSIVE_TRADE = 0.35;
const WEIGHT_DISAPPEARANCE_RATE = 0.10;

const IMBALANCE_BUFFER_SIZE = 10;
const TRADE_BUFFER_SIZE = 50;

const DISAPPEARANCE_QTY_DROP_THRESHOLD = 0.30;
const PREDICT_SCORE_BUY_THRESHOLD = 0.6;
const PREDICT_SCORE_SELL_THRESHOLD = -0.6;

// NEW: Hysteresis buffer to prevent signal flapping
const HYSTERESIS_BUFFER = 0.1;


// --- State Variables ---
let binanceWsClient = null;
let internalWsClient = null;
let lastSentSignal = 'Neutral'; // Initialize as Neutral

let state = {
    bestBidPrice: 0, bestBidQty: 0, bestAskPrice: 0, bestAskQty: 0,
    imbalanceHistory: [],
    tradeHistory: [],
    imbalancePercent: 0, deltaImbalance: 0, aggressiveTradeScore: 0, orderDisappearanceRate: 0
};

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) return;
    internalWsClient = new WebSocket(internalReceiverUrl);
    internalWsClient.on('error', (err) => console.error(`[Predictor] Internal receiver WebSocket error: ${err.message}`));
    internalWsClient.on('close', () => {
        console.log('[Predictor] Connection to internal receiver closed. Reconnecting...');
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
    internalWsClient.on('open', () => console.log('[Predictor] Connected to internal receiver at ' + internalReceiverUrl));
}

// --- Predictive Score Calculation (Helper functions are unchanged) ---

function updateImbalance() {
    const { bestBidQty, bestAskQty } = state;
    const totalQty = bestBidQty + bestAskQty;
    state.imbalancePercent = totalQty > 0 ? (bestBidQty - bestAskQty) / totalQty : 0;
    state.imbalanceHistory.push(state.imbalancePercent);
    if (state.imbalanceHistory.length > IMBALANCE_BUFFER_SIZE) state.imbalanceHistory.shift();
    if (state.imbalanceHistory.length === IMBALANCE_BUFFER_SIZE) {
        const delta = state.imbalanceHistory[state.imbalanceHistory.length - 1] - state.imbalanceHistory[0];
        // Clamp delta imbalance to [-1, 1] to prevent it from having outsized influence
        state.deltaImbalance = Math.max(-1, Math.min(1, delta));
    } else {
        state.deltaImbalance = 0;
    }
}

function updateOrderDisappearanceRate(prevBidQty, prevAskQty) {
    const { bestBidQty, bestAskQty } = state;
    let bidDisappearance = 0, askDisappearance = 0;
    if (prevBidQty > 0) {
        const bidQtyDrop = prevBidQty - bestBidQty;
        if (bidQtyDrop > 0 && (bidQtyDrop / prevBidQty) > DISAPPEARANCE_QTY_DROP_THRESHOLD) {
            bidDisappearance = bidQtyDrop / prevBidQty;
        }
    }
    if (prevAskQty > 0) {
        const askQtyDrop = prevAskQty - bestAskQty;
        if (askQtyDrop > 0 && (askQtyDrop / prevAskQty) > DISAPPEARANCE_QTY_DROP_THRESHOLD) {
            askDisappearance = askQtyDrop / prevAskQty;
        }
    }
    state.orderDisappearanceRate = bidDisappearance - askDisappearance;
}

function processNewTrade(trade) {
    const { bestBidPrice, bestAskPrice } = state;
    const tradePrice = parseFloat(trade.p), tradeQty = parseFloat(trade.q);
    let side = 'neutral';
    if (bestBidPrice > 0 && tradePrice <= bestBidPrice) side = 'sell';
    else if (bestAskPrice > 0 && tradePrice >= bestAskPrice) side = 'buy';
    if (side !== 'neutral') {
        state.tradeHistory.push({ side, qty: tradeQty });
        if (state.tradeHistory.length > TRADE_BUFFER_SIZE) state.tradeHistory.shift();
    }
    let buyQtySum = 0, sellQtySum = 0;
    state.tradeHistory.forEach(t => {
        if (t.side === 'buy') buyQtySum += t.qty; else sellQtySum += t.qty;
    });
    const totalTradeVol = buyQtySum + sellQtySum;
    state.aggressiveTradeScore = totalTradeVol > 0 ? (buyQtySum - sellQtySum) / totalTradeVol : 0;
}

// --- UPDATED: Main Signal Calculation with Hysteresis ---
function calculateAndSendPredictScore() {
    const { imbalancePercent, deltaImbalance, aggressiveTradeScore, orderDisappearanceRate } = state;
    const score = (WEIGHT_IMBALANCE * imbalancePercent) + (WEIGHT_DELTA_IMBALANCE * deltaImbalance) + (WEIGHT_AGGRESSIVE_TRADE * aggressiveTradeScore) + (WEIGHT_DISAPPEARANCE_RATE * orderDisappearanceRate);
    
    let newSignal = lastSentSignal;

    // State machine logic with hysteresis
    if (lastSentSignal === 'Neutral') {
        if (score > PREDICT_SCORE_BUY_THRESHOLD) {
            newSignal = 'Strong Buy';
        } else if (score < PREDICT_SCORE_SELL_THRESHOLD) {
            newSignal = 'Strong Sell';
        }
    } else if (lastSentSignal === 'Strong Buy') {
        // To exit 'Strong Buy', the score must drop significantly below the threshold
        if (score < PREDICT_SCORE_BUY_THRESHOLD - HYSTERESIS_BUFFER) {
            newSignal = 'Neutral';
        }
    } else if (lastSentSignal === 'Strong Sell') {
        // To exit 'Strong Sell', the score must rise significantly above the threshold
        if (score > PREDICT_SCORE_SELL_THRESHOLD + HYSTERESIS_BUFFER) {
            newSignal = 'Neutral';
        }
    }

    // This is the crucial check: only send an update if the signal has *changed*
    if (newSignal !== lastSentSignal) {
        const payload = {
            signal: newSignal,
            score: parseFloat(score.toFixed(4)),
            components: {
                imbalance: parseFloat(imbalancePercent.toFixed(4)),
                deltaImbalance: parseFloat(deltaImbalance.toFixed(4)),
                aggression: parseFloat(aggressiveTradeScore.toFixed(4)),
                disappearance: parseFloat(orderDisappearanceRate.toFixed(4))
            },
            timestamp: Date.now()
        };
        sendToInternalClient(payload);
        // CRITICAL: Update the state for the next evaluation
        lastSentSignal = newSignal;
        console.log(`[Predictor] SIGNAL CHANGE: ${newSignal.padEnd(12)}| Score: ${payload.score.toFixed(4)}`);
    }
}

function sendToInternalClient(payload) {
    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        try {
            internalWsClient.send(JSON.stringify(payload));
        } catch (sendError) {
            console.error(`[Predictor] Error sending data to internal receiver: ${sendError.message}`);
        }
    }
}

// --- Single Binance Stream Connection (Unchanged) ---
function connectToBinanceStreams() {
    if (binanceWsClient && (binanceWsClient.readyState === WebSocket.OPEN || binanceWsClient.readyState === WebSocket.CONNECTING)) return;
    
    console.log(`[Predictor] Connecting to Binance Combined Stream: ${BINANCE_COMBINED_STREAM_URL}`);
    binanceWsClient = new WebSocket(BINANCE_COMBINED_STREAM_URL);

    binanceWsClient.on('open', () => console.log('[Predictor] Successfully connected to Binance Combined Stream.'));

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
                updateImbalance();
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

    binanceWsClient.on('error', (err) => console.error(`[Predictor] Binance WebSocket error: ${err.message}`));
    
    binanceWsClient.on('close', () => {
        console.log('[Predictor] Binance connection closed. Reconnecting...');
        binanceWsClient = null;
        setTimeout(connectToBinanceStreams, RECONNECT_INTERVAL_MS);
    });
}

// --- Start the application ---
console.log(`[Predictor] Starting up for symbol: ${SYMBOL}...`);
connectToInternalReceiver();
connectToBinanceStreams();
// --- END OF FILE predictive_listener_combined.js ---
