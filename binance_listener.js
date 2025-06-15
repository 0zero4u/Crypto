// --- START OF FILE predictive_listener_advanced.js ---

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

const STREAM_NAMES = [`${SYMBOL.toLowerCase()}@bookTicker`, `${SYMBOL.toLowerCase()}@trade`];
const BINANCE_COMBINED_STREAM_URL = `wss://stream.binance.com:9443/stream?streams=${STREAM_NAMES.join('/')}`;

// --- Predictive Model Parameters ---
const WEIGHT_IMBALANCE = 0.30;
const WEIGHT_IMBALANCE_MOMENTUM = 0.30;
const WEIGHT_AGGRESSIVE_TRADE = 0.30;
const WEIGHT_DISAPPEARANCE_RATE = 0.10;

const TRADE_EMA_PERIOD = 20;
const IMBALANCE_SHORT_WINDOW = 5;
const IMBALANCE_LONG_WINDOW = 15;
const IMBALANCE_HISTORY_SIZE = IMBALANCE_LONG_WINDOW;

const DISAPPEARANCE_QTY_DROP_THRESHOLD = 0.15;
const PREDICT_SCORE_BUY_THRESHOLD = 0.5;
const PREDICT_SCORE_SELL_THRESHOLD = -0.5;
const HYSTERESIS_BUFFER = 0.08;


// --- State Variables ---
let binanceWsClient = null;
let internalWsClient = null;
let lastSentSignal = 'Neutral';

let state = {
    bestBidPrice: 0, bestBidQty: 0, bestAskPrice: 0, bestAskQty: 0,
    imbalanceHistory: [],
    imbalancePercent: 0,
    imbalanceMomentum: 0,
    aggressiveTradeScore: 0,
    orderDisappearanceRate: 0,
    emaAggression: 0,
    emaAbsTradeSize: 0,
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

// --- Predictive Score Calculation ---
function updateImbalanceAndMomentum() {
    const { bestBidQty, bestAskQty } = state;
    const totalQty = bestBidQty + bestAskQty;
    state.imbalancePercent = totalQty > 0 ? (bestBidQty - bestAskQty) / totalQty : 0;
    state.imbalanceHistory.push(state.imbalancePercent);
    if (state.imbalanceHistory.length > IMBALANCE_HISTORY_SIZE) {
        state.imbalanceHistory.shift();
    }
    if (state.imbalanceHistory.length >= IMBALANCE_LONG_WINDOW) {
        const short_slice = state.imbalanceHistory.slice(-IMBALANCE_SHORT_WINDOW);
        const long_slice = state.imbalanceHistory;
        const short_avg = short_slice.reduce((a, b) => a + b, 0) / short_slice.length;
        const long_avg = long_slice.reduce((a, b) => a + b, 0) / long_slice.length;
        state.imbalanceMomentum = short_avg - long_avg;
    }
}

function processNewTrade(trade) {
    const { bestBidPrice, bestAskPrice } = state;
    const tradePrice = parseFloat(trade.p);
    const tradeQty = parseFloat(trade.q);
    const alpha = 2 / (TRADE_EMA_PERIOD + 1);

    let tradeAggression = 0;
    if (bestBidPrice > 0 && tradePrice <= bestBidPrice) tradeAggression = -tradeQty;
    else if (bestAskPrice > 0 && tradePrice >= bestAskPrice) tradeAggression = +tradeQty;
    else return;

    state.emaAggression = (tradeAggression * alpha) + (state.emaAggression * (1 - alpha));
    state.emaAbsTradeSize = (tradeQty * alpha) + (state.emaAbsTradeSize * (1 - alpha));

    if (state.emaAbsTradeSize > 0) {
        state.aggressiveTradeScore = state.emaAggression / state.emaAbsTradeSize;
        state.aggressiveTradeScore = Math.max(-1, Math.min(1, state.aggressiveTradeScore));
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

function calculateAndSendPredictScore() {
    const { imbalancePercent, imbalanceMomentum, aggressiveTradeScore, orderDisappearanceRate } = state;
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
                momentum: parseFloat(imbalanceMomentum.toFixed(4)),
                aggression: parseFloat(aggressiveTradeScore.toFixed(4)),
                disappearance: parseFloat(orderDisappearanceRate.toFixed(4))
            },
            timestamp: Date.now()
        };
        sendToInternalClient(payload);
        lastSentSignal = newSignal;
        console.log(`[Predictor] SIGNAL CHANGE: ${newSignal.padEnd(12)}| Score: ${payload.score.toFixed(4)} | Momentum: ${payload.components.momentum.toFixed(4)}`);
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


// --- Single Binance Stream Connection ---
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
                
                updateImbalanceAndMomentum();
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
console.log(`[Predictor] Starting up Advanced Model for symbol: ${SYMBOL}...`);
connectToInternalReceiver();
connectToBinanceStreams();
// --- END OF FILE predictive_listener_advanced.js ---
