// --- START OF FILE predictive_listener_robust_v5-tuned-fast.js ---

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
const WEIGHT_IMBALANCE = 0.40;
const WEIGHT_PARTICIPATION = 0.20;
const WEIGHT_VWAP_PROGRESSION = 0.30;
const WEIGHT_IMBALANCE_MOMENTUM = 0.10;

const IMBALANCE_SHORT_WINDOW = 5;
const IMBALANCE_LONG_WINDOW = 15;
const IMBALANCE_HISTORY_SIZE = IMBALANCE_LONG_WINDOW;

// Parameters tuned for HYPER responsiveness based on log data
const URGENCY_TRADE_HISTORY_SIZE = 6; // Look at the last 4 trades for participation
const VWAP_CALCULATION_WINDOW = 6;    // Calculate VWAP over the last 4 trades for progression

const MIN_IMBALANCE_FOR_URGENCY = 0.7;

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
    recentTradeDirections: [],
    recentTradesForVWAP: [],
    participationScore: 0,
    progressionScore: 0,
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

function processTrade(trade) {
    const { imbalancePercent } = state;
    const tradePrice = parseFloat(trade.p);
    const tradeQty = parseFloat(trade.q);
    const tradeDirection = trade.m ? -1 : 1;

    // --- 1. Update Histories ---
    state.recentTradeDirections.push(tradeDirection);
    if (state.recentTradeDirections.length > URGENCY_TRADE_HISTORY_SIZE) {
        state.recentTradeDirections.shift();
    }
    state.recentTradesForVWAP.push({ price: tradePrice, qty: tradeQty });
    if (state.recentTradesForVWAP.length > VWAP_CALCULATION_WINDOW) {
        state.recentTradesForVWAP.shift();
    }

    // --- 2. Calculate Participation Score (Weak Urgency) ---
    if (Math.abs(imbalancePercent) < MIN_IMBALANCE_FOR_URGENCY) {
        state.participationScore = 0;
    } else {
        const imbalanceDirection = Math.sign(imbalancePercent);
        const supportingTrades = state.recentTradeDirections.filter(dir => dir === imbalanceDirection).length;
        const totalTrades = state.recentTradeDirections.length;
        state.participationScore = totalTrades > 0 ? (supportingTrades / totalTrades) * imbalanceDirection : 0;
    }

    // --- 3. Calculate VWAP-based Progression Score (Strong Urgency) ---
    if (state.recentTradesForVWAP.length < VWAP_CALCULATION_WINDOW || Math.abs(imbalancePercent) < MIN_IMBALANCE_FOR_URGENCY) {
        state.progressionScore *= 0.75;
        return;
    }

    let totalVolume = 0;
    let totalValue = 0;
    for (const t of state.recentTradesForVWAP) {
        totalValue += t.price * t.qty;
        totalVolume += t.qty;
    }
    const vwap = totalVolume > 0 ? totalValue / totalVolume : tradePrice;
    const spread = state.bestAskPrice - state.bestBidPrice;
    const deviation = tradePrice - vwap;

    let rawProgression = spread > 0 ? deviation / (2 * spread) : 0;
    rawProgression *= tradeDirection;

    const alpha = 0.3;
    state.progressionScore = (rawProgression * alpha) + (state.progressionScore * (1 - alpha));
    state.progressionScore = Math.max(-1, Math.min(1, state.progressionScore));
}

function calculateAndSendPredictScore() {
    const { imbalancePercent, participationScore, progressionScore, imbalanceMomentum } = state;

    const score = (WEIGHT_IMBALANCE * imbalancePercent) +
                  (WEIGHT_PARTICIPATION * participationScore) +
                  (WEIGHT_VWAP_PROGRESSION * progressionScore) +
                  (WEIGHT_IMBALANCE_MOMENTUM * imbalanceMomentum);

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
                participation: parseFloat(participationScore.toFixed(4)),
                progression: parseFloat(progressionScore.toFixed(4)),
                momentum: parseFloat(imbalanceMomentum.toFixed(4)),
            },
            timestamp: Date.now()
        };
        sendToInternalClient(payload);
        lastSentSignal = newSignal;
        console.log(`[Predictor] SIGNAL CHANGE: ${newSignal.padEnd(12)}| Score: ${payload.score.toFixed(4)} | Participation: ${payload.components.participation.toFixed(4)} | Progression: ${payload.components.progression.toFixed(4)}`);
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
                state.bestBidPrice = parseFloat(eventData.b);
                state.bestBidQty = parseFloat(eventData.B);
                state.bestAskPrice = parseFloat(eventData.a);
                state.bestAskQty = parseFloat(eventData.A);

                updateImbalanceAndMomentum();

            } else if (streamName === STREAM_NAMES[1]) { // trade
                if (eventData.m !== undefined) {
                    processTrade(eventData);
                    calculateAndSendPredictScore();
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
console.log(`[Predictor] Starting up ROBUST Model V5-Tuned-FAST (4-trade window) for symbol: ${SYMBOL}...`);
connectToInternalReceiver();
connectToBinanceStreams();

// --- END OF FILE predictive_listener_robust_v5-tuned-fast.js ---
