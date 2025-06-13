// --- START OF FILE binance_listener.js ---

const WebSocket = require('ws');

// --- Global Error Handlers (unchanged) ---
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
    const clientsToTerminate = [internalWsClient, binanceWsClient];
    clientsToTerminate.forEach(client => {
        if (client && typeof client.terminate === 'function') {
            try { client.terminate(); } catch (e) { console.error(`[Predictor] Error terminating a WebSocket client: ${e.message}`); }
        }
    });
    setTimeout(() => { process.exit(exitCode); }, 1000).unref();
}

// --- Predictor Configuration ---
const SYMBOL = 'BTCUSDT';
const BINANCE_STREAM_URL = `wss://fstream.binance.com/stream?streams=${SYMBOL.toLowerCase()}@depth5@0ms/${SYMBOL.toLowerCase()}@trade`;
const internalReceiverUrl = 'ws://localhost:8082';
const RECONNECT_INTERVAL_MS = 5000;

// --- Formula Weights, Normalizers & Filtering Parameters ---
const MODEL_VERSION = "2.3-filtered";
const WINDOW_SIZES = [5, 20];
const SPREAD_NORMALIZER = 0.10;
const BASELINE_WINDOW_SECONDS = 300;
const FREQ_MULTIPLIER = 3.0;
const FREQ_FLOOR_TPS = 15.0;
const SCORE_CHANGE_THRESHOLD = 0.5; // Send prediction if score changes by this much

const WEIGHTS = {
    wmp_mid_diff: 3.0,
    spread_normalized: -1.0,
    tvi_5: 1.0,
    delta_tvi_5: 2.5,
    freq_norm_5: 0.5,
    tvi_20: 1.0,
    delta_tvi_20: 1.0,
    freq_norm_20: 0.3
};

// --- Predictor State Variables ---
let binanceWsClient = null;
let internalWsClient = null;

let lastBookFeatures = { wmp_mid_diff: 0, spread_normalized: 0, source_timestamp: 0 };
let lastSentBidPrice = 0;
let lastSentPrediction = { score: 0, bias: 'NEUTRAL' }; // State for score filtering

let tviState = {};
let tradeFreqState = {};
let secondBuckets = new Array(BASELINE_WINDOW_SECONDS).fill(0);
let totalTrades5Min = 0;
let currentSecondTimestamp = 0;
let currentBucketIndex = 0;

function initializeState() {
    WINDOW_SIZES.forEach(size => {
        tviState[size] = { queue: [], buyVol: 0, sellVol: 0, lastTVI: 0 };
        tradeFreqState[size] = { queue: [] };
    });
    secondBuckets.fill(0);
    totalTrades5Min = 0;
    currentSecondTimestamp = Math.floor(Date.now() / 1000);
    currentBucketIndex = 0;
    lastSentBidPrice = 0;
    lastSentPrediction = { score: 0, bias: 'NEUTRAL' };
}

function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) return;
    internalWsClient = new WebSocket(internalReceiverUrl);
    internalWsClient.on('error', (err) => console.error(`[Predictor] Internal receiver WebSocket error: ${err.message}`));
    internalWsClient.on('close', () => {
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
}

// --- Feature Calculation Functions ---
function processDepthUpdate(data) {
    if (!data.b || !data.a || data.b.length === 0 || data.a.length === 0) return;
    try {
        const bestBidPrice = parseFloat(data.b[0][0]);
        
        // *** REVERTED LOGIC: Send price tick if it has changed, for maximum speed. ***
        if (bestBidPrice !== lastSentBidPrice) {
            sendToInternalClient({ p: bestBidPrice });
            lastSentBidPrice = bestBidPrice;
        }
        
        // Feature calculation for the predictor continues silently in the background
        const bestBidQty = parseFloat(data.b[0][1]);
        const bestAskPrice = parseFloat(data.a[0][0]);
        const bestAskQty = parseFloat(data.a[0][1]);
        if (bestBidQty === 0 || bestAskQty === 0) return;
        const midPrice = (bestBidPrice + bestAskPrice) / 2;
        const wmp = (bestBidPrice * bestAskQty + bestAskPrice * bestBidQty) / (bestBidQty + bestAskQty);
        const spread = bestAskPrice - bestBidPrice;
        lastBookFeatures.wmp_mid_diff = wmp - midPrice;
        lastBookFeatures.spread_normalized = spread / SPREAD_NORMALIZER;
        lastBookFeatures.source_timestamp = data.E;
    } catch (e) {
        console.error(`[Predictor] Error processing depth update: ${e.message}`);
    }
}

// --- updateTVI, updateTradeFreq, updateBaseline functions are unchanged ---
function updateTVI(trade, windowSize) { /* ... unchanged ... */ }
function updateTradeFreq(tradeTime, windowSize) { /* ... unchanged ... */ }
function updateBaseline(tradeTime) { /* ... unchanged ... */ }
// ... (Including full function bodies from previous version for completeness)
function updateTVI(trade, windowSize) {
    const state = tviState[windowSize];
    let removedTrade = null;
    if (state.queue.length >= windowSize) {
        removedTrade = state.queue.shift();
    }
    state.queue.push(trade);
    if (trade.m) { state.sellVol += trade.q; } else { state.buyVol += trade.q; }
    if (removedTrade) {
        if (removedTrade.m) { state.sellVol -= removedTrade.q; } else { state.buyVol -= removedTrade.q; }
    }
    const totalVol = state.buyVol + state.sellVol;
    const currentTVI = totalVol > 0 ? (state.buyVol - state.sellVol) / totalVol : 0;
    const deltaTVI = currentTVI - state.lastTVI;
    state.lastTVI = currentTVI;
    return { tvi: currentTVI, delta_tvi: deltaTVI };
}
function updateTradeFreq(tradeTime, windowSize) {
    const state = tradeFreqState[windowSize];
    state.queue.push(tradeTime);
    if (state.queue.length > windowSize) {
        state.queue.shift();
    }
    if (state.queue.length < windowSize) return 0;
    const timeElapsed = (state.queue[windowSize - 1] - state.queue[0]) / 1000;
    return timeElapsed > 0 ? windowSize / timeElapsed : 0;
}
function updateBaseline(tradeTime) {
    const tradeSecond = Math.floor(tradeTime / 1000);
    if (tradeSecond > currentSecondTimestamp) {
        const secondsElapsed = Math.min(tradeSecond - currentSecondTimestamp, BASELINE_WINDOW_SECONDS);
        for (let i = 0; i < secondsElapsed; i++) {
            currentBucketIndex = (currentBucketIndex + 1) % BASELINE_WINDOW_SECONDS;
            totalTrades5Min -= secondBuckets[currentBucketIndex];
            secondBuckets[currentBucketIndex] = 0;
        }
        currentSecondTimestamp = tradeSecond;
    }
    secondBuckets[currentBucketIndex]++;
    totalTrades5Min++;
}


function getBiasFromScore(score) {
    if (score > 2.5) return "STRONG_BUY_BIAS";
    if (score > 1.0) return "WEAK_BUY_BIAS";
    if (score < -2.5) return "STRONG_SELL_BIAS";
    if (score < -1.0) return "WEAK_SELL_BIAS";
    return "NEUTRAL";
}

function processTrade(data) {
    try {
        const trade = { q: parseFloat(data.q), m: data.m, T: data.T };
        updateBaseline(trade.T);
        const baselineFreq = totalTrades5Min / BASELINE_WINDOW_SECONDS;
        const flowFeatures = {};
        for (const size of WINDOW_SIZES) {
            const { tvi, delta_tvi } = updateTVI(trade, size);
            const tradeFreq = updateTradeFreq(trade.T, size);
            const fastMarketSpeed = Math.max(baselineFreq * FREQ_MULTIPLIER, FREQ_FLOOR_TPS);
            const freq_norm = fastMarketSpeed > 0 ? Math.min(tradeFreq, fastMarketSpeed) / fastMarketSpeed : 0;
            flowFeatures[size] = { tvi, delta_tvi, freq_norm };
        }
        const score = 
            (WEIGHTS.wmp_mid_diff * lastBookFeatures.wmp_mid_diff) +
            (WEIGHTS.spread_normalized * lastBookFeatures.spread_normalized) +
            (WEIGHTS.tvi_5 * flowFeatures[5].tvi) +
            (WEIGHTS.delta_tvi_5 * flowFeatures[5].delta_tvi) +
            (WEIGHTS.freq_norm_5 * flowFeatures[5].freq_norm) +
            (WEIGHTS.tvi_20 * flowFeatures[20].tvi) +
            (WEIGHTS.delta_tvi_20 * flowFeatures[20].delta_tvi) +
            (WEIGHTS.freq_norm_20 * flowFeatures[20].freq_norm);

        // *** NEW FILTERING LOGIC FOR PREDICTION SCORES ***
        const currentBias = getBiasFromScore(score);
        const scoreChangedSignificantly = Math.abs(score - lastSentPrediction.score) > SCORE_CHANGE_THRESHOLD;
        const biasChanged = currentBias !== lastSentPrediction.bias;

        if (scoreChangedSignificantly || biasChanged) {
            const finalPayload = createFinalPayload(data, score, currentBias, flowFeatures, baselineFreq);
            sendToInternalClient(finalPayload);
            
            // Update the state for the next comparison
            lastSentPrediction.score = score;
            lastSentPrediction.bias = currentBias;
        }
        
    } catch (e) {
        console.error(`[Predictor] CRITICAL ERROR in trade processor: ${e.message}`, e.stack);
    }
}

function createFinalPayload(tradeData, score, bias, flowFeatures, baselineFreq) {
    return {
        type: "prediction_score", 
        timestamp_utc: new Date(tradeData.E).toISOString(),
        event_timestamp_exchange: tradeData.E,
        model_version: MODEL_VERSION,
        prediction: {
            score: parseFloat(score.toFixed(4)),
            bias: bias,
        },
        features: {
            liquidity_pressure: {
                source_timestamp: lastBookFeatures.source_timestamp,
                wmp_mid_diff: parseFloat(lastBookFeatures.wmp_mid_diff.toFixed(4)),
                spread_normalized: parseFloat(lastBookFeatures.spread_normalized.toFixed(4)),
            },
            trade_flow_short_term: {
                window_size: 5,
                tvi: parseFloat(flowFeatures[5].tvi.toFixed(4)),
                delta_tvi: parseFloat(flowFeatures[5].delta_tvi.toFixed(4)),
                frequency_normalized: parseFloat(flowFeatures[5].freq_norm.toFixed(4)),
            },
            trade_flow_medium_term: {
                window_size: 20,
                tvi: parseFloat(flowFeatures[20].tvi.toFixed(4)),
                delta_tvi: parseFloat(flowFeatures[20].delta_tvi.toFixed(4)),
                frequency_normalized: parseFloat(flowFeatures[20].freq_norm.toFixed(4)),
            },
            market_context: {
                baseline_freq_5min: parseFloat(baselineFreq.toFixed(2)),
            }
        }
    };
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

function connectToBinanceStream() {
    if (binanceWsClient && (binanceWsClient.readyState === WebSocket.OPEN || binanceWsClient.readyState === WebSocket.CONNECTING)) return;
    initializeState();
    binanceWsClient = new WebSocket(BINANCE_STREAM_URL);
    binanceWsClient.on('open', () => console.log(`[Predictor] Connected to Binance stream: ${BINANCE_STREAM_URL}`));
    binanceWsClient.on('message', function incoming(data) {
        try {
            const message = JSON.parse(data.toString());
            if (message.stream.includes('@depth5')) {
                processDepthUpdate(message.data);
            } else if (message.stream.includes('@trade')) {
                processTrade(message.data);
            }
        } catch (e) {
            console.error(`[Predictor] CRITICAL ERROR in message handler: ${e.message}`, e.stack);
        }
    });
    binanceWsClient.on('error', (err) => console.error(`[Predictor] Binance WebSocket error: ${err.message}`));
    binanceWsClient.on('close', () => {
        console.log('[Predictor] Binance connection closed. Reconnecting...');
        binanceWsClient = null;
        setTimeout(connectToBinanceStream, RECONNECT_INTERVAL_MS);
    });
}

// --- Start the connections ---
connectToInternalReceiver();
connectToBinanceStream();

console.log(`[Predictor] PID: ${process.pid} --- Predictive Indicator Started for ${SYMBOL}`);

// --- END OF FILE binance_listener.js ---