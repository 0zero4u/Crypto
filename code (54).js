// --- START OF FILE binance_listener (17).js ---

// --- START OF FILE binance_listener.js ---

const WebSocket = require('ws');

// --- Global Error Handlers (Critical Logs Only) ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`);
    console.error(err.stack || err);
    console.error(`[Listener] Exception origin: ${origin}`);
    console.error(`[Listener] PID: ${process.pid} --- Exiting due to uncaught exception...`);
    cleanupAndExit(1);
});

process.on('unhandledRejection', (reason, promise) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNHANDLED PROMISE REJECTION`);
    console.error('[Listener] Unhandled Rejection at:', promise);
    console.error('[Listener] Reason:', reason instanceof Error ? reason.stack : reason);
    console.error(`[Listener] PID: ${process.pid} --- Exiting due to unhandled promise rejection...`);
    cleanupAndExit(1);
});

function cleanupAndExit(exitCode = 1) {
    const clientsToTerminate = [binanceTradeClient, binanceBookTickerClient, internalWsClient];
    clientsToTerminate.forEach(client => {
        if (client && typeof client.terminate === 'function') {
            try { client.terminate(); } catch (e) { /* Silenced */ }
        }
    });

    setTimeout(() => {
        process.exit(exitCode);
    }, 1000).unref();
}

// --- Listener Configuration ---
const SYMBOL = 'BTCUSDT';
const BINANCE_TRADE_STREAM_URL = `wss://stream.binance.com:9443/ws/${SYMBOL.toLowerCase()}@trade`;
const BINANCE_BOOKTICKER_STREAM_URL = `wss://stream.binance.com:9443/ws/${SYMBOL.toLowerCase()}@bookTicker`;
const internalReceiverUrl = 'ws://localhost:8082';
const RECONNECT_INTERVAL_MS = 5000;
const BATCH_SIZE = 5;
const PRICE_CHANGE_THRESHOLD = 0.5;
const TICK_SIZE = 0.01;
const EPSILON = 1e-9;

// --- Predictive Score Configuration (with Spread Dynamics) ---
const WEIGHTS = {
    a_TradeImbalance: 1.0,
    b_DeltaImbalance: 2.0,
    c_TradeROC: 0.5,
    d_TradeFrequency: 1.0,
    e_TickUrgency: 0.4,
    f_AggressiveMomentum: 0.6, // Synergy between price push and volume
    g_SpreadDelta: 3.0,        // NEW: High weight for market maker retreat signal
};

// --- Score Thresholds (Adjusted for new powerful term) ---
const SCORE_THRESHOLDS = {
    STRONG_BUY: 4.0,
    WEAK_BUY: 2.0,
    WEAK_SELL: -2.0,
    STRONG_SELL: -4.0,
};


// --- Listener State Variables ---
let binanceTradeClient = null;
let binanceBookTickerClient = null;
let internalWsClient = null;
let tradeBatch = [];
let batchCounter = 0;
let lastSentPayloadPrice = null;
let previousBatchStats = { tradeImbalance: 0, buyVolume: 0 };
// State for spread dynamics
let currentSpread = 0;
let previousSpread = 0;


// --- Internal Receiver Connection (Silent) ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    internalWsClient = new WebSocket(internalReceiverUrl);
    internalWsClient.on('error', () => {});
    internalWsClient.on('close', () => {
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
}

// --- Data Extraction & Processing ---
function extractTradeData(messageString) { /* ... no changes ... */ }
function getActionFromScore(score) { /* ... no changes ... */ }

function processTradeBatch() {
    batchCounter++;

    let buyVolume = 0, sellVolume = 0;
    tradeBatch.forEach(trade => { trade.isBuy ? buyVolume += trade.volume : sellVolume += trade.volume; });

    const firstTrade = tradeBatch[0];
    const lastTrade = tradeBatch[BATCH_SIZE - 1];
    
    const lastTradePrice = lastTrade.price;
    const batchDurationMs = lastTrade.timestamp - firstTrade.timestamp;
    const timeDeltaSeconds = batchDurationMs / 1000;

    // --- Calculate Score Components ---
    const tradeImbalance = (buyVolume - sellVolume) / (buyVolume + sellVolume + EPSILON);
    const deltaImbalance = tradeImbalance - previousBatchStats.tradeImbalance;
    const tradeROC = ((buyVolume - previousBatchStats.buyVolume) / (previousBatchStats.buyVolume + EPSILON)) * 100;
    const tradeFrequency = BATCH_SIZE / (timeDeltaSeconds + EPSILON);
    const tickUrgency = (lastTrade.price - firstTrade.price) / TICK_SIZE;
    const aggressiveMomentum = tickUrgency * Math.abs(tradeImbalance);
    
    // NEW: Read spread state at the moment of calculation
    const spreadDelta = currentSpread - previousSpread;

    const predictScore = (WEIGHTS.a_TradeImbalance * tradeImbalance) +
                         (WEIGHTS.b_DeltaImbalance * deltaImbalance) +
                         (WEIGHTS.c_TradeROC * tradeROC) +
                         (WEIGHTS.d_TradeFrequency * tradeFrequency) +
                         (WEIGHTS.e_TickUrgency * tickUrgency) +
                         (WEIGHTS.f_AggressiveMomentum * aggressiveMomentum) +
                         (WEIGHTS.g_SpreadDelta * spreadDelta); // NEW: Spread dynamics added

    if (lastSentPayloadPrice === null || Math.abs(lastTradePrice - lastSentPayloadPrice) >= PRICE_CHANGE_THRESHOLD) {
        const payload = {
            p: lastTradePrice,
            batch: batchCounter,
            predict_score: parseFloat(predictScore.toFixed(4)),
            spread_delta: parseFloat(spreadDelta.toFixed(4)), // NEW: Added to payload
            aggressive_momentum: parseFloat(aggressiveMomentum.toFixed(2)),
            tick_urgency: parseFloat(tickUrgency.toFixed(2)),
            trade_imbalance: parseFloat(tradeImbalance.toFixed(4)),
            delta_imbalance: parseFloat(deltaImbalance.toFixed(4)),
            trade_roc: parseFloat(tradeROC.toFixed(2)),
            trade_frequency: parseFloat(tradeFrequency.toFixed(2)),
            batch_duration_ms: batchDurationMs,
            action: getActionFromScore(predictScore)
        };
        sendToInternalClient(payload);
        lastSentPayloadPrice = lastTradePrice;
    }

    previousBatchStats = { tradeImbalance: tradeImbalance, buyVolume: buyVolume };
    tradeBatch = [];
}

function sendToInternalClient(payload) { /* ... no changes ... */ }

// --- Binance Stream Connections ---
function connectToTradeStream() {
    if (binanceTradeClient && (binanceTradeClient.readyState === WebSocket.OPEN || binanceTradeClient.readyState === WebSocket.CONNECTING)) return;
    binanceTradeClient = new WebSocket(BINANCE_TRADE_STREAM_URL);

    binanceTradeClient.on('open', () => {
        tradeBatch = [];
        batchCounter = 0;
        lastSentPayloadPrice = null;
        previousBatchStats = { tradeImbalance: 0, buyVolume: 0 };
    });

    binanceTradeClient.on('message', (data) => {
        try {
            const messageString = data.toString();
            const trade = extractTradeData(messageString);
            if (trade) {
                tradeBatch.push(trade);
                if (tradeBatch.length === BATCH_SIZE) {
                    processTradeBatch();
                }
            }
        } catch (e) {
            console.error(`[Listener] CRITICAL ERROR in trade handler: ${e.message}`, e.stack);
        }
    });

    binanceTradeClient.on('error', () => {});
    binanceTradeClient.on('close', () => {
        binanceTradeClient = null;
        setTimeout(connectToTradeStream, RECONNECT_INTERVAL_MS);
    });
}

function connectToBookTickerStream() {
    if (binanceBookTickerClient && (binanceBookTickerClient.readyState === WebSocket.OPEN || binanceBookTickerClient.readyState === WebSocket.CONNECTING)) return;
    binanceBookTickerClient = new WebSocket(BINANCE_BOOKTICKER_STREAM_URL);

    binanceBookTickerClient.on('open', () => {
        currentSpread = 0;
        previousSpread = 0;
    });

    binanceBookTickerClient.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());
            if (message.a && message.b) {
                const askPrice = parseFloat(message.a);
                const bidPrice = parseFloat(message.b);
                const newSpread = askPrice - bidPrice;
                
                // Update state for the next trade batch calculation
                previousSpread = currentSpread;
                currentSpread = newSpread;
            }
        } catch (e) {
            console.error(`[Listener] CRITICAL ERROR in bookTicker handler: ${e.message}`, e.stack);
        }
    });

    binanceBookTickerClient.on('error', () => {});
    binanceBookTickerClient.on('close', () => {
        binanceBookTickerClient = null;
        setTimeout(connectToBookTickerStream, RECONNECT_INTERVAL_MS);
    });
}

// Helper functions that were unchanged but are required for completeness
function extractTradeData(messageString) {
    try {
        const message = JSON.parse(messageString);
        if (message.t && message.p && message.q && typeof message.m !== 'undefined') {
            return { price: parseFloat(message.p), volume: parseFloat(message.q), isBuy: !message.m, timestamp: message.T, };
        }
        return null;
    } catch (error) {
        console.error(`[Listener] CRITICAL: Error parsing Binance JSON: ${error.message}. Data: ${messageString.substring(0,100)}...`);
        return null;
    }
}
function getActionFromScore(score) {
    if (score > SCORE_THRESHOLDS.STRONG_BUY) return 'STRONG BUY BIAS';
    if (score > SCORE_THRESHOLDS.WEAK_BUY) return 'WEAK BUY BIAS';
    if (score < SCORE_THRESHOLDS.STRONG_SELL) return 'STRONG SELL BIAS';
    if (score < SCORE_THRESHOLDS.WEAK_SELL) return 'WEAK SELL BIAS';
    return 'NEUTRAL';
}
function sendToInternalClient(payload) {
    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        try {
            internalWsClient.send(JSON.stringify(payload));
        } catch (sendError) { /* Silenced */ }
    }
}


// --- Start all connections ---
connectToInternalReceiver();
connectToTradeStream();
connectToBookTickerStream();

// --- No startup log for silent operation ---