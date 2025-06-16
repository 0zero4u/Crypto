// --- START OF FILE binance_listener.js ---

const WebSocket = require('ws');

// --- Global Error Handlers (Critical) ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[Predictor] [ERROR] [System] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`);
    console.error(err.stack || err);
    console.error(`[Predictor] [ERROR] [System] Exception origin: ${origin}`);
    cleanupAndExit(1);
});

process.on('unhandledRejection', (reason, promise) => {
    console.error(`[Predictor] [ERROR] [System] PID: ${process.pid} --- FATAL: UNHANDLED PROMISE REJECTION`);
    console.error('[Predictor] [ERROR] [System] Unhandled Rejection at:', promise);
    console.error('[Predictor] [ERROR] [System] Reason:', reason instanceof Error ? reason.stack : reason);
    cleanupAndExit(1);
});

function cleanupAndExit(exitCode = 1) {
    const clientsToTerminate = [internalWsClient, binanceWsClient];
    clientsToTerminate.forEach(client => {
        if (client && typeof client.terminate === 'function') {
            try { client.terminate(); } catch (e) { /* Silenced */ }
        }
    });
    setTimeout(() => {
        process.exit(exitCode);
    }, 1000).unref();
}

// --- Configuration ---
const SYMBOL = 'BTCUSDT';
const internalReceiverUrl = 'ws://localhost:8082';
const RECONNECT_INTERVAL_MS = 5000;

// --- DYNAMIC Liquidity Delta Model Parameters ---
const DELTA_MA_PERIOD = 100; // The number of recent deltas to average.
const THRESHOLD_MULTIPLIER = 3.0; // Trigger signal if delta is 3x the average delta. Tune this!
const STREAM_NAME = `${SYMBOL.toLowerCase()}@bookTicker`;
const BINANCE_STREAM_URL = `wss://stream.binance.com:9443/ws/${STREAM_NAME}`;

// --- State Variables ---
let binanceWsClient = null;
let internalWsClient = null;
let lastSentSignal = 'NEUTRAL';

let state = {
    bestBidPrice: 0, bestBidQty: 0, bestAskPrice: 0, bestAskQty: 0,
    lastBestBidPrice: 0,
    lastBestBidQty: 0, lastBestAskQty: 0,
    recentDeltas: [], // Stores recent absolute delta values for the moving average
};

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) return;
    internalWsClient = new WebSocket(internalReceiverUrl);
    internalWsClient.on('error', () => { /* Silenced */ });
    internalWsClient.on('close', () => {
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
}

// --- DYNAMIC DELTA IMBALANCE MODEL ---
function calculateDynamicDeltaAndSendSignal() {
    const { bestBidQty, bestAskQty, lastBestBidQty, lastBestAskQty, recentDeltas } = state;

    if (lastBestBidQty === 0 && lastBestAskQty === 0) return; // Cannot calculate delta on first run.

    const deltaBidQty = bestBidQty - lastBestBidQty;
    const deltaAskQty = bestAskQty - lastBestAskQty;
    const liquidityDelta = deltaBidQty - deltaAskQty;

    // Update our list of recent deltas for the moving average
    recentDeltas.push(Math.abs(liquidityDelta));
    if (recentDeltas.length > DELTA_MA_PERIOD) {
        recentDeltas.shift(); // Keep the list at a fixed size
    }

    // Wait until we have enough data to form a meaningful average
    if (recentDeltas.length < DELTA_MA_PERIOD / 2) {
        return;
    }

    // Calculate the dynamic threshold
    const avgDelta = recentDeltas.reduce((a, b) => a + b, 0) / recentDeltas.length;
    const dynamicDeltaThreshold = avgDelta * THRESHOLD_MULTIPLIER;

    // A delta of 0 should never trigger a signal, even if the average is tiny
    if (dynamicDeltaThreshold === 0) return;
    
    const score = liquidityDelta / dynamicDeltaThreshold;
    
    let newSignal = lastSentSignal;

    if (score >= 1.0) {
        newSignal = 'BUY';
    } else if (score <= -1.0) {
        newSignal = 'SELL';
    } else if (lastSentSignal !== 'NEUTRAL') {
        newSignal = 'NEUTRAL';
    }

    if (newSignal !== lastSentSignal) {
        const payload = {
            sig: newSignal,
            score: parseFloat(score.toFixed(4)),
            delta: parseFloat(liquidityDelta.toFixed(4)),
            dyn_thresh: parseFloat(dynamicDeltaThreshold.toFixed(4)), // Send the dynamic threshold for context
        };
        sendToInternalClient(payload);
        lastSentSignal = newSignal;
    }
}

function sendToInternalClient(payload) {
    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        const payloadStr = JSON.stringify(payload);
        try {
            internalWsClient.send(payloadStr);
        } catch (sendError) {
            console.error(`[Predictor] [ERROR] [Internal WS] FAILED to send payload. Error: ${sendError.message}`);
        }
    }
}

// --- Single Binance Stream Connection ---
function connectToBinanceStream() {
    if (binanceWsClient && (binanceWsClient.readyState === WebSocket.OPEN || binanceWsClient.readyState === WebSocket.CONNECTING)) return;
    binanceWsClient = new WebSocket(BINANCE_STREAM_URL);

    binanceWsClient.on('message', (data) => {
        try {
            const eventData = JSON.parse(data.toString());
            if (eventData.result === null && eventData.id !== undefined) return;

            if (eventData.b && eventData.a) {
                const newBestBidPrice = parseFloat(eventData.b);
                if (newBestBidPrice !== state.lastBestBidPrice && state.lastBestBidPrice !== 0) {
                    sendToInternalClient({ "Tick V": newBestBidPrice });
                }

                state.bestBidPrice = newBestBidPrice;
                state.bestBidQty = parseFloat(eventData.B);
                state.bestAskPrice = parseFloat(eventData.a);
                state.bestAskQty = parseFloat(eventData.A);
                
                calculateDynamicDeltaAndSendSignal();

                state.lastBestBidPrice = state.bestBidPrice;
                state.lastBestBidQty = state.bestBidQty;
                state.lastBestAskQty = state.bestAskQty;
            }
        } catch (e) {
            console.error(`[Predictor] [ERROR] [Binance WS] CRITICAL ERROR processing message: ${e.message}`, e.stack);
            console.error(`[Predictor] [ERROR] [Binance WS] Raw Data: ${data.toString()}`);
        }
    });

    binanceWsClient.on('error', () => { /* Silenced */ });
    binanceWsClient.on('close', () => {
        binanceWsClient = null;
        setTimeout(connectToBinanceStream, RECONNECT_INTERVAL_MS);
    });
}

// --- Start the application ---
connectToInternalReceiver();
connectToBinanceStream();