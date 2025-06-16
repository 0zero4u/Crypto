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

// --- Proportional Delta Model Parameters ---
const PROPORTIONAL_THRESHOLD = 0.25; // Trigger if delta is >25% of BBO depth. Tune this!
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

// --- PROPORTIONAL DELTA IMBALANCE MODEL ---
function calculateProportionalDeltaSignal() {
    const { bestBidQty, bestAskQty, lastBestBidQty, lastBestAskQty } = state;

    if (lastBestBidQty === 0 && lastBestAskQty === 0) return; // Cannot calculate on first run.

    const totalLastQty = lastBestBidQty + lastBestAskQty;
    // Prevent division by zero if the book was momentarily empty.
    if (totalLastQty === 0) return;

    const deltaBidQty = bestBidQty - lastBestBidQty;
    const deltaAskQty = bestAskQty - lastBestAskQty;
    const liquidityDelta = deltaBidQty - deltaAskQty;

    // The score is the delta as a proportion of the previous total BBO depth.
    const score = liquidityDelta / totalLastQty;
    
    let newSignal = lastSentSignal;

    if (score > PROPORTIONAL_THRESHOLD) {
        newSignal = 'BUY';
    } else if (score < -PROPORTIONAL_THRESHOLD) {
        newSignal = 'SELL';
    } else if (lastSentSignal !== 'NEUTRAL') {
        newSignal = 'NEUTRAL';
    }

    if (newSignal !== lastSentSignal) {
        const payload = {
            sig: newSignal,
            score: parseFloat(score.toFixed(4)), // Proportional score
            delta: parseFloat(liquidityDelta.toFixed(4)),
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

                // Update current state values before calculation
                state.bestBidPrice = newBestBidPrice;
                state.bestBidQty = parseFloat(eventData.B);
                state.bestAskPrice = parseFloat(eventData.a);
                state.bestAskQty = parseFloat(eventData.A);
                
                // Calculate signal based on the change from the PREVIOUS state.
                calculateProportionalDeltaSignal();

                // NOW, update the 'last' state for the NEXT calculation cycle.
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
