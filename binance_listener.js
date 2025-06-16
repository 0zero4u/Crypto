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

const STREAM_NAME = `${SYMBOL.toLowerCase()}@bookTicker`;
const BINANCE_STREAM_URL = `wss://stream.binance.com:9443/ws/${STREAM_NAME}`;

// --- BBO-Only Predictive Model Parameters ---
const SIGNAL_THRESHOLD = 0.003;
const HYSTERESIS_BUFFER = 0.0002;

// --- State Variables ---
let binanceWsClient = null;
let internalWsClient = null;
let lastSentSignal = 'NEUTRAL';

let state = {
    bestBidPrice: 0, bestBidQty: 0, bestAskPrice: 0, bestAskQty: 0,
    lastBestBidPrice: 0,
};

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) return;
    internalWsClient = new WebSocket(internalReceiverUrl);

    internalWsClient.on('error', () => { /* Silenced connection errors */ });
    internalWsClient.on('close', () => {
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
}

// --- BBO-Only Predictive Model Calculation ---
function calculateAndSendSignal() {
    const { bestBidPrice, bestBidQty, bestAskPrice, bestAskQty } = state;

    const totalVolume = bestBidQty + bestAskQty;
    if (totalVolume === 0 || bestBidPrice <= 0 || bestAskPrice <= 0) {
        return;
    }

    const midPrice = (bestBidPrice + bestAskPrice) / 2;
    const efficientPrice = ((bestBidPrice * bestAskQty) + (bestAskPrice * bestBidQty)) / totalVolume;
    const signalValue = efficientPrice - midPrice; // This is Y_est

    // --- SCORE SYSTEM (UNCAPPED VERSION) ---
    // The raw, uncapped score representing the magnitude of the imbalance.
    const score = signalValue / SIGNAL_THRESHOLD;

    let newSignal = lastSentSignal;

    if (lastSentSignal === 'NEUTRAL') {
        if (score >= 1.0) newSignal = 'BUY'; // Use score for comparison
        else if (score <= -1.0) newSignal = 'SELL';
    } else if (lastSentSignal === 'BUY') {
        // Hysteresis is now relative to the score
        if (score < (1.0 - (HYSTERESIS_BUFFER / SIGNAL_THRESHOLD))) newSignal = 'NEUTRAL';
    } else if (lastSentSignal === 'SELL') {
        if (score > (-1.0 + (HYSTERESIS_BUFFER / SIGNAL_THRESHOLD))) newSignal = 'NEUTRAL';
    }

    if (newSignal !== lastSentSignal) {
        const payload = {
            sig: newSignal,
            score: parseFloat(score.toFixed(4)), // The new UNCAPPED score
            y_est: parseFloat(signalValue.toFixed(6)),
            s_est: parseFloat(efficientPrice.toFixed(4)),
            p_mid: parseFloat(midPrice.toFixed(4)),
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

            if (eventData.result === null && eventData.id !== undefined) {
                return; // Ignore confirmation message.
            }

            if (eventData.b && eventData.a) {
                const newBestBidPrice = parseFloat(eventData.b);
                
                if (newBestBidPrice !== state.lastBestBidPrice && state.lastBestBidPrice !== 0) {
                    const tickPayload = { "Tick V": newBestBidPrice };
                    sendToInternalClient(tickPayload);
                }

                state.bestBidPrice = newBestBidPrice;
                state.bestBidQty = parseFloat(eventData.B);
                state.bestAskPrice = parseFloat(eventData.a);
                state.bestAskQty = parseFloat(eventData.A);
                state.lastBestBidPrice = newBestBidPrice;

                calculateAndSendSignal();
            }
        } catch (e) {
            console.error(`[Predictor] [ERROR] [Binance WS] CRITICAL ERROR processing message: ${e.message}`, e.stack);
            console.error(`[Predictor] [ERROR] [Binance WS] Raw Data: ${data.toString()}`);
        }
    });

    binanceWsClient.on('error', () => { /* Silenced connection errors */ });
    binanceWsClient.on('close', () => {
        binanceWsClient = null;
        setTimeout(connectToBinanceStream, RECONNECT_INTERVAL_MS);
    });
}

// --- Start the application ---
connectToInternalReceiver();
connectToBinanceStream();
