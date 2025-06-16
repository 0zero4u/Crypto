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
const SIGNAL_THRESHOLD = 0.001;
const HYSTERESIS_BUFFER = 0.0002;

// --- Dynamic Delta Imbalance Parameters ---
// These parameters control how the change in order book quantity influences the signal.
const MIN_DELTA_WEIGHT = 0.05;    // Baseline weight for delta score (e.g., 5%)
const MAX_DELTA_WEIGHT = 0.50;    // Max weight during significant events (e.g., 50%)
const DELTA_MIDPOINT = 2.0;       // The abs(deltaImbalance) considered "significant". For BTC, 2 BTC change is a good start.
const DELTA_CURVE_STEEPNESS = 1.5;// Controls how sharp the weight transition is. 1-2 is a good range.
const DELTA_SENSITIVITY_THRESHOLD = 1.0; // Normalization for the delta *score*. A value of 1.0 makes it straight-forward.

// --- State Variables ---
let binanceWsClient = null;
let internalWsClient = null;
let lastSentSignal = 'NEUTRAL';

let state = {
    bestBidPrice: 0, bestBidQty: 0, bestAskPrice: 0, bestAskQty: 0,
    lastBestBidPrice: 0,
    lastBestBidQty: 0,
    lastBestAskQty: 0,
};

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) return;
    internalWsClient = new WebSocket(internalReceiverUrl);

    internalWsClient.on('open', () => {
        console.log(`[Predictor] [INFO] [Internal WS] Connection established to ${internalReceiverUrl}`);
    });
    internalWsClient.on('error', () => { /* Silenced connection errors, handled by 'close' */ });
    internalWsClient.on('close', () => {
        console.log(`[Predictor] [WARN] [Internal WS] Connection lost. Reconnecting in ${RECONNECT_INTERVAL_MS / 1000}s...`);
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
}

// --- Helper function for Dynamic Weight Calculation ---
/**
 * Calculates a dynamic weight using a scaled sigmoid function.
 * This allows the model to give more importance to larger, more significant changes in quantity imbalance.
 * @param {number} deltaImbalance The raw change in quantity imbalance.
 * @returns {number} The calculated weight, between MIN_DELTA_WEIGHT and MAX_DELTA_WEIGHT.
 */
function calculateDynamicDeltaWeight(deltaImbalance) {
    const absDelta = Math.abs(deltaImbalance);
    
    // Sigmoid function: 1 / (1 + e^(-k * (x - x0)))
    // x = absDelta, x0 = DELTA_MIDPOINT, k = DELTA_CURVE_STEEPNESS
    const exponent = -DELTA_CURVE_STEEPNESS * (absDelta - DELTA_MIDPOINT);
    const sigmoid = 1 / (1 + Math.exp(exponent));

    // Scale the sigmoid output (0 to 1) to our desired weight range
    const dynamicWeight = MIN_DELTA_WEIGHT + (MAX_DELTA_WEIGHT - MIN_DELTA_WEIGHT) * sigmoid;
    
    return dynamicWeight;
}

// --- BBO-Only Predictive Model Calculation ---
function calculateAndSendSignal() {
    const { bestBidPrice, bestBidQty, bestAskPrice, bestAskQty, lastBestBidQty, lastBestAskQty } = state;

    const totalVolume = bestBidQty + bestAskQty;
    if (totalVolume === 0 || bestBidPrice <= 0 || bestAskPrice <= 0) {
        return; // Not enough data to calculate
    }

    // --- 1. Static Imbalance (Original Method) ---
    // Calculates imbalance based on the current snapshot of prices and quantities.
    const midPrice = (bestBidPrice + bestAskPrice) / 2;
    const efficientPrice = ((bestBidPrice * bestAskQty) + (bestAskPrice * bestBidQty)) / totalVolume;
    const signalValue = efficientPrice - midPrice;
    const staticScore = signalValue / SIGNAL_THRESHOLD;

    // --- 2. Delta Imbalance (Leading Indicator) ---
    // Calculates the change in quantity imbalance from the previous tick to the current one.
    let deltaImbalance = 0;
    if (lastBestBidQty > 0 || lastBestAskQty > 0) { // Ensure we have a previous state to compare
        const currentImbalance = bestBidQty - bestAskQty;
        const previousImbalance = lastBestBidQty - lastBestAskQty;
        deltaImbalance = currentImbalance - previousImbalance;
    }
    const deltaScore = deltaImbalance / DELTA_SENSITIVITY_THRESHOLD;

    // --- 3. Calculate Dynamic Weight and Combine Scores ---
    // The weight of the delta score is adjusted based on the magnitude of the imbalance change.
    const dynamicDeltaWeight = calculateDynamicDeltaWeight(deltaImbalance);
    const combinedScore = ((1 - dynamicDeltaWeight) * staticScore) + (dynamicDeltaWeight * deltaScore);

    // --- 4. Signal Generation with Hysteresis ---
    let newSignal = lastSentSignal;
    if (lastSentSignal === 'NEUTRAL') {
        if (combinedScore >= 1.0) newSignal = 'BUY';
        else if (combinedScore <= -1.0) newSignal = 'SELL';
    } else if (lastSentSignal === 'BUY') {
        if (combinedScore < (1.0 - (HYSTERESIS_BUFFER / SIGNAL_THRESHOLD))) newSignal = 'NEUTRAL';
    } else if (lastSentSignal === 'SELL') {
        if (combinedScore > (-1.0 + (HYSTERESIS_BUFFER / SIGNAL_THRESHOLD))) newSignal = 'NEUTRAL';
    }

    // --- 5. Send Signal if State Changes ---
    if (newSignal !== lastSentSignal) {
        const payload = {
            sig: newSignal,
            combined_score: parseFloat(combinedScore.toFixed(4)),
            dynamic_delta_weight: parseFloat(dynamicDeltaWeight.toFixed(4)),
            static_score: parseFloat(staticScore.toFixed(4)),
            delta_score: parseFloat(deltaScore.toFixed(4)),
            delta_imbalance: parseFloat(deltaImbalance.toFixed(4)),
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

    binanceWsClient.on('open', () => {
        console.log(`[Predictor] [INFO] [Binance WS] Subscribed to ${STREAM_NAME}`);
    });

    binanceWsClient.on('message', (data) => {
        try {
            const eventData = JSON.parse(data.toString());

            if (eventData.result === null && eventData.id !== undefined) {
                return; // Ignore subscription confirmation message.
            }

            if (eventData.b && eventData.a) { // Standard book ticker message
                const newBestBidPrice = parseFloat(eventData.b);
                
                // Send a simple price tick event if the price changed
                if (newBestBidPrice !== state.lastBestBidPrice && state.lastBestBidPrice !== 0) {
                    const tickPayload = { "Tick V": newBestBidPrice };
                    sendToInternalClient(tickPayload);
                }

                // Update current state with new data from the stream
                state.bestBidPrice = newBestBidPrice;
                state.bestBidQty = parseFloat(eventData.B);
                state.bestAskPrice = parseFloat(eventData.a);
                state.bestAskQty = parseFloat(eventData.A);

                // This is the core logic trigger. It runs on EVERY bookTicker update.
                calculateAndSendSignal();
                
                // After calculation, update "last" state values for the next event
                state.lastBestBidPrice = state.bestBidPrice;
                state.lastBestBidQty = state.bestBidQty;
                state.lastBestAskQty = state.bestAskQty;
            }
        } catch (e) {
            console.error(`[Predictor] [ERROR] [Binance WS] CRITICAL ERROR processing message: ${e.message}`, e.stack);
            console.error(`[Predictor] [ERROR] [Binance WS] Raw Data: ${data.toString()}`);
        }
    });

    binanceWsClient.on('error', () => { /* Silenced connection errors, handled by 'close' */ });
    binanceWsClient.on('close', () => {
        console.log(`[Predictor] [WARN] [Binance WS] Connection lost. Reconnecting in ${RECONNECT_INTERVAL_MS / 1000}s...`);
        binanceWsClient = null;
        setTimeout(connectToBinanceStream, RECONNECT_INTERVAL_MS);
    });
}

// --- Start the application ---
console.log("[Predictor] [INFO] [System] Starting predictor process...");
connectToInternalReceiver();
connectToBinanceStream();
