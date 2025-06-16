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
const SIGNAL_THRESHOLD = 0.0001; 
const STRONG_SIGNAL_THRESHOLD_MULTIPLIER = 2.0;
const HYSTERESIS_BUFFER = 0.00002;
// <<< NEW: Threshold to prevent strong signals without momentum confirmation
const MOMENTUM_CONFIRMATION_WEIGHT_THRESHOLD = 0.30; // Require at least 15% delta weight for a STRONG signal

// --- Dynamic Delta Imbalance Parameters ---
// <<< MODIFIED: Increased the baseline weight for the delta score
const MIN_DELTA_WEIGHT = 0.10;    // Baseline weight for delta score (e.g., 10%), up from 5%
const MAX_DELTA_WEIGHT = 0.50;    // Max weight during significant events (e.g., 50%)
const DELTA_MIDPOINT = 2.0;       
const DELTA_CURVE_STEEPNESS = 1.5;
const DELTA_SENSITIVITY_THRESHOLD = 1.0; 

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
function calculateDynamicDeltaWeight(deltaImbalance) {
    const absDelta = Math.abs(deltaImbalance);
    const exponent = -DELTA_CURVE_STEEPNESS * (absDelta - DELTA_MIDPOINT);
    const sigmoid = 1 / (1 + Math.exp(exponent));
    const dynamicWeight = MIN_DELTA_WEIGHT + (MAX_DELTA_WEIGHT - MIN_DELTA_WEIGHT) * sigmoid;
    return dynamicWeight;
}

// --- BBO-Only Predictive Model Calculation ---
function calculateAndSendSignal() {
    const { bestBidPrice, bestBidQty, bestAskPrice, bestAskQty, lastBestBidQty, lastBestAskQty } = state;

    const totalVolume = bestBidQty + bestAskQty;
    if (totalVolume === 0 || bestBidPrice <= 0 || bestAskPrice <= 0) {
        return;
    }

    // --- 1. Static Imbalance ---
    const midPrice = (bestBidPrice + bestAskPrice) / 2;
    const efficientPrice = ((bestBidPrice * bestAskQty) + (bestAskPrice * bestBidQty)) / totalVolume;
    const signalValue = efficientPrice - midPrice;
    const staticScore = signalValue / SIGNAL_THRESHOLD;

    // --- 2. Delta Imbalance ---
    let deltaImbalance = 0;
    if (lastBestBidQty > 0 || lastBestAskQty > 0) {
        const currentImbalance = bestBidQty - bestAskQty;
        const previousImbalance = lastBestBidQty - lastBestAskQty;
        deltaImbalance = currentImbalance - previousImbalance;
    }
    const deltaScore = deltaImbalance / DELTA_SENSITIVITY_THRESHOLD;

    // --- 3. Calculate Dynamic Weight and Combine Scores ---
    const dynamicDeltaWeight = calculateDynamicDeltaWeight(deltaImbalance);
    const combinedScore = ((1 - dynamicDeltaWeight) * staticScore) + (dynamicDeltaWeight * deltaScore);

    // --- 4. Signal Generation with Hysteresis and Graded Strength ---
    const WEAK_BUY_THRESHOLD = 1.0;
    const STRONG_BUY_THRESHOLD = WEAK_BUY_THRESHOLD * STRONG_SIGNAL_THRESHOLD_MULTIPLIER;
    const WEAK_SELL_THRESHOLD = -1.0;
    const STRONG_SELL_THRESHOLD = WEAK_SELL_THRESHOLD * STRONG_SIGNAL_THRESHOLD_MULTIPLIER;

    let newSignal = lastSentSignal;

    let potentialSignal = 'NEUTRAL';
    if (combinedScore >= STRONG_BUY_THRESHOLD) {
        potentialSignal = 'STRONG_BUY';
    } else if (combinedScore >= WEAK_BUY_THRESHOLD) {
        potentialSignal = 'WEAK_BUY';
    } else if (combinedScore <= STRONG_SELL_THRESHOLD) {
        potentialSignal = 'STRONG_SELL';
    } else if (combinedScore <= WEAK_SELL_THRESHOLD) {
        potentialSignal = 'WEAK_SELL';
    }
    
    // <<< NEW: MOMENTUM CONFIRMATION LOGIC
    // Downgrade a "STRONG" signal to "WEAK" if it lacks momentum confirmation.
    // This prevents spoof-like orders from triggering a high-confidence alert.
    if ((potentialSignal === 'STRONG_BUY' || potentialSignal === 'STRONG_SELL') && dynamicDeltaWeight < MOMENTUM_CONFIRMATION_WEIGHT_THRESHOLD) {
        potentialSignal = (potentialSignal === 'STRONG_BUY') ? 'WEAK_BUY' : 'WEAK_SELL';
    }

    // Apply Hysteresis: Decide whether to switch state
    if (lastSentSignal === 'NEUTRAL') {
        newSignal = potentialSignal;
    } else if (lastSentSignal.includes('BUY')) {
        if (combinedScore < (WEAK_BUY_THRESHOLD - (HYSTERESIS_BUFFER / SIGNAL_THRESHOLD))) {
            newSignal = 'NEUTRAL';
        } else {
            newSignal = potentialSignal;
        }
    } else if (lastSentSignal.includes('SELL')) {
        if (combinedScore > (WEAK_SELL_THRESHOLD + (HYSTERESIS_BUFFER / SIGNAL_THRESHOLD))) {
            newSignal = 'NEUTRAL';
        } else {
            newSignal = potentialSignal;
        }
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
                return;
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

                calculateAndSendSignal();
                
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
