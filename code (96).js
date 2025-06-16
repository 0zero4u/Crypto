// --- START OF FILE binance_listener.js ---

const WebSocket = require('ws');

// --- Global Error Handlers ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[Predictor] [ERROR] [System] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`);
    console.error(err.stack || err);
    console.error(`[Predictor] [ERROR] [System] Exception origin: ${origin}`);
    console.error(`[Predictor] [ERROR] [System] PID: ${process.pid} --- Exiting due to uncaught exception...`);
    cleanupAndExit(1);
});

process.on('unhandledRejection', (reason, promise) => {
    console.error(`[Predictor] [ERROR] [System] PID: ${process.pid} --- FATAL: UNHANDLED PROMISE REJECTION`);
    console.error('[Predictor] [ERROR] [System] Unhandled Rejection at:', promise);
    console.error('[Predictor] [ERROR] [System] Reason:', reason instanceof Error ? reason.stack : reason);
    console.error(`[Predictor] [ERROR] [System] PID: ${process.pid} --- Exiting due to unhandled promise rejection...`);
    cleanupAndExit(1);
});

function cleanupAndExit(exitCode = 1) {
    console.log('[Predictor] [INFO] [System] Initiating graceful shutdown...');
    const clientsToTerminate = [internalWsClient, binanceWsClient];
    clientsToTerminate.forEach(client => {
        if (client && typeof client.terminate === 'function') {
            try { client.terminate(); } catch (e) { console.error(`[Predictor] [ERROR] [System] Error terminating a WebSocket client: ${e.message}`); }
        }
    });
    setTimeout(() => {
        console.log(`[Predictor] [INFO] [System] Exiting with code ${exitCode}.`);
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
    console.log(`[Predictor] [INFO] [Internal WS] Attempting to connect to: ${internalReceiverUrl}`);
    internalWsClient = new WebSocket(internalReceiverUrl);

    internalWsClient.on('open', () => console.log('[Predictor] [INFO] [Internal WS] Connection established.'));
    internalWsClient.on('error', (err) => console.error(`[Predictor] [ERROR] [Internal WS] Connection error: ${err.message}`));
    internalWsClient.on('close', () => {
        console.warn('[Predictor] [WARN] [Internal WS] Connection closed. Reconnecting in ' + (RECONNECT_INTERVAL_MS / 1000) + 's...');
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
}

// --- BBO-Only Predictive Model Calculation ---
function calculateAndSendSignal() {
    const { bestBidPrice, bestBidQty, bestAskPrice, bestAskQty } = state;

    const totalVolume = bestBidQty + bestAskQty;
    if (totalVolume === 0 || bestBidPrice <= 0 || bestAskPrice <= 0) {
        console.warn(`[Predictor] [WARN] [Signal Logic] Invalid BBO data for calculation. Skipping. BidP: ${bestBidPrice}, AskP: ${bestAskPrice}, TotalVol: ${totalVolume}`);
        return;
    }

    const midPrice = (bestBidPrice + bestAskPrice) / 2;
    const efficientPrice = ((bestBidPrice * bestAskQty) + (bestAskPrice * bestBidQty)) / totalVolume;
    const signalValue = efficientPrice - midPrice;

    console.log(`[Predictor] [DEBUG] [Signal Logic] Calculated Y_est: ${signalValue.toFixed(6)} (P_mid: ${midPrice.toFixed(4)}, S_est: ${efficientPrice.toFixed(4)})`);

    let newSignal = lastSentSignal;

    if (lastSentSignal === 'NEUTRAL') {
        if (signalValue > SIGNAL_THRESHOLD) newSignal = 'BUY';
        else if (signalValue < -SIGNAL_THRESHOLD) newSignal = 'SELL';
    } else if (lastSentSignal === 'BUY') {
        if (signalValue < (SIGNAL_THRESHOLD - HYSTERESIS_BUFFER)) newSignal = 'NEUTRAL';
    } else if (lastSentSignal === 'SELL') {
        if (signalValue > (-SIGNAL_THRESHOLD + HYSTERESIS_BUFFER)) newSignal = 'NEUTRAL';
    }

    if (newSignal !== lastSentSignal) {
        console.log(`[Predictor] [INFO] [Signal Logic] SIGNAL STATE CHANGE: ${lastSentSignal} -> ${newSignal}`);
        const payload = {
            sig: newSignal,
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
        console.log(`[Predictor] [DEBUG] [Internal WS] Sending payload: ${payloadStr}`);
        try {
            internalWsClient.send(payloadStr);
        } catch (sendError) {
            console.error(`[Predictor] [ERROR] [Internal WS] FAILED to send payload. Error: ${sendError.message}`);
        }
    } else {
        console.warn(`[Predictor] [WARN] [Internal WS] Cannot send payload, client not connected. State: ${internalWsClient ? internalWsClient.readyState : 'null'}`);
    }
}

// --- Single Binance Stream Connection ---
function connectToBinanceStream() {
    if (binanceWsClient && (binanceWsClient.readyState === WebSocket.OPEN || binanceWsClient.readyState === WebSocket.CONNECTING)) return;
    console.log(`[Predictor] [INFO] [Binance WS] Attempting to connect to: ${BINANCE_STREAM_URL}`);
    binanceWsClient = new WebSocket(BINANCE_STREAM_URL);

    binanceWsClient.on('open', () => console.log('[Predictor] [INFO] [Binance WS] Connection established. Listening for BBO data.'));

    binanceWsClient.on('message', (data) => {
        try {
            const eventData = JSON.parse(data.toString());

            if (eventData.e === 'bookTicker') {
                const newBestBidPrice = parseFloat(eventData.b);
                console.log(`[Predictor] [DEBUG] [Binance WS] Received BBO: BidP=${newBestBidPrice} BidQ=${eventData.B} | AskP=${eventData.a} AskQ=${eventData.A}`);

                // --- Tick Change Detection Logic for "Tick V" ---
                if (newBestBidPrice !== state.lastBestBidPrice && state.lastBestBidPrice !== 0) {
                    console.log(`[Predictor] [INFO] [TickV] Best Bid changed: ${state.lastBestBidPrice} -> ${newBestBidPrice}. Sending Tick V.`);
                    const tickPayload = { "Tick V": newBestBidPrice };
                    sendToInternalClient(tickPayload);
                }

                // Update state
                state.bestBidPrice = newBestBidPrice;
                state.bestBidQty = parseFloat(eventData.B);
                state.bestAskPrice = parseFloat(eventData.a);
                state.bestAskQty = parseFloat(eventData.A);
                state.lastBestBidPrice = newBestBidPrice;

                // On every update, run the main calculation
                calculateAndSendSignal();
            } else {
                console.warn(`[Predictor] [WARN] [Binance WS] Received unexpected message type: ${eventData.e || 'Unknown'}`);
            }
        } catch (e) {
            console.error(`[Predictor] [ERROR] [Binance WS] CRITICAL ERROR processing message: ${e.message}`, e.stack);
            console.error(`[Predictor] [ERROR] [Binance WS] Raw Data: ${data.toString()}`);
        }
    });

    binanceWsClient.on('error', (err) => console.error(`[Predictor] [ERROR] [Binance WS] Connection error: ${err.message}`));
    binanceWsClient.on('close', () => {
        console.warn('[Predictor] [WARN] [Binance WS] Connection closed. Reconnecting in ' + (RECONNECT_INTERVAL_MS / 1000) + 's...');
        binanceWsClient = null;
        setTimeout(connectToBinanceStream, RECONNECT_INTERVAL_MS);
    });
}

// --- Start the application ---
console.log(`[Predictor] [INFO] [System] Starting BBO Model w/ TickV for ${SYMBOL}. PID: ${process.pid}`);
connectToInternalReceiver();
connectToBinanceStream();