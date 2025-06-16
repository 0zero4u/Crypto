// --- START OF FILE binance_listener_bbo_model_with_tick_v.js ---

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

// --- BBO-Only Model uses only the bookTicker stream ---
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
    bestBidPrice: 0,
    bestBidQty: 0,
    bestAskPrice: 0,
    bestAskQty: 0,
    lastBestBidPrice: 0, // <-- NEW: To track bid price changes
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

// --- BBO-Only Predictive Model Calculation and Signal Generation ---
function calculateAndSendSignal() {
    const { bestBidPrice, bestBidQty, bestAskPrice, bestAskQty } = state;

    const totalVolume = bestBidQty + bestAskQty;
    if (totalVolume === 0 || bestBidPrice <= 0 || bestAskPrice <= 0) {
        return;
    }

    const midPrice = (bestBidPrice + bestAskPrice) / 2;
    const efficientPrice = ((bestBidPrice * bestAskQty) + (bestAskPrice * bestBidQty)) / totalVolume;
    const signalValue = efficientPrice - midPrice;

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
        const payload = {
            sig: newSignal,
            y_est: parseFloat(signalValue.toFixed(6)),
            s_est: parseFloat(efficientPrice.toFixed(4)),
            p_mid: parseFloat(midPrice.toFixed(4)),
        };

        sendToInternalClient(payload);
        lastSentSignal = newSignal;
        console.log(`[Predictor] SIGNAL CHANGE: ${newSignal.padEnd(8)}| Sent Payload: ${JSON.stringify(payload)}`);
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
function connectToBinanceStream() {
    if (binanceWsClient && (binanceWsClient.readyState === WebSocket.OPEN || binanceWsClient.readyState === WebSocket.CONNECTING)) return;

    console.log(`[Predictor] Connecting to Binance Stream: ${BINANCE_STREAM_URL}`);
    binanceWsClient = new WebSocket(BINANCE_STREAM_URL);

    binanceWsClient.on('open', () => console.log('[Predictor] Successfully connected to Binance Stream.'));

    binanceWsClient.on('message', (data) => {
        try {
            const eventData = JSON.parse(data.toString());

            if (eventData.e === 'bookTicker') {
                const newBestBidPrice = parseFloat(eventData.b);

                // --- START: NEW Tick Change Detection Logic ---
                // Check if the bid price has changed and it's not the first-ever update.
                if (newBestBidPrice !== state.lastBestBidPrice && state.lastBestBidPrice !== 0) {
                    const tickPayload = { "Tick V": newBestBidPrice };
                    sendToInternalClient(tickPayload);
                    // Optional: Log this separate event for clarity
                    // console.log(`[Predictor] TICK CHANGE: Sent Best Bid update -> ${newBestBidPrice}`);
                }
                // --- END: NEW Tick Change Detection Logic ---

                // Update state for the main formula calculation
                state.bestBidPrice = newBestBidPrice;
                state.bestBidQty = parseFloat(eventData.B);
                state.bestAskPrice = parseFloat(eventData.a);
                state.bestAskQty = parseFloat(eventData.A);

                // IMPORTANT: Update the last known bid price for the next comparison
                state.lastBestBidPrice = newBestBidPrice;

                // On every update to the BBO, calculate the primary signal
                calculateAndSendSignal();
            }
        } catch (e) {
            console.error(`[Predictor] CRITICAL ERROR in Stream handler: ${e.message}`, e.stack);
        }
    });

    binanceWsClient.on('error', (err) => console.error(`[Predictor] Binance WebSocket error: ${err.message}`));

    binanceWsClient.on('close', () => {
        console.log('[Predictor] Binance connection closed. Reconnecting...');
        binanceWsClient = null;
        setTimeout(connectToBinanceStream, RECONNECT_INTERVAL_MS);
    });
}

// --- Start the application ---
console.log(`[Predictor] Starting up BBO-Only Predictive Model w/ TickV for symbol: ${SYMBOL}...`);
connectToInternalReceiver();
connectToBinanceStream();
