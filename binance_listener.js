

const WebSocket = require('ws');

// --- Global Error Handlers ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`);
    console.error(err.stack || err);
    console.error(`[Listener] Exception origin: ${origin}`);
    process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNHANDLED PROMISE REJECTION`);
    console.error('[Listener] Unhandled Rejection at:', promise);
    console.error('[Listener] Reason:', reason instanceof Error ? reason.stack : reason);
    process.exit(1);
});

// --- Configuration ---
const binanceBookTickerUrl = 'wss://stream.binance.com:9443/ws/btcusdt@bookTicker';
const internalReceiverUrl = 'ws://localhost:8082';
const RECONNECT_INTERVAL_MS = 5000;
const BOOKTICKER_PRICE_CHANGE_THRESHOLD = 1.2; // Set to 0 to disable filtering
const CALCULATION_INTERVAL_MS = 15;

// --- State Variables ---
let binanceBookTickerWsClient = null;
let internalWsClient = null;
let lastSentBestBidPrice = null;
let lastEvaluatedTime = 0;

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) return;
    internalWsClient = new WebSocket(internalReceiverUrl);
    internalWsClient.on('open', () => {
        console.log(`[Listener] Connected to internal receiver.`);
        lastSentBestBidPrice = null;
    });
    internalWsClient.on('error', err => console.error(`[Listener] Internal WS error:`, err.message));
    internalWsClient.on('close', () => {
        console.log(`[Listener] Internal receiver closed. Reconnecting...`);
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
}

// --- Binance Spot bookTicker Stream ---
function connectToBinanceBookTicker() {
    binanceBookTickerWsClient = new WebSocket(binanceBookTickerUrl);
    binanceBookTickerWsClient.on('open', () => {
        console.log('[Listener] Connected to Binance bookTicker stream.');
        lastSentBestBidPrice = null;
    });

    binanceBookTickerWsClient.on('message', (data) => {
        try {
            const now = Date.now();
            if (now - lastEvaluatedTime < CALCULATION_INTERVAL_MS) return;
            lastEvaluatedTime = now;

            const msg = JSON.parse(data);
            const bestBidPrice = parseFloat(msg.b);
            const eventTime = msg.E;

            if (isNaN(bestBidPrice)) return;

            let shouldSend = false;
            if (lastSentBestBidPrice === null || BOOKTICKER_PRICE_CHANGE_THRESHOLD === 0) {
                shouldSend = true;
            } else {
                const diff = Math.abs(bestBidPrice - lastSentBestBidPrice);
                if (diff >= BOOKTICKER_PRICE_CHANGE_THRESHOLD) shouldSend = true;
            }

            if (shouldSend && internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                const payload = JSON.stringify({ e: "bookTicker", E: eventTime, p: msg.b });
                internalWsClient.send(payload);
                lastSentBestBidPrice = bestBidPrice;
            }

        } catch (err) {
            console.error('[Listener] Error in bookTicker handler:', err.message);
        }
    });

    binanceBookTickerWsClient.on('close', () => {
        console.log('[Listener] bookTicker WebSocket closed. Reconnecting...');
        setTimeout(connectToBinanceBookTicker, RECONNECT_INTERVAL_MS);
    });

    binanceBookTickerWsClient.on('error', err => console.error('[Listener] bookTicker WS error:', err.message));
}

// --- Initialize ---
connectToInternalReceiver();
connectToBinanceBookTicker();
