// --- START OF FILE binance_listener.js ---

const WebSocket = require('ws');

// --- Global Error Handlers (Essential for diagnosing fatal crashes) ---
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

// --- Heartbeat and State Management ---
let bybitHeartbeatInterval = null;

function cleanupAndExit(exitCode = 1) {
    if (bybitHeartbeatInterval) clearInterval(bybitHeartbeatInterval);
    
    const clientsToTerminate = [internalWsClient, binanceWsClient, bybitWsClient, okxWsClient];
    clientsToTerminate.forEach(client => {
        if (client && typeof client.terminate === 'function') {
            try { client.terminate(); } catch (e) { console.error(`[Listener] Error terminating a WebSocket client: ${e.message}`); }
        }
    });
    setTimeout(() => {
        process.exit(exitCode);
    }, 1000).unref();
}

// --- Listener Configuration ---
const SYMBOL = 'BTCUSDT';
const OKX_SYMBOL = 'BTC-USDT';
const RECONNECT_INTERVAL_MS = 5000;
const AVG_PRICE_CHANGE_THRESHOLD = 0.5;
const internalReceiverUrl = 'ws://localhost:8082';

// --- Exchange Stream URLs and Subscription Messages ---
const BINANCE_SPOT_STREAM_URL = `wss://stream.binance.com:9443/ws/${SYMBOL.toLowerCase()}@bookTicker`;
const BYBIT_SPOT_STREAM_URL = 'wss://stream.bybit.com/v5/public/spot';
const BYBIT_SUBSCRIBE_MSG = JSON.stringify({ op: "subscribe", args: [`orderbook.1.${SYMBOL}`] });
const BYBIT_PING_MSG = JSON.stringify({ op: "ping" });
const OKX_STREAM_URL = 'wss://ws.okx.com:8443/ws/v5/public';
const OKX_SUBSCRIBE_MSG = JSON.stringify({ op: "subscribe", args: [{ channel: "bbo-tbt", instId: OKX_SYMBOL }] });

// --- Listener State Variables ---
let internalWsClient = null;
let binanceWsClient = null, bybitWsClient = null, okxWsClient = null;
let latestBinanceBid = null, latestBybitBid = null, latestOkxBid = null;
let last_sent_avg_price = null;

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    internalWsClient = new WebSocket(internalReceiverUrl);
    internalWsClient.on('error', () => { /* Silent */ });
    internalWsClient.on('close', () => {
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
}

// --- Data Processing and Forwarding ---
function processNewPriceUpdate(exchange, price) {
    switch(exchange) {
        case 'binance': latestBinanceBid = price; break;
        case 'bybit': latestBybitBid = price; break;
        case 'okx': latestOkxBid = price; break;
    }
    calculateAndSendAverage();
}

function calculateAndSendAverage() {
    const prices = [latestBinanceBid, latestBybitBid, latestOkxBid].filter(p => p !== null && p > 0);
    if (prices.length === 0) return;

    const current_avg_price = prices.reduce((sum, price) => sum + price, 0) / prices.length;

    let shouldSend = false;
    if (last_sent_avg_price === null) {
        shouldSend = true;
    } else {
        const priceDifference = Math.abs(current_avg_price - last_sent_avg_price);
        if (priceDifference >= AVG_PRICE_CHANGE_THRESHOLD) {
            shouldSend = true;
        }
    }
    
    if (shouldSend) {
        const pricePayload = { p: current_avg_price };
        sendToInternalClient(pricePayload);
        last_sent_avg_price = current_avg_price;
    }
}

function sendToInternalClient(payload) {
    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        try { internalWsClient.send(JSON.stringify(payload)); } catch (e) { /* Silent */ }
    }
}

// --- Exchange Connection Functions ---

// Binance: Relies on standard ping/pong, handled automatically by `ws`. No extra code needed.
function connectToBinance() {
    binanceWsClient = new WebSocket(BINANCE_SPOT_STREAM_URL);
    binanceWsClient.on('open', () => { latestBinanceBid = null; last_sent_avg_price = null; });
    binanceWsClient.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());
            // Payload format: { "b": "best_bid_price", ... }
            if (message.b) { processNewPriceUpdate('binance', parseFloat(message.b)); }
        } catch (e) { /* Silent */ }
    });
    binanceWsClient.on('error', () => { /* Silent */ });
    binanceWsClient.on('close', () => {
        binanceWsClient = null;
        setTimeout(connectToBinance, RECONNECT_INTERVAL_MS);
    });
}

// Bybit: Requires client to send a ping every 20 seconds.
function connectToBybit() {
    bybitWsClient = new WebSocket(BYBIT_SPOT_STREAM_URL);
    
    // Clear any lingering heartbeat interval from a previous connection
    if (bybitHeartbeatInterval) clearInterval(bybitHeartbeatInterval);

    bybitWsClient.on('open', () => {
        latestBybitBid = null; last_sent_avg_price = null;
        bybitWsClient.send(BYBIT_SUBSCRIBE_MSG);
        
        // ** Start sending pings to keep connection alive **
        bybitHeartbeatInterval = setInterval(() => {
            if (bybitWsClient.readyState === WebSocket.OPEN) {
                bybitWsClient.send(BYBIT_PING_MSG);
            }
        }, 20000); // 20 seconds
    });

    bybitWsClient.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());
            // Payload format: { data: { b: [["price", "size"]], ... } }
            if (message.topic && message.topic.startsWith('orderbook.1') && message.data) {
                const bestBid = message.data.b?.[0]?.[0];
                if (bestBid) { processNewPriceUpdate('bybit', parseFloat(bestBid)); }
            }
        } catch (e) { /* Silent */ }
    });

    bybitWsClient.on('error', () => { /* Silent */ });

    bybitWsClient.on('close', () => {
        bybitWsClient = null;
        if (bybitHeartbeatInterval) clearInterval(bybitHeartbeatInterval); // ** Stop pings **
        setTimeout(connectToBybit, RECONNECT_INTERVAL_MS);
    });
}

// OKX: Requires client to respond to server's "ping" message with a "pong" message.
function connectToOkx() {
    okxWsClient = new WebSocket(OKX_STREAM_URL);
    okxWsClient.on('open', () => {
        latestOkxBid = null; last_sent_avg_price = null;
        okxWsClient.send(OKX_SUBSCRIBE_MSG);
    });

    okxWsClient.on('message', (data) => {
        const messageString = data.toString();
        
        // ** Handle heartbeat **
        if (messageString === 'ping') {
            if (okxWsClient.readyState === WebSocket.OPEN) {
                okxWsClient.send('pong');
            }
            return;
        }

        try {
            const message = JSON.parse(messageString);
            // Payload format: { data: [{ bids: [["price", "size", ...]], ... }] }
            if (message.arg?.channel === 'bbo-tbt' && message.data?.[0]) {
                const bestBid = message.data[0].bids?.[0]?.[0];
                if (bestBid) { processNewPriceUpdate('okx', parseFloat(bestBid)); }
            }
        } catch (e) { /* Silent */ }
    });

    okxWsClient.on('error', () => { /* Silent */ });

    okxWsClient.on('close', () => {
        okxWsClient = null;
        setTimeout(connectToOkx, RECONNECT_INTERVAL_MS);
    });
}

// --- Start all connections ---
connectToInternalReceiver();
connectToBinance();
connectToBybit();
connectToOkx();

// --- No startup log for silent operation ---
// --- END OF FILE binance_listener.js ---
