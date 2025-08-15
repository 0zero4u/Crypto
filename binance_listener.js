// bybit_listener_optimized.js
const WebSocket = require('ws');

// --- Process-wide Error Handling ---
process.on('uncaughtException', (err) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`, err.stack || err);
    cleanupAndExit(1);
});
process.on('unhandledRejection', (reason) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNHANDLED PROMISE REJECTION`, reason);
    cleanupAndExit(1);
});

function cleanupAndExit(exitCode = 1) {
    const clientsToTerminate = [internalWsClient, bybitWsClient, okxWsClient];
    console.error('[Listener] Initiating cleanup...');
    clientsToTerminate.forEach(client => {
        if (client && (client.readyState === WebSocket.OPEN || client.readyState === WebSocket.CONNECTING)) {
            try { client.terminate(); } 
            catch (e) { console.error(`[Listener] Error during WebSocket termination: ${e.message}`); }
        }
    });
    setTimeout(() => {
        console.error(`[Listener] Exiting with code ${exitCode}.`);
        process.exit(exitCode);
    }, 1000).unref();
}

// --- Configuration ---
const SYMBOL_BYBIT = 'BTCUSDT';
const SYMBOL_OKX = 'BTC-USDT';
const RECONNECT_INTERVAL_MS = 5000;
const AVERAGE_PRICE_CHANGE_THRESHOLD = 1.0;

const internalReceiverUrl = 'ws://instance-20250627-040948.asia-south2-a.c.ace-server-460719-b7.internal:8082/internal';

// --- Exchange Stream URLs ---
const BYBIT_STREAM_URL = 'wss://stream.bybit.com/v5/public/spot';
const OKX_STREAM_URL = 'wss://ws.okx.com:8443/ws/v5/public';

// --- WebSocket Clients and State ---
let internalWsClient, bybitWsClient, okxWsClient;
let last_sent_price = null;
let latest_prices = { bybit: null, okx: null };

// Optimization: Reusable payload object
const payload_to_send = { type: 'S', p: 0.0 };

function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) return;
    internalWsClient = new WebSocket(internalReceiverUrl);
    internalWsClient.on('close', () => {
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
}

function sendToInternalClient(payload) {
    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        try { internalWsClient.send(JSON.stringify(payload)); } catch {}
    }
}

function calculateAndSendAverage() {
    const valid_prices = Object.values(latest_prices).filter(p => p !== null);
    if (!valid_prices.length) return;
    const average_price = valid_prices.reduce((sum, p) => sum + p, 0) / valid_prices.length;
    if (last_sent_price === null || Math.abs(average_price - last_sent_price) >= AVERAGE_PRICE_CHANGE_THRESHOLD) {
        payload_to_send.p = average_price;
        sendToInternalClient(payload_to_send);
        last_sent_price = average_price;
    }
}

function connectToBybit() {
    bybitWsClient = new WebSocket(BYBIT_STREAM_URL);
    bybitWsClient.on('open', () => {
        try {
            bybitWsClient.send(JSON.stringify({ op: "subscribe", args: [`publicTrade.${SYMBOL_BYBIT}`] }));
        } catch {}
    });
    bybitWsClient.on('message', (data) => {
        try {
            const msg = JSON.parse(data.toString());
            if (msg.topic?.startsWith('publicTrade') && msg.data) {
                const tradePrice = parseFloat(msg.data[0].p);
                if (!isNaN(tradePrice)) {
                    latest_prices.bybit = tradePrice;
                    calculateAndSendAverage();
                }
            }
        } catch {}
    });
    bybitWsClient.on('close', () => {
        latest_prices.bybit = null;
        bybitWsClient = null;
        setTimeout(connectToBybit, RECONNECT_INTERVAL_MS);
    });
    const heartbeat = setInterval(() => {
        if (bybitWsClient?.readyState === WebSocket.OPEN) {
            try { bybitWsClient.send(JSON.stringify({ op: 'ping' })); } catch {}
        } else clearInterval(heartbeat);
    }, 20000);
}

function connectToOkx() {
    okxWsClient = new WebSocket(OKX_STREAM_URL);
    okxWsClient.on('open', () => {
        try {
            okxWsClient.send(JSON.stringify({ op: "subscribe", args: [{ channel: "trades", instId: SYMBOL_OKX }] }));
        } catch {}
    });
    okxWsClient.on('message', (data) => {
        try {
            const msg = JSON.parse(data.toString());
            if (msg.arg?.channel === 'trades' && msg.data) {
                const tradePrice = parseFloat(msg.data[0].px);
                if (!isNaN(tradePrice)) {
                    latest_prices.okx = tradePrice;
                    calculateAndSendAverage();
                }
            }
        } catch {}
    });
    okxWsClient.on('close', () => {
        latest_prices.okx = null;
        okxWsClient = null;
        setTimeout(connectToOkx, RECONNECT_INTERVAL_MS);
    });
    const heartbeat = setInterval(() => {
        if (okxWsClient?.readyState === WebSocket.OPEN) {
            try { okxWsClient.send('ping'); } catch {}
        } else clearInterval(heartbeat);
    }, 25000);
}

// --- Script Entry Point ---
connectToInternalReceiver();
connectToBybit();
connectToOkx();
