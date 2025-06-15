// --- START OF FILE binance_listener.js ---

const WebSocket = require('ws');
const https = require('https'); // Required for KuCoin connection token

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
let kucoinHeartbeatInterval = null;

function cleanupAndExit(exitCode = 1) {
    if (bybitHeartbeatInterval) clearInterval(bybitHeartbeatInterval);
    if (kucoinHeartbeatInterval) clearInterval(kucoinHeartbeatInterval);

    const clientsToTerminate = [internalWsClient, binanceWsClient, bybitWsClient, okxWsClient, kucoinWsClient];
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
const KUCOIN_SYMBOL = 'BTC-USDT';
const RECONNECT_INTERVAL_MS = 5000;
const AVG_PRICE_CHANGE_THRESHOLD = 1.0;
const KUCOIN_PRICE_CHANGE_THRESHOLD = 1.0; // *** ADDED: Separate threshold for KuCoin ***
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
let binanceWsClient = null, bybitWsClient = null, okxWsClient = null, kucoinWsClient = null;
let latestBinanceBid = null, latestBybitBid = null, latestOkxBid = null; // KuCoin bid removed from this group
let last_sent_avg_price = null;
let last_sent_kucoin_bid = null; // *** ADDED: State for last sent KuCoin price ***

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
    // *** MODIFIED: KuCoin is no longer processed here ***
    switch(exchange) {
        case 'binance': latestBinanceBid = price; break;
        case 'bybit':   latestBybitBid = price; break;
        case 'okx':     latestOkxBid = price; break;
    }
    calculateAndSendAverage();
}

// *** ADDED: New function to handle KuCoin price updates independently ***
function processKucoinPrice(price) {
    let shouldSend = false;
    if (last_sent_kucoin_bid === null) {
        shouldSend = true; // Send the first price received
    } else {
        const priceDifference = Math.abs(price - last_sent_kucoin_bid);
        if (priceDifference >= KUCOIN_PRICE_CHANGE_THRESHOLD) {
            shouldSend = true;
        }
    }

    if (shouldSend) {
        const pricePayload = { ku: price };
        sendToInternalClient(pricePayload);
        last_sent_kucoin_bid = price;
    }
}


function calculateAndSendAverage() {
    // *** MODIFIED: KuCoin price is removed from this calculation ***
    const prices = [latestBinanceBid, latestBybitBid, latestOkxBid].filter(p => p !== null && p > 0);
    if (prices.length === 0) return;

    const rawAveragePrice = prices.reduce((sum, price) => sum + price, 0) / prices.length;
    const current_avg_price = parseFloat(rawAveragePrice.toFixed(2));

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
        // *** MODIFIED: Payload for average price no longer contains KuCoin data ***
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

// Binance
function connectToBinance() {
    binanceWsClient = new WebSocket(BINANCE_SPOT_STREAM_URL);
    binanceWsClient.on('open', () => { latestBinanceBid = null; last_sent_avg_price = null; });
    binanceWsClient.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());
            if (message.b) { processNewPriceUpdate('binance', parseFloat(message.b)); }
        } catch (e) { /* Silent */ }
    });
    binanceWsClient.on('error', () => { /* Silent */ });
    binanceWsClient.on('close', () => {
        binanceWsClient = null;
        setTimeout(connectToBinance, RECONNECT_INTERVAL_MS);
    });
}

// Bybit
function connectToBybit() {
    bybitWsClient = new WebSocket(BYBIT_SPOT_STREAM_URL);
    if (bybitHeartbeatInterval) clearInterval(bybitHeartbeatInterval);

    bybitWsClient.on('open', () => {
        latestBybitBid = null; last_sent_avg_price = null;
        bybitWsClient.send(BYBIT_SUBSCRIBE_MSG);
        bybitHeartbeatInterval = setInterval(() => {
            if (bybitWsClient.readyState === WebSocket.OPEN) { bybitWsClient.send(BYBIT_PING_MSG); }
        }, 20000);
    });
    bybitWsClient.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());
            if (message.topic && message.topic.startsWith('orderbook.1') && message.data) {
                const bestBid = message.data.b?.[0]?.[0];
                if (bestBid) { processNewPriceUpdate('bybit', parseFloat(bestBid)); }
            }
        } catch (e) { /* Silent */ }
    });
    bybitWsClient.on('error', () => { /* Silent */ });
    bybitWsClient.on('close', () => {
        bybitWsClient = null;
        if (bybitHeartbeatInterval) clearInterval(bybitHeartbeatInterval);
        setTimeout(connectToBybit, RECONNECT_INTERVAL_MS);
    });
}

// OKX
function connectToOkx() {
    okxWsClient = new WebSocket(OKX_STREAM_URL);
    okxWsClient.on('open', () => {
        latestOkxBid = null; last_sent_avg_price = null;
        okxWsClient.send(OKX_SUBSCRIBE_MSG);
    });
    okxWsClient.on('message', (data) => {
        const messageString = data.toString();
        if (messageString === 'ping') {
            if (okxWsClient.readyState === WebSocket.OPEN) { okxWsClient.send('pong'); }
            return;
        }
        try {
            const message = JSON.parse(messageString);
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

// KuCoin: Requires POST request for token, then WebSocket connection with heartbeat.
function connectToKucoin() {
    const postData = JSON.stringify({});
    const options = {
        hostname: 'api.kucoin.com', port: 443, path: '/api/v1/bullet-public', method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Content-Length': postData.length }
    };

    const req = https.request(options, (res) => {
        let data = '';
        res.on('data', (chunk) => { data += chunk; });
        res.on('end', () => {
            try {
                const response = JSON.parse(data);
                if (response.code !== '200000' || !response.data.token) {
                    setTimeout(connectToKucoin, RECONNECT_INTERVAL_MS); return;
                }
                const { token, instanceServers } = response.data;
                const endpoint = instanceServers[0].endpoint;
                const pingInterval = instanceServers[0].pingInterval;
                establishKucoinWsConnection(`${endpoint}?token=${token}`, pingInterval);
            } catch (e) { setTimeout(connectToKucoin, RECONNECT_INTERVAL_MS); }
        });
    });
    req.on('error', () => { setTimeout(connectToKucoin, RECONNECT_INTERVAL_MS); });
    req.write(postData);
    req.end();
}

function establishKucoinWsConnection(url, pingInterval) {
    if (kucoinWsClient) { kucoinWsClient.terminate(); }
    if (kucoinHeartbeatInterval) { clearInterval(kucoinHeartbeatInterval); }

    kucoinWsClient = new WebSocket(url);

    kucoinWsClient.on('open', () => {
        last_sent_kucoin_bid = null; // Reset on new connection
        const subscribeMsg = { id: Date.now(), type: 'subscribe', topic: `/market/level1:${KUCOIN_SYMBOL}`, privateChannel: false, response: true };
        kucoinWsClient.send(JSON.stringify(subscribeMsg));

        kucoinHeartbeatInterval = setInterval(() => {
            if (kucoinWsClient.readyState === WebSocket.OPEN) {
                kucoinWsClient.send(JSON.stringify({ id: Date.now(), type: 'ping' }));
            }
        }, pingInterval - 2000); // Send ping slightly before interval expires
    });

    kucoinWsClient.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());
            if (message.type === 'message' && message.topic === `/market/level1:${KUCOIN_SYMBOL}`) {
                if (message.data && message.data.bestBid) {
                    // *** MODIFIED: Call the new dedicated function for KuCoin prices ***
                    processKucoinPrice(parseFloat(message.data.bestBid));
                }
            }
        } catch (e) { /* Silent */ }
    });

    kucoinWsClient.on('error', () => { /* Silent */ });
    kucoinWsClient.on('close', () => {
        kucoinWsClient = null;
        if (kucoinHeartbeatInterval) clearInterval(kucoinHeartbeatInterval);
        setTimeout(connectToKucoin, RECONNECT_INTERVAL_MS);
    });
}


// --- Start all connections ---
connectToInternalReceiver();
connectToBinance();
connectToBybit();
connectToOkx();
connectToKucoin();

// --- No startup log for silent operation ---
// --- END OF FILE binance_listener.js ---