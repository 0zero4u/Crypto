// --- START OF FILE binance_listener_optimized.js (Corrected) ---

const WebSocket = require('ws');
const https = require('https'); // FIX: Changed 'httpss' to 'https'

// --- Global Error Handlers ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`);
    console.error(err.stack || err);
    cleanupAndExit(1);
});
process.on('unhandledRejection', (reason, promise) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNHANDLED PROMISE REJECTION`);
    console.error('[Listener] Unhandled Rejection at:', promise);
    console.error('[Listener] Reason:', reason instanceof Error ? reason.stack : reason);
    cleanupAndExit(1);
});

// --- Heartbeat and State Management ---
let kucoinHeartbeatInterval = null;
function cleanupAndExit(exitCode = 1) {
    if (kucoinHeartbeatInterval) clearInterval(kucoinHeartbeatInterval);
    const clientsToTerminate = [internalWsClient, binanceWsClient, bybitWsClient, okxWsClient, kucoinWsClient];
    clientsToTerminate.forEach(client => {
        if (client && typeof client.terminate === 'function') {
            try { client.terminate(); } catch (e) { console.error(`[Listener] Error terminating a WebSocket client: ${e.message}`); }
        }
    });
    setTimeout(() => { process.exit(exitCode); }, 1000).unref();
}

// --- Listener Configuration ---
const SYMBOL = 'BTCUSDT';
const OKX_SYMBOL = 'BTC-USDT';
const KUCOIN_SYMBOL = 'BTC-USDT';
const RECONNECT_INTERVAL_MS = 5000;
const AVG_PRICE_CHANGE_THRESHOLD = 1.0;
const KUCOIN_PRICE_CHANGE_THRESHOLD = 1.0;
const internalReceiverUrl = 'ws://localhost:8082';

// --- Exchange Stream URLs ---
const BINANCE_SPOT_STREAM_URL = `wss://stream.binance.com:9443/ws/${SYMBOL.toLowerCase()}@bookTicker`;
const BYBIT_SPOT_STREAM_URL = 'wss://stream.bybit.com/v5/public/spot';
const OKX_STREAM_URL = 'wss://ws.okx.com:8443/ws/v5/public';

// --- Listener State Variables ---
let internalWsClient = null, binanceWsClient = null, bybitWsClient = null, okxWsClient = null, kucoinWsClient = null;
let latestBinanceBid = null, latestBybitBid = null, latestOkxBid = null;
let last_sent_avg_price = null, last_sent_kucoin_bid = null;

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) return;
    internalWsClient = new WebSocket(internalReceiverUrl);
    internalWsClient.on('error', () => {}); // Mute connection errors, reconnect logic handles it
    internalWsClient.on('close', () => {
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
}

// --- Data Processing and Forwarding ---
function processNewPriceUpdate(exchange, price) {
    switch(exchange) {
        case 'binance': latestBinanceBid = price; break;
        case 'bybit':   latestBybitBid = price; break;
        case 'okx':     latestOkxBid = price; break;
    }
    calculateAndSendAverage();
}

function processKucoinPrice(price) {
    let shouldSend = false;
    if (last_sent_kucoin_bid === null) {
        shouldSend = true;
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
        const pricePayload = { p: current_avg_price };
        sendToInternalClient(pricePayload);
        last_sent_avg_price = current_avg_price;
    }
}

function sendToInternalClient(payload) {
    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        try {
            internalWsClient.send(JSON.stringify(payload));
        } catch (e) { /* Silent: Errors will be caught by the receiver or on next send attempt */ }
    }
}

// --- Exchange Connection Functions ---

function connectToBinance() {
    binanceWsClient = new WebSocket(BINANCE_SPOT_STREAM_URL);
    binanceWsClient.on('open', () => { latestBinanceBid = null; last_sent_avg_price = null; });
    binanceWsClient.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());
            if (message.b) { processNewPriceUpdate('binance', parseFloat(message.b)); }
        } catch (e) {/* Mute parsing errors on non-critical streams */}
    });
    binanceWsClient.on('error', () => {});
    binanceWsClient.on('close', () => {
        binanceWsClient = null;
        setTimeout(connectToBinance, RECONNECT_INTERVAL_MS);
    });
}

function connectToBybit() {
    const BYBIT_SUBSCRIBE_MSG = JSON.stringify({ op: "subscribe", args: [`orderbook.1.${SYMBOL}`] });
    bybitWsClient = new WebSocket(BYBIT_SPOT_STREAM_URL);

    bybitWsClient.on('open', () => {
        latestBybitBid = null;
        last_sent_avg_price = null;
        bybitWsClient.send(BYBIT_SUBSCRIBE_MSG);
    });

    bybitWsClient.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());
            if (message.op === 'ping') {
                if (bybitWsClient.readyState === WebSocket.OPEN) {
                    bybitWsClient.send(JSON.stringify({ op: "pong" }));
                }
                return;
            }

            if (message.topic && message.topic.startsWith('orderbook.1') && message.data) {
                const bestBid = message.data.b?.[0]?.[0];
                if (bestBid) { processNewPriceUpdate('bybit', parseFloat(bestBid)); }
            }
        } catch (e) {/* Mute parsing errors */}
    });
    bybitWsClient.on('error', () => {});
    bybitWsClient.on('close', () => {
        bybitWsClient = null;
        setTimeout(connectToBybit, RECONNECT_INTERVAL_MS);
    });
}

function connectToOkx() {
    const OKX_SUBSCRIBE_MSG = JSON.stringify({ op: "subscribe", args: [{ channel: "bbo-tbt", instId: OKX_SYMBOL }] });
    okxWsClient = new WebSocket(OKX_STREAM_URL);
    okxWsClient.on('open', () => {
        latestOkxBid = null;
        last_sent_avg_price = null;
        okxWsClient.send(OKX_SUBSCRIBE_MSG);
    });
    okxWsClient.on('message', (data) => {
        const messageString = data.toString();
        if (messageString === 'ping') {
            if (okxWsClient.readyState === WebSocket.OPEN) {
                okxWsClient.send('pong');
            }
            return;
        }
        try {
            const message = JSON.parse(messageString);
            if (message.arg?.channel === 'bbo-tbt' && message.data?.[0]) {
                const bestBid = message.data[0].bids?.[0]?.[0];
                if (bestBid) { processNewPriceUpdate('okx', parseFloat(bestBid)); }
            }
        } catch (e) {/* Mute parsing errors */}
    });
    okxWsClient.on('error', () => {});
    okxWsClient.on('close', () => {
        okxWsClient = null;
        setTimeout(connectToOkx, RECONNECT_INTERVAL_MS);
    });
}

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
                    console.error(`[Listener] KuCoin Error: Failed to get a valid token. Response: ${data}`);
                    setTimeout(connectToKucoin, RECONNECT_INTERVAL_MS);
                    return;
                }
                const { token, instanceServers } = response.data;
                const endpoint = instanceServers[0].endpoint;
                const pingInterval = instanceServers[0].pingInterval;
                establishKucoinWsConnection(`${endpoint}?token=${token}`, pingInterval);
            } catch (e) {
                console.error(`[Listener] KuCoin Error: Error parsing token JSON response. Details: ${e.message}`);
                setTimeout(connectToKucoin, RECONNECT_INTERVAL_MS);
            }
        });
    });
    req.on('error', (e) => {
        console.error(`[Listener] KuCoin Error: HTTPS request for token failed. Details: ${e.message}`);
        setTimeout(connectToKucoin, RECONNECT_INTERVAL_MS);
    });
    req.write(postData);
    req.end();
}

function establishKucoinWsConnection(url, pingInterval) {
    if (kucoinWsClient) { kucoinWsClient.terminate(); }
    if (kucoinHeartbeatInterval) { clearInterval(kucoinHeartbeatInterval); }

    kucoinWsClient = new WebSocket(url);

    kucoinWsClient.on('open', () => {
        last_sent_kucoin_bid = null;
        const topic = `/spotMarket/level1:${KUCOIN_SYMBOL}`;
        const subscribeMsg = { id: Date.now(), type: 'subscribe', topic: topic, privateChannel: false, response: true };
        kucoinWsClient.send(JSON.stringify(subscribeMsg));

        kucoinHeartbeatInterval = setInterval(() => {
            if (kucoinWsClient.readyState === WebSocket.OPEN) {
                kucoinWsClient.send(JSON.stringify({ id: Date.now(), type: 'ping' }));
            }
        }, pingInterval - 2000);
    });

    kucoinWsClient.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());
            if (message.subject === 'level1' && message.data && message.data.bids) {
                const bestBidPrice = message.data.bids[0];
                if (bestBidPrice) {
                    processKucoinPrice(parseFloat(bestBidPrice));
                }
            }
        } catch (e) {
             console.error(`[Listener] KuCoin Error: Could not parse message from server. Details: ${e.message}`);
        }
    });

    kucoinWsClient.on('error', (err) => {
        console.error(`[Listener] KuCoin Error: WebSocket connection error. Details: ${err.message}`);
    });

    kucoinWsClient.on('close', (code) => {
        console.warn(`[Listener] KuCoin Info: WebSocket connection closed with code ${code}. Reconnecting...`);
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

console.log(`[Listener] PID: ${process.pid} --- Service started. Connecting to exchanges...`);
// --- END OF FILE binance_listener_optimized.js (Corrected) ---
