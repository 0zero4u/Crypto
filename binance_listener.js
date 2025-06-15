// --- START OF FILE kucoin_only_listener_quiet.js ---

const WebSocket = require('ws');
const https = require('https');

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
    const clientsToTerminate = [internalWsClient, kucoinWsClient];
    clientsToTerminate.forEach(client => {
        if (client && typeof client.terminate === 'function') {
            try { client.terminate(); } catch (e) { console.error(`[Listener] Error terminating a WebSocket client: ${e.message}`); }
        }
    });
    setTimeout(() => { process.exit(exitCode); }, 1000).unref();
}

// --- Listener Configuration ---
const KUCOIN_SYMBOL = 'BTC-USDT';
const RECONNECT_INTERVAL_MS = 5000;
const PRICE_CHANGE_THRESHOLD = 0.5;
const internalReceiverUrl = 'ws://localhost:8082';

// --- Listener State Variables ---
let internalWsClient = null, kucoinWsClient = null;
let last_sent_price = null;

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
function processPriceUpdate(price) {
    let shouldSend = false;
    if (last_sent_price === null) {
        shouldSend = true;
    } else {
        const priceDifference = Math.abs(price - last_sent_price);
        if (priceDifference >= PRICE_CHANGE_THRESHOLD) {
            shouldSend = true;
        }
    }
    if (shouldSend) {
        const pricePayload = { p: price };
        sendToInternalClient(pricePayload);
        last_sent_price = price;
    }
}

function sendToInternalClient(payload) {
    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        try {
            internalWsClient.send(JSON.stringify(payload));
        } catch (e) { /* Silent: Errors will be caught by the receiver or on next send attempt */ }
    }
}

// --- KuCoin Connection Functions ---
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
        last_sent_price = null;
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
                    processPriceUpdate(parseFloat(bestBidPrice));
                }
            }
        } catch (e) {
             console.error(`[Listener] KuCoin Error: Could not parse message from server. Details: ${e.message}`);
        }
    });

    kucoinWsClient.on('error', (err) => {
        console.error(`[Listener] KuCoin Error: WebSocket connection error. Details: ${err.message}`);
    });

    kucoinWsClient.on('close', () => {
        kucoinWsClient = null;
        if (kucoinHeartbeatInterval) clearInterval(kucoinHeartbeatInterval);
        setTimeout(connectToKucoin, RECONNECT_INTERVAL_MS);
    });
}

// --- Start all connections ---
connectToInternalReceiver();
connectToKucoin();
// --- END OF FILE kucoin_only_listener_quiet.js ---
