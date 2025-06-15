// --- START OF FILE kucoin_only_listener_quiet.js ---

const WebSocket = require('ws');
const https = require('https');

// --- Global Error Handlers (Essential for fatal errors) ---
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
            try { client.terminate(); } catch (e) { /* silent exit */ }
        }
    });
    setTimeout(() => { process.exit(exitCode); }, 1000).unref();
}

// --- Listener Configuration ---
const KUCOIN_SYMBOL = 'BTC-USDT';
const RECONNECT_INTERVAL_MS = 5000;
const PRICE_CHANGE_THRESHOLD = 0.1;
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
    // Case 1: This is the very first price update.
    // Just send the current price, as there is no "last price" to send yet.
    if (last_sent_price === null) {
        sendToInternalClient({ p: price });
        last_sent_price = price;
        return; // Exit after sending the initial price
    }

    // Case 2: We have a previous price, so we can calculate the difference.
    const priceDifference = Math.abs(price - last_sent_price);

    // Check if the change is significant enough to trigger an update.
    if (priceDifference >= PRICE_CHANGE_THRESHOLD) {
        // First, send the last known price to show where the jump started.
        sendToInternalClient({ p: last_sent_price });

        // Immediately after, send the new price that caused the jump.
        sendToInternalClient({ p: price });

        // IMPORTANT: Update the last_sent_price to the new price for the next comparison.
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
                    // SILENT: Failed to get token, will retry.
                    setTimeout(connectToKucoin, RECONNECT_INTERVAL_MS);
                    return;
                }
                const { token, instanceServers } = response.data;
                const endpoint = instanceServers[0].endpoint;
                const pingInterval = instanceServers[0].pingInterval;
                establishKucoinWsConnection(`${endpoint}?token=${token}`, pingInterval);
            } catch (e) {
                // SILENT: Error parsing JSON, will retry.
                setTimeout(connectToKucoin, RECONNECT_INTERVAL_MS);
            }
        });
    });
    req.on('error', (e) => {
        // SILENT: HTTPS request failed, will retry.
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
        }, pingInterval - 2000); // Send ping slightly before interval expires
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
             // This log is kept as it indicates a data format issue from the exchange, not a connection issue.
             console.error(`[Listener] KuCoin Error: Could not parse message from server. Details: ${e.message}`);
        }
    });

    // SILENT: The 'close' event will handle reconnection. No need to log redundant errors.
    kucoinWsClient.on('error', () => {});

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