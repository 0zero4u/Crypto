const WebSocket = require('ws');

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

// --- State Management ---
function cleanupAndExit(exitCode = 1) {
    // Terminate both WebSocket clients
    const clientsToTerminate = [internalWsClient, okxWsClient];
    clientsToTerminate.forEach(client => {
        if (client && typeof client.terminate === 'function') {
            try { client.terminate(); } catch (e) { console.error(`[Listener] Error terminating a WebSocket client: ${e.message}`); }
        }
    });
    setTimeout(() => { process.exit(exitCode); }, 1000).unref();
}

// --- Listener Configuration ---
const OKX_SYMBOL = 'BTC-USDT';
const RECONNECT_INTERVAL_MS = 5000;
const PRICE_CHANGE_THRESHOLD = 0.01;
const internalReceiverUrl = 'ws://localhost:8082'; // Restored internal client URL

// --- Exchange Stream URL ---
const OKX_STREAM_URL = 'wss://ws.okx.com:8443/ws/v5/public';

// --- Listener State Variables ---
let internalWsClient = null; // Restored internal client variable
let okxWsClient = null;
let last_sent_price = null;

// --- Internal Receiver Connection (Restored) ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) return;
    console.error(`[Internal] Attempting to connect to receiver at ${internalReceiverUrl}`);
    internalWsClient = new WebSocket(internalReceiverUrl);

    internalWsClient.on('open', () => {
        console.error('[Internal] Connection to receiver established.');
    });

    internalWsClient.on('error', (err) => {
        console.error(`[Internal] WebSocket error: ${err.message}`);
        // The close event will handle reconnecting
    });

    internalWsClient.on('close', () => {
        console.error('[Internal] Connection to receiver closed. Reconnecting...');
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
}

// --- Data Forwarding to Internal Client (Restored) ---
function sendToInternalClient(payload) {
    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        try {
            const dataToSend = JSON.stringify(payload);
            console.error(`[OKX Listener] Forwarding to internal client: ${dataToSend}`);
            internalWsClient.send(dataToSend);
        } catch (e) {
            console.error(`[Internal] Failed to send message: ${e.message}`);
        }
    } else {
        console.error('[Internal] Cannot send message, receiver not connected.');
    }
}

// --- OKX Data Processing ---
function processOkxPrice(current_price) {
    let shouldSend = false;

    if (last_sent_price === null) {
        shouldSend = true;
    } else {
        const price_difference = Math.abs(current_price - last_sent_price);
        if (price_difference >= PRICE_CHANGE_THRESHOLD) {
            shouldSend = true;
        }
    }
    
    if (shouldSend) {
        const pricePayload = { p: current_price };
        sendToInternalClient(pricePayload); // Send to internal client, not console.log
        last_sent_price = current_price;
    }
}

// --- OKX Exchange Connection Function ---
function connectToOkx() {
    const OKX_SUBSCRIBE_MSG = JSON.stringify({ op: "subscribe", args: [{ channel: "bbo-tbt", instId: OKX_SYMBOL }] });
    okxWsClient = new WebSocket(OKX_STREAM_URL);

    okxWsClient.on('open', () => {
        console.error("[OKX Listener] WebSocket connection opened. Subscribing to ticker...");
        last_sent_price = null;
        okxWsClient.send(OKX_SUBSCRIBE_MSG);
    });

    okxWsClient.on('message', (data) => {
        const messageString = data.toString();

        if (messageString === 'ping') {
            if (okxWsClient.readyState === WebSocket.OPEN) okxWsClient.send('pong');
            return;
        }
        try {
            const message = JSON.parse(messageString);
            if (message.event === 'subscribe') {
                 console.error(`[OKX Listener] Successfully subscribed to ${message.arg.channel}`);
                 return;
            }
            if (message.arg?.channel === 'bbo-tbt' && message.data?.[0]) {
                const bestBid = message.data[0].bids?.[0]?.[0];
                if (bestBid) {
                    processOkxPrice(parseFloat(bestBid));
                }
            }
        } catch (e) { /* Silently ignore non-JSON messages */ }
    });

    okxWsClient.on('error', (err) => {
        console.error('[OKX Listener] WebSocket connection error:', err.message);
    });

    okxWsClient.on('close', (code, reason) => {
        console.error(`[OKX Listener] WebSocket connection closed. Code: ${code}, Reason: ${reason}. Reconnecting...`);
        okxWsClient = null;
        setTimeout(connectToOkx, RECONNECT_INTERVAL_MS);
    });
}

// --- Start all connections ---
console.error(`[OKX Listener] Starting script with price change threshold: ${PRICE_CHANGE_THRESHOLD}`);
connectToInternalReceiver(); // Start connecting to the internal receiver
connectToOkx();              // Start connecting to the exchange