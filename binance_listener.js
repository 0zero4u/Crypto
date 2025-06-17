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
    if (okxWsClient && typeof okxWsClient.terminate === 'function') {
        try { okxWsClient.terminate(); } catch (e) { console.error(`[Listener] Error terminating WebSocket client: ${e.message}`); }
    }
    setTimeout(() => { process.exit(exitCode); }, 1000).unref();
}

// --- Listener Configuration ---
const OKX_SYMBOL = 'BTC-USDT';
const RECONNECT_INTERVAL_MS = 5000;

// --- Exchange Stream URL ---
const OKX_STREAM_URL = 'wss://ws.okx.com:8443/ws/v5/public';

// --- Listener State Variables ---
let okxWsClient = null;
let latest_okx_bid = null;

// --- Data Processing ---
function processOkxPrice(price) {
    // Forward to client if the best bid price has changed.
    if (latest_okx_bid !== price) {
        latest_okx_bid = price;

        const pricePayload = { p: price };
        console.log(JSON.stringify(pricePayload)); // "Send" to client by logging
    }
}

// --- Exchange Connection Function ---

function connectToOkx() {
    const OKX_SUBSCRIBE_MSG = JSON.stringify({ op: "subscribe", args: [{ channel: "bbo-tbt", instId: OKX_SYMBOL }] });
    okxWsClient = new WebSocket(OKX_STREAM_URL);

    okxWsClient.on('open', () => {
        console.error("[OKX Listener] Connection opened. Subscribing to ticker..."); // Use console.error for logs
        latest_okx_bid = null;
        okxWsClient.send(OKX_SUBSCRIBE_MSG);
    });

    okxWsClient.on('message', (data) => {
        const messageString = data.toString();
        // Respond to heartbeat pings
        if (messageString === 'ping') {
            if (okxWsClient.readyState === WebSocket.OPEN) {
                okxWsClient.send('pong');
            }
            return;
        }
        try {
            const message = JSON.parse(messageString);
            // Check for valid price data
            if (message.arg?.channel === 'bbo-tbt' && message.data?.[0]) {
                const bestBid = message.data[0].bids?.[0]?.[0];
                if (bestBid) {
                    processOkxPrice(parseFloat(bestBid));
                }
            } else if (message.event === 'subscribe') {
                 console.error(`[OKX Listener] Successfully subscribed to ${message.arg.channel}`);
            }
        } catch (e) {
            // Silently ignore parsing errors for non-data messages
        }
    });

    okxWsClient.on('error', (err) => {
        console.error('[OKX Listener] WebSocket error:', err.message);
    });

    okxWsClient.on('close', () => {
        console.error('[OKX Listener] Connection closed. Reconnecting...'); // Use console.error for logs
        okxWsClient = null;
        setTimeout(connectToOkx, RECONNECT_INTERVAL_MS);
    });
}

// --- Start connection ---
console.error('[OKX Listener] Starting...'); // Use console.error for logs
connectToOkx();
