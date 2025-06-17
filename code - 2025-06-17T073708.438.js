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
    console.error(`[OKX DEBUG] Processing price: ${price}. Last sent price: ${latest_okx_bid}`);
    // Forward to client if the best bid price has changed.
    if (latest_okx_bid !== price) {
        console.error(`[OKX DEBUG] Price has changed. Old: ${latest_okx_bid}, New: ${price}. Sending update.`);
        latest_okx_bid = price;

        const pricePayload = { p: price };
        // This is the actual data output for the client
        console.log(JSON.stringify(pricePayload));
    } else {
        console.error(`[OKX DEBUG] Price is unchanged. No update sent.`);
    }
}

// --- Exchange Connection Function ---

function connectToOkx() {
    const OKX_SUBSCRIBE_MSG = JSON.stringify({ op: "subscribe", args: [{ channel: "bbo-tbt", instId: OKX_SYMBOL }] });
    okxWsClient = new WebSocket(OKX_STREAM_URL);

    okxWsClient.on('open', () => {
        console.error("[OKX Listener] WebSocket connection opened. Subscribing to ticker...");
        latest_okx_bid = null;
        okxWsClient.send(OKX_SUBSCRIBE_MSG);
    });

    okxWsClient.on('message', (data) => {
        const messageString = data.toString();
        console.error('[OKX DEBUG] RAW MESSAGE RECEIVED:', messageString);

        // Respond to heartbeat pings from the server
        if (messageString === 'ping') {
            if (okxWsClient.readyState === WebSocket.OPEN) {
                console.error('[OKX DEBUG] Received ping, sending pong.');
                okxWsClient.send('pong');
            }
            return;
        }
        try {
            const message = JSON.parse(messageString);

            // Check for subscription success message
            if (message.event === 'subscribe' && message.arg?.channel === 'bbo-tbt') {
                 console.error(`[OKX Listener] Successfully subscribed to channel: ${message.arg.channel}`);
                 return;
            }

            // Check for valid price data
            if (message.arg?.channel === 'bbo-tbt' && message.data?.[0]) {
                const bestBid = message.data[0].bids?.[0]?.[0];
                console.error(`[OKX DEBUG] Extracted best bid string: "${bestBid}"`);

                if (bestBid) {
                    processOkxPrice(parseFloat(bestBid));
                } else {
                    console.error('[OKX DEBUG] Message is a bbo-tbt update, but best bid was not found in data.');
                }
            }
        } catch (e) {
            console.error('[OKX Listener] Error parsing JSON message from server:', e.message);
        }
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

// --- Start connection ---
console.error('[OKX Listener] Starting script...');
connectToOkx();