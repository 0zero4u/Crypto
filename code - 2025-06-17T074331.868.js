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
const PRICE_CHANGE_THRESHOLD = 0.01; // The minimum price change required to send an update

// --- Exchange Stream URL ---
const OKX_STREAM_URL = 'wss://ws.okx.com:8443/ws/v5/public';

// --- Listener State Variables ---
let okxWsClient = null;
let last_sent_price = null; // Stores the last price that was actually sent to the client

// --- Data Processing ---
function processOkxPrice(current_price) {
    let shouldSend = false;

    // Case 1: This is the very first price tick after connecting. Always send it.
    if (last_sent_price === null) {
        shouldSend = true;
        console.error(`[OKX DEBUG] First price received: ${current_price}. Sending initial update.`);
    } else {
        // Case 2: Compare the new price with the last *sent* price.
        const price_difference = Math.abs(current_price - last_sent_price);
        if (price_difference >= PRICE_CHANGE_THRESHOLD) {
            shouldSend = true;
            console.error(`[OKX DEBUG] Price change threshold met. Old: ${last_sent_price}, New: ${current_price}, Diff: ${price_difference.toFixed(4)}. Sending update.`);
        } else {
            // Optional: Log when a price is received but ignored.
            // console.error(`[OKX DEBUG] Price change ${price_difference.toFixed(4)} did not meet threshold. No update sent.`);
        }
    }
    
    if (shouldSend) {
        const pricePayload = { p: current_price };
        // This is the actual data output for the client
        console.log(JSON.stringify(pricePayload));
        last_sent_price = current_price; // IMPORTANT: Update the last sent price
    }
}

// --- Exchange Connection Function ---

function connectToOkx() {
    const OKX_SUBSCRIBE_MSG = JSON.stringify({ op: "subscribe", args: [{ channel: "bbo-tbt", instId: OKX_SYMBOL }] });
    okxWsClient = new WebSocket(OKX_STREAM_URL);

    okxWsClient.on('open', () => {
        console.error("[OKX Listener] WebSocket connection opened. Subscribing to ticker...");
        last_sent_price = null; // Reset on new connection
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

            if (message.event === 'subscribe' && message.arg?.channel === 'bbo-tbt') {
                 console.error(`[OKX Listener] Successfully subscribed to channel: ${message.arg.channel}`);
                 return;
            }

            if (message.arg?.channel === 'bbo-tbt' && message.data?.[0]) {
                const bestBid = message.data[0].bids?.[0]?.[0];
                if (bestBid) {
                    processOkxPrice(parseFloat(bestBid));
                }
            }
        } catch (e) {
            // Silently ignore non-JSON messages or parsing errors
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
console.error(`[OKX Listener] Starting script with price change threshold: ${PRICE_CHANGE_THRESHOLD}`);
connectToOkx();