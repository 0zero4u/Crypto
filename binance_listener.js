// --- START OF MODIFIED FILE bybit_listener.js ---

const WebSocket = require('ws');

// --- Global Error Handlers ---
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

function cleanupAndExit(exitCode = 1) {
    console.log(`[Listener] PID: ${process.pid} --- Initiating cleanup...`); // Keep: Critical shutdown info
    const clientsToTerminate = [internalWsClient, bybitWsClient]; // MODIFIED
    clientsToTerminate.forEach(client => {
        if (client && typeof client.terminate === 'function') {
            try { client.terminate(); } catch (e) { console.error(`[Listener] Error terminating a WebSocket client: ${e.message}`); } // Keep: Error
        }
    });

    const intervalsToClear = [bybitPingIntervalId]; // MODIFIED
    intervalsToClear.forEach(intervalId => {
        if (intervalId) { try { clearInterval(intervalId); } catch(e) { /* ignore */ } }
    });

    setTimeout(() => {
        console.log(`[Listener] PID: ${process.pid} --- Exiting with code ${exitCode}.`); // Keep: Critical shutdown info
        process.exit(exitCode);
    }, 1000).unref();
}

// --- Listener Configuration ---
const SYMBOL = 'BTCUSDT'; // Bybit Instrument ID (e.g., BTCUSDT, ETHUSDT)
const BYBIT_STREAM_URL = 'wss://stream.bybit.com/v5/public/spot'; // For Spot. Use /v5/public/linear for USDT perpetuals
const internalReceiverUrl = 'ws://localhost:8082';
const RECONNECT_INTERVAL_MS = 5000;
const BYBIT_PING_INTERVAL_MS = 18 * 1000; // Bybit recommends pinging every 20s if no other messages. Be proactive.

// --- Tunable Parameters ---
const BID_PRICE_FILTER_THRESHOLD = 1.0;

// --- Listener State Variables ---
let bybitWsClient = null; // MODIFIED
let internalWsClient = null;
let bybitPingIntervalId = null; // MODIFIED
let last_sent_filtered_best_bid_price = null;

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    internalWsClient = new WebSocket(internalReceiverUrl);
    internalWsClient.on('error', (err) => {
        console.error(`[Listener] PID: ${process.pid} --- Internal receiver WebSocket error: ${err.message}`); // Keep: Error
    });
    internalWsClient.on('close', (code, reason) => {
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
}

// --- Data Extraction Functions ---
function extractBybitOrderbookData(messageString) { // MODIFIED function name
    try {
        const message = JSON.parse(messageString);

        // Handle Ping/Pong and Subscription confirmations
        if (message.op) {
            if (message.op === 'pong') {
                // console.log(`[Listener] PID: ${process.pid} --- Bybit Pong received: ${JSON.stringify(message)}`); // Optional: verbose
                return { type: 'pong' };
            }
            if (message.op === 'subscribe') {
                if (message.success) {
                    console.log(`[Listener] PID: ${process.pid} --- Bybit subscribed to: ${JSON.stringify(message.req_id || message.args)}`); // Keep: Info
                } else {
                    console.error(`[Listener] PID: ${process.pid} --- Bybit subscription error: ${message.ret_msg} (Args: ${JSON.stringify(message.req_id || message.args)})`); // Keep: Error
                }
                return { type: 'event', eventData: message };
            }
             if (message.op === 'auth' && message.success) {
                console.log(`[Listener] PID: ${process.pid} --- Bybit Auth successful.`); // Keep: Info
                return { type: 'event', eventData: message };
            }
        }

        // Handle orderbook data
        // Example: {"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1672304484468,"data":{"s":"BTCUSDT","b":[["20000.00","1.5"]],"a":[["20001.00","2.0"]],"u":12345,"seq":1}}
        if (message.topic && message.topic === `orderbook.1.${SYMBOL}` && message.data) {
            const orderbook = message.data;
            if (orderbook.s === SYMBOL && orderbook.b && orderbook.b.length > 0 && orderbook.b[0].length > 0) {
                return {
                    type: 'orderbook_update', // MODIFIED type
                    price_bid: parseFloat(orderbook.b[0][0]), // Best bid price
                    symbol: orderbook.s,
                    event_time: parseInt(message.ts, 10) || Date.now()
                };
            }
        }
        // console.log(`[Listener] PID: ${process.pid} --- Bybit unhandled message structure: ${messageString.substring(0,100)}`); // Optional: for debugging unhandled valid JSON
        return null;
    } catch (error) {
        console.error(`[Listener] Error parsing Bybit JSON: ${error.message}. Data segment: ${messageString.substring(0,70)}...`); // Keep: Error
        return null;
    }
}

// --- Signal Processing ---
function processBybitOrderbookUpdate(parsedData) { // MODIFIED function name
    if (!parsedData || parsedData.type !== 'orderbook_update') return; // MODIFIED type check

    const current_bybit_data = parsedData;

    if (current_bybit_data.price_bid > 0) {
        if (last_sent_filtered_best_bid_price === null ||
            Math.abs(current_bybit_data.price_bid - last_sent_filtered_best_bid_price) >= BID_PRICE_FILTER_THRESHOLD) {
            
            const bidPricePayload = {
                k: "P", // Key for "Price type" (e.g. Price)
                p: current_bybit_data.price_bid, // Key for "Price"
                SS: current_bybit_data.symbol    // Key for "Symbol Source" (Symbol from Source)
            };
            sendToInternalClient(bidPricePayload);
            last_sent_filtered_best_bid_price = current_bybit_data.price_bid;
        }
    }
}

function sendToInternalClient(payload) {
    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        try {
            internalWsClient.send(JSON.stringify(payload));
        } catch (sendError) {
            console.error(`[Listener] PID: ${process.pid} --- Error sending data to internal receiver: ${sendError.message}`); // Keep: Error
        }
    }
}

// --- Bybit Stream Connection --- // MODIFIED section name
function connectToBybitStream() { // MODIFIED function name
    if (bybitWsClient && (bybitWsClient.readyState === WebSocket.OPEN || bybitWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    bybitWsClient = new WebSocket(BYBIT_STREAM_URL); // MODIFIED

    bybitWsClient.on('open', function open() {
        last_sent_filtered_best_bid_price = null;

        const subscriptionMsg = {
            op: "subscribe",
            args: [`orderbook.1.${SYMBOL}`] // MODIFIED subscription message
        };
        try {
            bybitWsClient.send(JSON.stringify(subscriptionMsg));
        } catch (e) {
            console.error(`[Listener] PID: ${process.pid} --- Error sending Bybit subscription: ${e.message}`); // Keep: Error
        }

        if (bybitPingIntervalId) clearInterval(bybitPingIntervalId); // MODIFIED
        bybitPingIntervalId = setInterval(() => { // MODIFIED
            if (bybitWsClient && bybitWsClient.readyState === WebSocket.OPEN) {
                try {
                    bybitWsClient.send(JSON.stringify({ op: "ping" })); // MODIFIED Bybit ping
                } catch (pingError) {
                    // console.error(`[Listener] PID: ${process.pid} --- Error sending ping to Bybit: ${pingError.message}`); // Potentially too verbose
                }
            }
        }, BYBIT_PING_INTERVAL_MS); // MODIFIED
    });

    bybitWsClient.on('message', function incoming(data) {
        try {
            const messageString = data.toString();
            const parsedData = extractBybitOrderbookData(messageString); // MODIFIED
            if (parsedData && parsedData.type === 'orderbook_update') { // MODIFIED type check
                processBybitOrderbookUpdate(parsedData); // MODIFIED
            }
        } catch (e) {
            console.error(`[Listener] PID: ${process.pid} --- CRITICAL ERROR in Bybit message handler: ${e.message}`, e.stack); // Keep: Critical Error // MODIFIED
        }
    });

    bybitWsClient.on('error', function error(err) {
        console.error(`[Listener] PID: ${process.pid} --- Bybit WebSocket error: ${err.message}`); // Keep: Error // MODIFIED
    });

    bybitWsClient.on('close', function close(code, reason) {
        if (bybitPingIntervalId) { clearInterval(bybitPingIntervalId); bybitPingIntervalId = null; } // MODIFIED
        bybitWsClient = null; // MODIFIED
        // console.log(`[Listener] PID: ${process.pid} --- Bybit WebSocket closed. Code: ${code}, Reason: ${reason ? reason.toString() : 'N/A'}. Reconnecting...`); // Optional: more detail on close
        setTimeout(connectToBybitStream, RECONNECT_INTERVAL_MS); // MODIFIED
    });
}

// --- Start the connections ---
connectToInternalReceiver();
connectToBybitStream(); // MODIFIED

// Essential startup log
console.log(`[Listener] PID: ${process.pid} --- Bybit Orderbook Listener started for ${SYMBOL} (Depth 1). Filter: ${BID_PRICE_FILTER_THRESHOLD}.`); // Keep: Critical Startup Info // MODIFIED
// --- END OF MODIFIED FILE bybit_listener.js ---
