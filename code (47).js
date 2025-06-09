// --- START OF FILE bybit_listener.js ---

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
    const clientsToTerminate = [internalWsClient, bybitWsClient];
    clientsToTerminate.forEach(client => {
        if (client && typeof client.terminate === 'function') {
            try { client.terminate(); } catch (e) { console.error(`[Listener] Error terminating a WebSocket client: ${e.message}`); } // Keep: Error
        }
    });

    const intervalsToClear = [pingIntervalId];
    intervalsToClear.forEach(intervalId => {
        if (intervalId) { try { clearInterval(intervalId); } catch(e) { /* ignore */ } }
    });

    setTimeout(() => {
        console.log(`[Listener] PID: ${process.pid} --- Exiting with code ${exitCode}.`); // Keep: Critical shutdown info
        process.exit(exitCode);
    }, 1000).unref();
}

// --- Listener Configuration ---
const SYMBOL = 'BTCUSDT';
const BYBIT_STREAM_URL = 'wss://stream.bybit.com/v5/public/linear';
const internalReceiverUrl = 'ws://localhost:8082';
const RECONNECT_INTERVAL_MS = 5000;
const PRICE_CHANGE_THRESHOLD = 1.0;
const BYBIT_PING_INTERVAL_MS = 20000; // Bybit requires a ping every 20 seconds

// --- Listener State Variables ---
let bybitWsClient = null;
let internalWsClient = null;
let last_sent_bid_price = null;
let pingIntervalId = null;

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
function extractBybitOrderbookData(messageString) {
    try {
        const message = JSON.parse(messageString);
        // Check for actual orderbook data (snapshot or delta)
        if (message.topic && message.topic.startsWith('orderbook') && message.data && message.data.b && message.data.b.length > 0) {
            return {
                type: 'orderbook_update',
                price_bid: parseFloat(message.data.b[0][0]), // Best bid price is the first entry
                symbol: message.data.s,
                event_time: message.ts
            };
        }
        return null;
    } catch (error) {
        console.error(`[Listener] Error parsing Bybit JSON: ${error.message}. Data segment: ${messageString.substring(0,70)}...`); // Keep: Error
        return null;
    }
}

// --- Signal Processing ---
function processOrderbookUpdate(parsedData) {
    if (!parsedData || parsedData.type !== 'orderbook_update') return;

    const current_bid_price = parsedData.price_bid;

    if (current_bid_price > 0) {
        let shouldSend = false;
        if (last_sent_bid_price === null) {
            shouldSend = true;
        } else {
            const priceDifference = Math.abs(current_bid_price - last_sent_bid_price);
            if (priceDifference >= PRICE_CHANGE_THRESHOLD) {
                shouldSend = true;
            }
        }
        
        if (shouldSend) {
            const pricePayload = {
                k: "P",
                p: current_bid_price,
                SS: parsedData.symbol
            };
            sendToInternalClient(pricePayload);
            last_sent_bid_price = current_bid_price;
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

// --- Bybit Stream Connection ---
function connectToBybitStream() {
    if (bybitWsClient && (bybitWsClient.readyState === WebSocket.OPEN || bybitWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    bybitWsClient = new WebSocket(BYBIT_STREAM_URL);

    bybitWsClient.on('open', function open() {
        const subscription = {
            "op": "subscribe",
            "args": [`orderbook.1.${SYMBOL}`]
        };
        bybitWsClient.send(JSON.stringify(subscription));

        if (pingIntervalId) clearInterval(pingIntervalId);
        pingIntervalId = setInterval(() => {
            if (bybitWsClient.readyState === WebSocket.OPEN) {
                bybitWsClient.send(JSON.stringify({ op: "ping" }));
            }
        }, BYBIT_PING_INTERVAL_MS);

        last_sent_bid_price = null;
    });

    bybitWsClient.on('message', function incoming(data) {
        try {
            const messageString = data.toString();
            if (messageString.includes('"op":"pong"')) {
                return;
            }
            const parsedData = extractBybitOrderbookData(messageString);
            if (parsedData) {
                // Process the data immediately upon arrival
                processOrderbookUpdate(parsedData);
            }
        } catch (e) {
            console.error(`[Listener] PID: ${process.pid} --- CRITICAL ERROR in Bybit message handler: ${e.message}`, e.stack); // Keep: Critical Error
        }
    });

    bybitWsClient.on('error', function error(err) {
        console.error(`[Listener] PID: ${process.pid} --- Bybit WebSocket error: ${err.message}`); // Keep: Error
    });

    bybitWsClient.on('close', function close(code, reason) {
        if (pingIntervalId) { clearInterval(pingIntervalId); pingIntervalId = null; }
        bybitWsClient = null;
        setTimeout(connectToBybitStream, RECONNECT_INTERVAL_MS);
    });
}

// --- Start the connections ---
connectToInternalReceiver();
connectToBybitStream();

// Essential startup log
console.log(`[Listener] PID: ${process.pid} --- Bybit Listener started for ${SYMBOL} (Best Bid). Filtering on >= ${PRICE_CHANGE_THRESHOLD} price change.`); // Keep: Critical Startup Info
// --- END OF FILE bybit_listener.js ---