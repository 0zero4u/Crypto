// --- START OF FILE binance_listener.js ---

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
    // Non-error logs are removed for silent operation.
    const clientsToTerminate = [internalWsClient, binanceWsClient];
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
// UPDATED: Changed to Binance Spot trade stream URL
const BINANCE_STREAM_URL = `wss://stream.binance.com:9443/ws/${SYMBOL.toLowerCase()}@trade`;
const internalReceiverUrl = 'ws://localhost:8082';
const RECONNECT_INTERVAL_MS = 5000;
const PRICE_CHANGE_THRESHOLD = 1.0;

// --- Listener State Variables ---
let binanceWsClient = null;
let internalWsClient = null;
// RENAMED: from last_sent_bid_price to last_sent_price for clarity
let last_sent_price = null;

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    internalWsClient = new WebSocket(internalReceiverUrl);
    internalWsClient.on('error', (err) => {
        console.error(`[Listener] Internal receiver WebSocket error: ${err.message}`);
    });
    internalWsClient.on('close', () => {
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
}

// --- Data Extraction Functions ---
// RENAMED & REWRITTEN: To parse trade data instead of book ticker data
function extractBinanceTradeData(messageString) {
    try {
        const message = JSON.parse(messageString);
        // Trade stream payload has 's' for symbol and 'p' for price
        if (message.s && message.p) {
            return {
                price: parseFloat(message.p)
            };
        }
        return null;
    } catch (error) {
        console.error(`[Listener] Error parsing Binance JSON: ${error.message}. Data: ${messageString.substring(0,70)}...`);
        return null;
    }
}

// --- Signal Processing ---
// RENAMED & UPDATED: To handle trade data
function processBinanceTradeUpdate(parsedData) {
    if (!parsedData) return;

    const current_price = parsedData.price;

    if (current_price > 0) {
        let shouldSend = false;
        if (last_sent_price === null) {
            shouldSend = true;
        } else {
            const priceDifference = Math.abs(current_price - last_sent_price);
            if (priceDifference >= PRICE_CHANGE_THRESHOLD) {
                shouldSend = true;
            }
        }
        
        if (shouldSend) {
            const pricePayload = {
                p: current_price
            };
            sendToInternalClient(pricePayload);
            last_sent_price = current_price;
        }
    }
}

function sendToInternalClient(payload) {
    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        try {
            internalWsClient.send(JSON.stringify(payload));
        } catch (sendError) {
            console.error(`[Listener] Error sending data to internal receiver: ${sendError.message}`);
        }
    }
}

// --- Binance Stream Connection ---
function connectToBinanceStream() {
    if (binanceWsClient && (binanceWsClient.readyState === WebSocket.OPEN || binanceWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    // UPDATED: Using the new URL constant
    binanceWsClient = new WebSocket(BINANCE_STREAM_URL);

    binanceWsClient.on('open', function open() {
        // UPDATED: Resetting the correct state variable
        last_sent_price = null;
    });

    binanceWsClient.on('message', function incoming(data) {
        try {
            const messageString = data.toString();
            // UPDATED: Calling the new extraction and processing functions
            const parsedData = extractBinanceTradeData(messageString);
            if (parsedData) {
                processBinanceTradeUpdate(parsedData);
            }
        } catch (e) {
            console.error(`[Listener] CRITICAL ERROR in Binance message handler: ${e.message}`, e.stack);
        }
    });

    binanceWsClient.on('error', function error(err) {
        console.error(`[Listener] Binance WebSocket error: ${err.message}`);
    });

    binanceWsClient.on('close', function close() {
        binanceWsClient = null;
        setTimeout(connectToBinanceStream, RECONNECT_INTERVAL_MS);
    });
}

// --- Start the connections ---
connectToInternalReceiver();
connectToBinanceStream();

// --- No startup log for silent operation ---
// --- END OF FILE binance_listener.js ---