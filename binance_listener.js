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
    const clientsToTerminate = [internalWsClient, bybitWsClient];
    clientsToTerminate.forEach(client => {
        if (client && typeof client.terminate === 'function') {
            try { client.terminate(); } catch (e) { console.error(`[Listener] Error terminating a WebSocket client: ${e.message}`); } // Keep: Error
        }
    });

    const intervalsToClear = [bybitPingIntervalId];
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
const BYBIT_STREAM_URL = 'wss://stream.bybit.com/v5/public/linear'; // For USDT perpetuals (tickers stream)
const internalReceiverUrl = 'ws://localhost:8082';
const RECONNECT_INTERVAL_MS = 5000;
const BYBIT_PING_INTERVAL_MS = 18 * 1000; // Bybit recommends pinging every 20s if no other messages. Be proactive.

// --- Listener State Variables ---
let bybitWsClient = null;
let internalWsClient = null;
let bybitPingIntervalId = null;
let last_sent_mark_price = null; // MODIFIED: To store the last sent mark price

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
function extractBybitTickerData(messageString) { // MODIFIED function name
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

        // Handle ticker data
        // Example: {"topic":"tickers.BTCUSDT","type":"snapshot","ts":1690413396005,"data":{"symbol":"BTCUSDT", "markPrice":"25803.50", ...}}
        if (message.topic && message.topic === `tickers.${SYMBOL}` && message.data && message.data.markPrice !== undefined) {
            const tickerData = message.data;
            return {
                type: 'ticker_update', // MODIFIED type
                price_mark: parseFloat(tickerData.markPrice), // Mark price
                symbol: tickerData.symbol,
                event_time: parseInt(message.ts, 10) || Date.now()
            };
        }
        // console.log(`[Listener] PID: ${process.pid} --- Bybit unhandled message structure: ${messageString.substring(0,100)}`); // Optional: for debugging unhandled valid JSON
        return null;
    } catch (error) {
        console.error(`[Listener] Error parsing Bybit JSON: ${error.message}. Data segment: ${messageString.substring(0,70)}...`); // Keep: Error
        return null;
    }
}

// --- Signal Processing ---
function processBybitTickerUpdate(parsedData) { // MODIFIED function name
    if (!parsedData || parsedData.type !== 'ticker_update') return; // MODIFIED type check

    const current_ticker_data = parsedData;

    if (current_ticker_data.price_mark > 0) { // Ensure mark price is valid
        // Send if it's the first price or if the mark price has changed
        if (last_sent_mark_price === null || current_ticker_data.price_mark !== last_sent_mark_price) {
            
            const pricePayload = {
                k: "P", // Key for "Price type" (e.g. Price)
                p: current_ticker_data.price_mark, // Key for "Price" (using mark price)
                SS: current_ticker_data.symbol    // Key for "Symbol Source" (Symbol from Source)
            };
            sendToInternalClient(pricePayload);
            last_sent_mark_price = current_ticker_data.price_mark;
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
        last_sent_mark_price = null; // Reset last sent price on new connection

        const subscriptionMsg = {
            op: "subscribe",
            args: [`tickers.${SYMBOL}`] // MODIFIED subscription message for tickers
        };
        try {
            bybitWsClient.send(JSON.stringify(subscriptionMsg));
        } catch (e) {
            console.error(`[Listener] PID: ${process.pid} --- Error sending Bybit subscription: ${e.message}`); // Keep: Error
        }

        if (bybitPingIntervalId) clearInterval(bybitPingIntervalId);
        bybitPingIntervalId = setInterval(() => {
            if (bybitWsClient && bybitWsClient.readyState === WebSocket.OPEN) {
                try {
                    bybitWsClient.send(JSON.stringify({ op: "ping" })); // Bybit ping
                } catch (pingError) {
                    // console.error(`[Listener] PID: ${process.pid} --- Error sending ping to Bybit: ${pingError.message}`); // Potentially too verbose
                }
            }
        }, BYBIT_PING_INTERVAL_MS);
    });

    bybitWsClient.on('message', function incoming(data) {
        try {
            const messageString = data.toString();
            const parsedData = extractBybitTickerData(messageString); // MODIFIED to use new extraction function
            if (parsedData && parsedData.type === 'ticker_update') { // MODIFIED type check
                processBybitTickerUpdate(parsedData); // MODIFIED to use new processing function
            }
        } catch (e) {
            console.error(`[Listener] PID: ${process.pid} --- CRITICAL ERROR in Bybit message handler: ${e.message}`, e.stack); // Keep: Critical Error
        }
    });

    bybitWsClient.on('error', function error(err) {
        console.error(`[Listener] PID: ${process.pid} --- Bybit WebSocket error: ${err.message}`); // Keep: Error
    });

    bybitWsClient.on('close', function close(code, reason) {
        if (bybitPingIntervalId) { clearInterval(bybitPingIntervalId); bybitPingIntervalId = null; }
        bybitWsClient = null;
        // console.log(`[Listener] PID: ${process.pid} --- Bybit WebSocket closed. Code: ${code}, Reason: ${reason ? reason.toString() : 'N/A'}. Reconnecting...`); // Optional: more detail on close
        setTimeout(connectToBybitStream, RECONNECT_INTERVAL_MS);
    });
}

// --- Start the connections ---
connectToInternalReceiver();
connectToBybitStream();

// Essential startup log
console.log(`[Listener] PID: ${process.pid} --- Bybit Ticker Listener started for ${SYMBOL} (Mark Price).`); // Keep: Critical Startup Info // MODIFIED
// --- END OF MODIFIED FILE bybit_listener.js ---
