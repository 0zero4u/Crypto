// --- START OF MODIFIED FILE gateio_listener.js ---

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
    const clientsToTerminate = [internalWsClient, gateIoWsClient];
    clientsToTerminate.forEach(client => {
        if (client && typeof client.terminate === 'function') {
            try { client.terminate(); } catch (e) { console.error(`[Listener] Error terminating a WebSocket client: ${e.message}`); } // Keep: Error
        }
    });

    const intervalsToClear = [gateIoPingIntervalId];
    intervalsToClear.forEach(intervalId => {
        if (intervalId) { try { clearInterval(intervalId); } catch(e) { /* ignore */ } }
    });

    setTimeout(() => {
        console.log(`[Listener] PID: ${process.pid} --- Exiting with code ${exitCode}.`); // Keep: Critical shutdown info
        process.exit(exitCode);
    }, 1000).unref();
}

// --- Listener Configuration ---
const SYMBOL_GATEIO = 'BTC_USDT';
const GATEIO_STREAM_URL = 'wss://api.gateio.ws/ws/v4/';
const internalReceiverUrl = 'ws://localhost:8082';
const RECONNECT_INTERVAL_MS = 5000;
const GATEIO_PING_INTERVAL_MS = 25 * 1000;
const FILTER_THRESHOLD = 1.0;

// --- Listener State Variables ---
let gateIoWsClient = null;
let internalWsClient = null;
let gateIoPingIntervalId = null;
let last_sent_best_bid_price = null;

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
function extractGateIoBookTickerData(messageString) {
    try {
        const message = JSON.parse(messageString);

        if (message.channel) {
            if (message.channel === 'spot.pong') {
                return { type: 'pong' };
            }
            if (message.event === 'subscribe' && message.channel === 'spot.book_ticker') {
                if (message.result && message.result.status === 'success') {
                    // console.log(`[Listener] PID: ${process.pid} --- Gate.io subscribed to: ${JSON.stringify(message.channel)} payload: ${JSON.stringify(message.payload)}`); // REMOVED: Less critical for normal ops
                } else {
                    console.error(`[Listener] PID: ${process.pid} --- Gate.io subscription error: ${JSON.stringify(message.error || message)} (Channel: ${message.channel})`); // Keep: Error
                }
                return { type: 'event', eventData: message };
            }
            // if (message.event === 'unsubscribe' && message.channel === 'spot.book_ticker') { // REMOVED: Less critical
            //      console.log(`[Listener] PID: ${process.pid} --- Gate.io unsubscribed from: ${JSON.stringify(message.channel)}`);
            //      return { type: 'event', eventData: message };
            // }
        }

        if (message.channel === 'spot.book_ticker' && message.event === 'update' && message.result) {
            const tickerData = message.result;
            if (tickerData.s === SYMBOL_GATEIO && tickerData.b !== undefined) {
                return {
                    type: 'book_ticker_update',
                    best_bid: parseFloat(tickerData.b),
                    symbol: tickerData.s,
                    event_time: parseInt(tickerData.t, 10) || Date.now()
                };
            }
        }
        // console.log(`[Listener] PID: ${process.pid} --- Gate.io unhandled message structure: ${messageString.substring(0,100)}`); // REMOVED: Debug only
        return null;
    } catch (error) {
        console.error(`[Listener] Error parsing Gate.io JSON: ${error.message}. Data segment: ${messageString.substring(0,70)}...`); // Keep: Error
        return null;
    }
}

// --- Signal Processing ---
function processGateIoBookTickerUpdate(parsedData) {
    if (!parsedData || parsedData.type !== 'book_ticker_update') return;

    const current_data = parsedData;

    if (current_data.best_bid > 0) {
        if (last_sent_best_bid_price === null || Math.abs(current_data.best_bid - last_sent_best_bid_price) >= FILTER_THRESHOLD) {
            const pricePayload = {
                k: "P",
                p: current_data.best_bid,
                SS: current_data.symbol
            };
            sendToInternalClient(pricePayload);
            last_sent_best_bid_price = current_data.best_bid;
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

// --- Gate.io Stream Connection ---
function connectToGateIoStream() {
    if (gateIoWsClient && (gateIoWsClient.readyState === WebSocket.OPEN || gateIoWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    gateIoWsClient = new WebSocket(GATEIO_STREAM_URL);

    gateIoWsClient.on('open', function open() {
        last_sent_best_bid_price = null;

        const subscriptionMsg = {
            time: Math.floor(Date.now() / 1000),
            channel: "spot.book_ticker",
            event: "subscribe",
            payload: [SYMBOL_GATEIO]
        };
        try {
            gateIoWsClient.send(JSON.stringify(subscriptionMsg));
        } catch (e) {
            console.error(`[Listener] PID: ${process.pid} --- Error sending Gate.io subscription: ${e.message}`); // Keep: Error
        }

        if (gateIoPingIntervalId) clearInterval(gateIoPingIntervalId);
        gateIoPingIntervalId = setInterval(() => {
            if (gateIoWsClient && gateIoWsClient.readyState === WebSocket.OPEN) {
                try {
                    gateIoWsClient.send(JSON.stringify({
                        time: Math.floor(Date.now() / 1000),
                        channel: "spot.ping"
                    }));
                } catch (pingError) {
                    // console.error(`[Listener] PID: ${process.pid} --- Error sending ping to Gate.io: ${pingError.message}`); // REMOVED: Potentially too verbose
                }
            }
        }, GATEIO_PING_INTERVAL_MS);
    });

    gateIoWsClient.on('message', function incoming(data) {
        try {
            const messageString = data.toString();
            const parsedData = extractGateIoBookTickerData(messageString);
            if (parsedData && parsedData.type === 'book_ticker_update') {
                processGateIoBookTickerUpdate(parsedData);
            }
        } catch (e) {
            console.error(`[Listener] PID: ${process.pid} --- CRITICAL ERROR in Gate.io message handler: ${e.message}`, e.stack); // Keep: Critical Error
        }
    });

    gateIoWsClient.on('error', function error(err) {
        console.error(`[Listener] PID: ${process.pid} --- Gate.io WebSocket error: ${err.message}`); // Keep: Error
    });

    gateIoWsClient.on('close', function close(code, reason) {
        if (gateIoPingIntervalId) { clearInterval(gateIoPingIntervalId); gateIoPingIntervalId = null; }
        gateIoWsClient = null;
        // console.log(`[Listener] PID: ${process.pid} --- Gate.io WebSocket closed. Code: ${code}, Reason: ${reason ? reason.toString() : 'N/A'}. Reconnecting...`); // REMOVED: Less critical for normal ops
        setTimeout(connectToGateIoStream, RECONNECT_INTERVAL_MS);
    });
}

// --- Start the connections ---
connectToInternalReceiver();
connectToGateIoStream();

// Essential startup log
console.log(`[Listener] PID: ${process.pid} --- Gate.io Book Ticker Listener started for ${SYMBOL_GATEIO} (Best Bid). Filter Threshold: ${FILTER_THRESHOLD}`); // Keep: Critical Startup Info
// --- END OF MODIFIED FILE gateio_listener.js ---