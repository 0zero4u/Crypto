
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
    const clientsToTerminate = [internalWsClient, okxWsClient];
    clientsToTerminate.forEach(client => {
        if (client && typeof client.terminate === 'function') {
            try { client.terminate(); } catch (e) { console.error(`[Listener] Error terminating a WebSocket client: ${e.message}`); } // Keep: Error
        }
    });

    const intervalsToClear = [okxPingIntervalId];
    intervalsToClear.forEach(intervalId => {
        if (intervalId) { try { clearInterval(intervalId); } catch(e) { /* ignore */ } }
    });

    setTimeout(() => {
        console.log(`[Listener] PID: ${process.pid} --- Exiting with code ${exitCode}.`); // Keep: Critical shutdown info
        process.exit(exitCode);
    }, 1000).unref();
}


// --- Listener Configuration ---
const SYMBOL = 'BTC-USDT'; // OKX Instrument ID
const okxStreamUrl = 'wss://ws.okx.com:8443/ws/v5/public';
const internalReceiverUrl = 'ws://localhost:8082';
const RECONNECT_INTERVAL_MS = 5000;
const OKX_PING_INTERVAL_MS = 25 * 1000;

// --- Tunable Parameters ---
const BID_PRICE_FILTER_THRESHOLD = 1.0;

// --- Listener State Variables ---
let okxWsClient = null;
let internalWsClient = null;
let okxPingIntervalId = null;
let last_sent_filtered_best_bid_price = null;

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    internalWsClient = new WebSocket(internalReceiverUrl);
    // internalWsClient.on('open', () => {}); // REMOVED: Verbose
    internalWsClient.on('error', (err) => {
        console.error(`[Listener] PID: ${process.pid} --- Internal receiver WebSocket error: ${err.message}`); // Keep: Error
    });
    internalWsClient.on('close', (code, reason) => {
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
}

// --- Data Extraction Functions ---
function extractOkxTickerData(messageString) {
    try {
        const message = JSON.parse(messageString);

        if (typeof messageString === 'string' && messageString.toLowerCase() === 'pong') {
            return { type: 'pong' }; // No log for pong
        }

        if (message.event) {
            if (message.event === 'subscribe') {
                console.log(`[Listener] PID: ${process.pid} --- OKX subscribed to: ${JSON.stringify(message.arg)}`); // Keep: Info
            } else if (message.event === 'error') {
                console.error(`[Listener] PID: ${process.pid} --- OKX API error: ${message.msg} (Code: ${message.code}) Channel: ${message.arg ? JSON.stringify(message.arg) : 'N/A'}`); // Keep: Error
            }
            return { type: 'event', eventData: message };
        }

        if (message.arg && message.arg.channel === 'tickers' && message.data && message.data.length > 0) {
            const ticker = message.data[0];
            if (ticker && typeof ticker.bidPx === 'string' && ticker.instId === SYMBOL) {
                return {
                    type: 'ticker',
                    price_bid: parseFloat(ticker.bidPx),
                    symbol: ticker.instId,
                    event_time: parseInt(ticker.ts, 10) || Date.now()
                };
            }
        }
        return null; // No log for unhandled/invalid structure here, rely on outer try-catch if parsing fails
    } catch (error) {
        console.error(`[Listener] Error parsing OKX JSON: ${error.message}. Data segment: ${messageString.substring(0,70)}...`); // Keep: Error
        return null;
    }
}

// --- Signal Processing ---
function processOkxTickerUpdate(parsedData) {
    if (!parsedData || parsedData.type !== 'ticker') return;

    const curr_okx_ticker = parsedData;

    if (curr_okx_ticker.price_bid > 0) {
        if (last_sent_filtered_best_bid_price === null ||
            Math.abs(curr_okx_ticker.price_bid - last_sent_filtered_best_bid_price) >= BID_PRICE_FILTER_THRESHOLD) {
            
            const bidPricePayload = {
                k: "P",
                p: curr_okx_ticker.price_bid,
                SS: curr_okx_ticker.symbol
            };
            sendToInternalClient(bidPricePayload);
            last_sent_filtered_best_bid_price = curr_okx_ticker.price_bid;
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
    // else { /* No log if internal client not open, it will try to reconnect */ }
}

// --- OKX Stream Connection ---
function connectToOkxStream() {
    if (okxWsClient && (okxWsClient.readyState === WebSocket.OPEN || okxWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    okxWsClient = new WebSocket(okxStreamUrl);

    okxWsClient.on('open', function open() {
        last_sent_filtered_best_bid_price = null;

        const subscriptionMsg = {
            op: "subscribe",
            args: [ { channel: "tickers", instId: SYMBOL } ]
        };
        try {
            okxWsClient.send(JSON.stringify(subscriptionMsg));
        } catch (e) {
            console.error(`[Listener] PID: ${process.pid} --- Error sending OKX subscription: ${e.message}`); // Keep: Error
        }

        if (okxPingIntervalId) clearInterval(okxPingIntervalId);
        okxPingIntervalId = setInterval(() => {
            if (okxWsClient && okxWsClient.readyState === WebSocket.OPEN) {
                try {
                    okxWsClient.send('ping');
                } catch (pingError) {
                    // console.error(`[Listener] PID: ${process.pid} --- Error sending ping to OKX: ${pingError.message}`); // Potentially too verbose if transient
                }
            }
        }, OKX_PING_INTERVAL_MS);
    });

    okxWsClient.on('message', function incoming(data) {
        try {
            const messageString = data.toString();
            const parsedData = extractOkxTickerData(messageString);
            if (parsedData && parsedData.type === 'ticker') {
                processOkxTickerUpdate(parsedData);
            }
        } catch (e) {
            console.error(`[Listener] PID: ${process.pid} --- CRITICAL ERROR in OKX message handler: ${e.message}`, e.stack); // Keep: Critical Error
        }
    });

    okxWsClient.on('error', function error(err) {
        console.error(`[Listener] PID: ${process.pid} --- OKX WebSocket error: ${err.message}`); // Keep: Error
    });

    okxWsClient.on('close', function close(code, reason) {
        if (okxPingIntervalId) { clearInterval(okxPingIntervalId); okxPingIntervalId = null; }
        okxWsClient = null;
        setTimeout(connectToOkxStream, RECONNECT_INTERVAL_MS);
    });
}

// --- Start the connections ---
connectToInternalReceiver();
connectToOkxStream();

// Essential startup log
console.log(`[Listener] PID: ${process.pid} --- OKX Ticker Listener started for ${SYMBOL}. Filter: ${BID_PRICE_FILTER_THRESHOLD}.`); // Keep: Critical Startup Info
