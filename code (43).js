// --- START OF MODIFIED FILE huobi_listener.js ---

const WebSocket = require('ws');
const pako = require('pako'); // For GZIP decompression

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
    console.log(`[Listener] PID: ${process.pid} --- Initiating cleanup...`);
    const clientsToTerminate = [internalWsClient, exchangeWsClient];
    clientsToTerminate.forEach(client => {
        if (client && typeof client.terminate === 'function') {
            try { client.terminate(); } catch (e) { console.error(`[Listener] Error terminating a WebSocket client: ${e.message}`); }
        }
    });

    const intervalsToClear = [exchangePingIntervalId];
    intervalsToClear.forEach(intervalId => {
        if (intervalId) { try { clearInterval(intervalId); } catch(e) { /* ignore */ } }
    });

    setTimeout(() => {
        console.log(`[Listener] PID: ${process.pid} --- Exiting with code ${exitCode}.`);
        process.exit(exitCode);
    }, 1000).unref();
}

// --- Listener Configuration ---
const SYMBOL_PAIR = 'BTCUSDT'; // Huobi Instrument ID (e.g., btcusdt, ethusdt - typically lowercase for topics)
const HUOBI_STREAM_URL = 'wss://api.huobi.pro/ws'; // Huobi WebSocket API
const internalReceiverUrl = 'ws://localhost:8082';
const RECONNECT_INTERVAL_MS = 5000;
const HUOBI_CLIENT_PING_INTERVAL_MS = 4 * 1000; // Huobi requires client ping if no data sent in 5s. Be proactive.
const SUBSCRIPTION_ID = "my_unique_bbo_request_001";

// --- Listener State Variables ---
let exchangeWsClient = null;
let internalWsClient = null;
let exchangePingIntervalId = null;
let last_sent_bid_price = null;

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    internalWsClient = new WebSocket(internalReceiverUrl);
    internalWsClient.on('error', (err) => {
        console.error(`[Listener] PID: ${process.pid} --- Internal receiver WebSocket error: ${err.message}`);
    });
    internalWsClient.on('close', (code, reason) => {
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
}

// --- Data Extraction Functions ---
function extractHuobiBboData(messageString) {
    try {
        const message = JSON.parse(messageString);

        // Handle Huobi's server-initiated PING
        if (message.ping) {
            // console.log(`[Listener] PID: ${process.pid} --- Huobi Server PING received: ${message.ping}`); // Optional
            return { type: 'server_ping', ts: message.ping };
        }

        // Handle Huobi's PONG (response to our client ping)
        if (message.pong) {
            // console.log(`[Listener] PID: ${process.pid} --- Huobi Client PONG received: ${message.pong}`); // Optional
            return { type: 'pong_response' };
        }

        // Handle Subscription confirmations
        if (message.id === SUBSCRIPTION_ID && message.status === 'ok') {
            console.log(`[Listener] PID: ${process.pid} --- Huobi subscribed to: ${message.subbed}`);
            return { type: 'event', eventData: message };
        }
        if (message.id === SUBSCRIPTION_ID && message.status === 'error') {
            console.error(`[Listener] PID: ${process.pid} --- Huobi subscription error: ${message["err-msg"]} (Code: ${message["err-code"]})`);
            return { type: 'event', eventData: message };
        }

        // Handle BBO data update
        // Example: {"ch":"market.btcusdt.bbo","ts":1690413396005,"tick":{"symbol":"btcusdt","bid":25803.50,"bidSize":0.1,"ask":25803.60,"askSize":0.2, ...}}
        if (message.ch && message.ch === `market.${SYMBOL_PAIR.toLowerCase()}.bbo` && message.tick && message.tick.bid !== undefined) {
            const bboData = message.tick;
            return {
                type: 'bbo_update',
                price_bid: parseFloat(bboData.bid), // Best bid price
                symbol: bboData.symbol.toUpperCase(), // Standardize to uppercase like Bybit example
                event_time: parseInt(message.ts, 10) || Date.now()
            };
        }
        // console.log(`[Listener] PID: ${process.pid} --- Huobi unhandled message structure: ${messageString.substring(0,100)}`);
        return null;
    } catch (error) {
        console.error(`[Listener] Error parsing Huobi JSON: ${error.message}. Data segment: ${messageString.substring(0,70)}...`);
        return null;
    }
}

// --- Signal Processing ---
function processHuobiBboUpdate(parsedData) {
    if (!parsedData || parsedData.type !== 'bbo_update') return;

    const current_bbo_data = parsedData;

    if (current_bbo_data.price_bid > 0) { // Ensure best bid price is valid
        // Send if it's the first price or if the best bid price has changed
        if (last_sent_bid_price === null || current_bbo_data.price_bid !== last_sent_bid_price) {
            const pricePayload = {
                k: "P", // Key for "Price type" (e.g. Price)
                p: current_bbo_data.price_bid, // Key for "Price" (using best bid price)
                SS: current_bbo_data.symbol    // Key for "Symbol Source" (Symbol from Source)
            };
            sendToInternalClient(pricePayload);
            last_sent_bid_price = current_bbo_data.price_bid;
        }
    }
}

function sendToInternalClient(payload) {
    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        try {
            internalWsClient.send(JSON.stringify(payload));
        } catch (sendError) {
            console.error(`[Listener] PID: ${process.pid} --- Error sending data to internal receiver: ${sendError.message}`);
        }
    }
}

// --- Huobi Stream Connection ---
function connectToHuobiStream() {
    if (exchangeWsClient && (exchangeWsClient.readyState === WebSocket.OPEN || exchangeWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    exchangeWsClient = new WebSocket(HUOBI_STREAM_URL);

    exchangeWsClient.on('open', function open() {
        last_sent_bid_price = null; // Reset last sent price on new connection

        const subscriptionMsg = {
            sub: `market.${SYMBOL_PAIR.toLowerCase()}.bbo`,
            id: SUBSCRIPTION_ID
        };
        try {
            exchangeWsClient.send(JSON.stringify(subscriptionMsg));
        } catch (e) {
            console.error(`[Listener] PID: ${process.pid} --- Error sending Huobi subscription: ${e.message}`);
        }

        if (exchangePingIntervalId) clearInterval(exchangePingIntervalId);
        // Client-initiated PING to keep connection alive
        exchangePingIntervalId = setInterval(() => {
            if (exchangeWsClient && exchangeWsClient.readyState === WebSocket.OPEN) {
                try {
                    exchangeWsClient.send(JSON.stringify({ ping: Date.now() }));
                } catch (pingError) {
                    // console.error(`[Listener] PID: ${process.pid} --- Error sending ping to Huobi: ${pingError.message}`); // Potentially verbose
                }
            }
        }, HUOBI_CLIENT_PING_INTERVAL_MS);
    });

    exchangeWsClient.on('message', function incoming(data) {
        try {
            // Data from Huobi is GZIP compressed, so we need to decompress it first
            const decompressedData = pako.inflate(data, { to: 'string' });
            const parsedData = extractHuobiBboData(decompressedData);

            if (parsedData) {
                if (parsedData.type === 'server_ping') {
                    // Respond to server's PING with a PONG
                    if (exchangeWsClient && exchangeWsClient.readyState === WebSocket.OPEN) {
                        try {
                            exchangeWsClient.send(JSON.stringify({ pong: parsedData.ts }));
                        } catch (pongError) {
                            console.error(`[Listener] PID: ${process.pid} --- Error sending PONG to Huobi: ${pongError.message}`);
                        }
                    }
                } else if (parsedData.type === 'bbo_update') {
                    processHuobiBboUpdate(parsedData);
                }
                // Other message types like 'pong_response' or 'event' are handled or logged within extractHuobiBboData
            }
        } catch (e) {
            if (e instanceof RangeError && e.message.includes("incorrect data check")) {
                 console.warn(`[Listener] PID: ${process.pid} --- pako decompression error (likely not GZIP data or corrupted): ${e.message}. Raw data sample: ${data.slice(0,30).toString('hex')}`);
            } else {
                 console.error(`[Listener] PID: ${process.pid} --- CRITICAL ERROR in Huobi message handler: ${e.message}`, e.stack);
            }
        }
    });

    exchangeWsClient.on('error', function error(err) {
        console.error(`[Listener] PID: ${process.pid} --- Huobi WebSocket error: ${err.message}`);
    });

    exchangeWsClient.on('close', function close(code, reason) {
        if (exchangePingIntervalId) { clearInterval(exchangePingIntervalId); exchangePingIntervalId = null; }
        exchangeWsClient = null;
        // console.log(`[Listener] PID: ${process.pid} --- Huobi WebSocket closed. Code: ${code}, Reason: ${reason ? reason.toString() : 'N/A'}. Reconnecting...`);
        setTimeout(connectToHuobiStream, RECONNECT_INTERVAL_MS);
    });
}

// --- Start the connections ---
connectToInternalReceiver();
connectToHuobiStream();

console.log(`[Listener] PID: ${process.pid} --- Huobi BBO Listener started for ${SYMBOL_PAIR} (Best Bid Price).`);
// --- END OF MODIFIED FILE huobi_listener.js ---