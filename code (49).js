// --- START OF FILE coinswitch_listener.js ---
// Final Version: Logging is reduced to critical events and errors only.

const { io } = require("socket.io-client");
const WebSocket = require('ws');

// --- Critical Error Handlers ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION | Origin: ${origin}`, err.stack || err);
    cleanupAndExit(1);
});
process.on('unhandledRejection', (reason, promise) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNHANDLED REJECTION`, reason instanceof Error ? reason.stack : reason);
    cleanupAndExit(1);
});

function cleanupAndExit(exitCode = 1) {
    console.log(`[Listener] PID: ${process.pid} --- Initiating cleanup...`);
    const clientsToTerminate = [internalWsClient, coinswitchSocket];
    clientsToTerminate.forEach(client => {
        if (client && typeof client.disconnect === 'function') {
            try { client.disconnect(); } catch (e) { console.error(`[Listener] Error disconnecting Socket.IO client: ${e.message}`); }
        } else if (client && typeof client.terminate === 'function') {
            try { client.terminate(); } catch (e) { console.error(`[Listener] Error terminating a WebSocket client: ${e.message}`); }
        }
    });
    setTimeout(() => {
        console.log(`[Listener] PID: ${process.pid} --- Exiting with code ${exitCode}.`);
        process.exit(exitCode);
    }, 1000).unref();
}

// --- Listener Configuration ---
const SYMBOL = 'BTCUSDT';
const internalReceiverUrl = 'ws://localhost:8082';
const RECONNECT_INTERVAL_MS = 5000;
const PRICE_CHANGE_THRESHOLD = 1.0;

// --- CoinSwitch Specific Configuration ---
const COINSWITCH_BASE_URL = 'wss://ws.coinswitch.co/';
const COINSWITCH_PARAMETER = 'exchange_2';
const COINSWITCH_NAMESPACE = `/${COINSWITCH_PARAMETER}`;
const COINSWITCH_HANDSHAKE_PATH = `/pro/realtime-rates-socket/futures/${COINSWITCH_PARAMETER}`;

// --- Listener State Variables ---
let coinswitchSocket = null;
let internalWsClient = null;
let last_sent_bid_price = null;

// --- Internal Receiver Connection (Silent on success) ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) return;
    internalWsClient = new WebSocket(internalReceiverUrl);
    internalWsClient.on('error', (err) => {
        console.error(`[Listener] PID: ${process.pid} --- Internal receiver WebSocket error: ${err.message}`);
    });
    internalWsClient.on('close', () => {
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
}

// --- Data Extraction & Processing ---
function extractCoinSwitchOrderbookData(data) {
    try {
        if (data && data.data && data.data.bids && data.data.bids.length > 0) {
            const payload = data.data;
            return {
                type: 'orderbook_update',
                price_bid: parseFloat(payload.bids[0][0]),
                symbol: payload.symbol,
            };
        }
        return null;
    } catch (error) {
        console.error(`[Listener] Error parsing CoinSwitch data: ${error.message}. Data:`, JSON.stringify(data));
        return null;
    }
}

function processOrderbookUpdate(parsedData) {
    if (!parsedData || parsedData.type !== 'orderbook_update') return;
    const current_bid_price = parsedData.price_bid;
    const current_symbol = parsedData.symbol;

    if (current_bid_price > 0) {
        let shouldSend = false;
        if (last_sent_bid_price === null || last_sent_bid_price.symbol !== current_symbol) {
            shouldSend = true;
        } else {
            const priceDifference = Math.abs(current_bid_price - last_sent_bid_price.price);
            if (priceDifference >= PRICE_CHANGE_THRESHOLD) {
                shouldSend = true;
            }
        }
        if (shouldSend) {
            const pricePayload = { k: "P", p: current_bid_price, SS: current_symbol };
            sendToInternalClient(pricePayload);
            last_sent_bid_price = { price: current_bid_price, symbol: current_symbol };
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

// --- CoinSwitch Stream Connection ---
function connectToCoinSwitchStream() {
    if (coinswitchSocket && coinswitchSocket.connected) return;
    if (coinswitchSocket) coinswitchSocket.disconnect();
    
    const connectionUrl = COINSWITCH_BASE_URL + COINSWITCH_NAMESPACE;
    coinswitchSocket = io(connectionUrl, {
        path: COINSWITCH_HANDSHAKE_PATH,
        transports: ['websocket'],
        reconnection: false,
        timeout: 10000,
    });

    coinswitchSocket.on('connect', () => {
        console.log(`[Listener] ✅ CoinSwitch connected successfully! Socket ID: ${coinswitchSocket.id}`);
        last_sent_bid_price = null;
        coinswitchSocket.emit("FETCH_ORDER_BOOK_CS_PRO", { 'event': 'subscribe', 'pair': SYMBOL });
    });

    coinswitchSocket.on('ORDER_BOOK_CS_PRO', (data) => {
        const parsedData = extractCoinSwitchOrderbookData(data);
        if (parsedData) processOrderbookUpdate(parsedData);
    });

    coinswitchSocket.on('connect_error', (err) => {
        console.error(`[Listener] ❌ CoinSwitch connection error: ${err.message}`);
    });

    coinswitchSocket.on('disconnect', (reason) => {
        console.log(`[Listener] CoinSwitch disconnected. Reason: ${reason}. Reconnecting in ${RECONNECT_INTERVAL_MS / 1000}s...`);
        coinswitchSocket.close();
        coinswitchSocket = null;
        setTimeout(connectToCoinSwitchStream, RECONNECT_INTERVAL_MS);
    });
}

// --- Start ---
connectToInternalReceiver();
connectToCoinSwitchStream();
console.log(`[Listener] PID: ${process.pid} --- CoinSwitch Listener started for ${SYMBOL}. Filtering on >= ${PRICE_CHANGE_THRESHOLD} price change.`);
// --- END OF FILE coinswitch_listener.js ---