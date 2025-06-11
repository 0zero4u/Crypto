// --- START OF FILE delta_listener.js ---

const WebSocket = require('ws');

// --- Global Error Handlers (Critical Only) ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION | ${err.message}`);
    cleanupAndExit(1);
});
process.on('unhandledRejection', (reason, promise) => {
    const reasonMsg = reason instanceof Error ? reason.message : String(reason);
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNHANDLED REJECTION | ${reasonMsg}`);
    cleanupAndExit(1);
});

function cleanupAndExit(exitCode = 1) {
    console.log(`[Listener] PID: ${process.pid} --- Initiating cleanup and exit...`); // Critical shutdown log
    const clientsToTerminate = [internalWsClient, deltaWsClient];
    clientsToTerminate.forEach(client => {
        if (client && typeof client.terminate === 'function') {
            try { client.terminate(); } catch (e) { /* Silent on purpose */ }
        }
    });

    const timers = [pingIntervalId, pongTimeoutId];
    timers.forEach(timer => {
        if (timer) { try { clearInterval(timer); clearTimeout(timer); } catch(e) { /* ignore */ } }
    });

    setTimeout(() => {
        console.log(`[Listener] PID: ${process.pid} --- Exiting with code ${exitCode}.`); // Critical shutdown log
        process.exit(exitCode);
    }, 1000).unref();
}

// --- Listener Configuration ---
const SYMBOL = 'BTCUSD';
const DELTA_STREAM_URL = 'wss://socket.india.delta.exchange';
const internalReceiverUrl = 'ws://localhost:8082';
const RECONNECT_INTERVAL_MS = 5000;
const PRICE_CHANGE_THRESHOLD = 1.0;
const PING_INTERVAL_MS = 30000;
const PONG_TIMEOUT_MS = 5000;

// --- Listener State Variables ---
let deltaWsClient = null;
let internalWsClient = null;
let last_sent_price = null;
let pingIntervalId = null;
let pongTimeoutId = null;
let isPongReceived = true;

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    internalWsClient = new WebSocket(internalReceiverUrl);
    // Errors are suppressed. The app will simply fail to send data silently.
    internalWsClient.on('error', (err) => { /* Silent */ });
    internalWsClient.on('close', (code, reason) => {
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
}

// --- Signal Processing ---
function processTradeUpdate(tradeData) {
    if (!tradeData || !tradeData.price) return;

    const current_price = tradeData.price;

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
            const pricePayload = { k: "P", p: current_price, SS: tradeData.symbol };
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
            // Suppress send errors to reduce log noise.
        }
    }
}

// --- Delta Exchange Stream Connection ---
function connectToDeltaStream() {
    if (deltaWsClient && (deltaWsClient.readyState === WebSocket.OPEN || deltaWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    deltaWsClient = new WebSocket(DELTA_STREAM_URL);

    deltaWsClient.on('open', function open() {
        deltaWsClient.send(JSON.stringify({ "type": "subscribe", "payload": { "channels": [{ "name": "all_trades", "symbols": [SYMBOL] }] } }));
        deltaWsClient.send(JSON.stringify({ "type": "enable_heartbeat" }));

        if (pingIntervalId) clearInterval(pingIntervalId);
        if (pongTimeoutId) clearTimeout(pongTimeoutId);
        isPongReceived = true;

        pingIntervalId = setInterval(() => {
            if (deltaWsClient.readyState === WebSocket.OPEN) {
                isPongReceived = false;
                deltaWsClient.send(JSON.stringify({ type: "ping" }));
                
                pongTimeoutId = setTimeout(() => {
                    if (!isPongReceived) {
                        // Pong timeout is a connection error. Terminate silently to trigger reconnect.
                        if (deltaWsClient) deltaWsClient.terminate();
                    }
                }, PONG_TIMEOUT_MS);
            }
        }, PING_INTERVAL_MS);

        last_sent_price = null;
    });

    deltaWsClient.on('message', function incoming(data) {
        try {
            const message = JSON.parse(data.toString());

            if (message.type === 'pong') {
                isPongReceived = true;
                if (pongTimeoutId) clearTimeout(pongTimeoutId);
                return;
            }
            if (message.type === 'heartbeat' || message.type === 'subscription_success') {
                return; // Ignore acknowledged control messages
            }

            if (message.type === 'all_trades_snapshot' && Array.isArray(message.trades)) {
                if (message.trades.length > 0) {
                     const latestTrade = message.trades[message.trades.length - 1];
                     processTradeUpdate({ price: parseFloat(latestTrade.price), symbol: message.symbol });
                }
            } else if (message.type === 'all_trades' && message.price) {
                processTradeUpdate({ price: parseFloat(message.price), symbol: message.symbol });
            }
        } catch (e) {
            // This is a critical data processing error. Keep it.
            console.error(`[Listener] PID: ${process.pid} --- CRITICAL ERROR in message handler: ${e.message}`);
        }
    });

    // Suppress general WebSocket errors. 'close' event will handle reconnection.
    deltaWsClient.on('error', function error(err) { /* Silent */ });

    deltaWsClient.on('close', function close(code, reason) {
        if (pingIntervalId) { clearInterval(pingIntervalId); pingIntervalId = null; }
        if (pongTimeoutId) { clearTimeout(pongTimeoutId); pongTimeoutId = null; }
        deltaWsClient = null;
        setTimeout(connectToDeltaStream, RECONNECT_INTERVAL_MS);
    });
}

// --- Start the connections ---
connectToInternalReceiver();
connectToDeltaStream();

// Critical startup log
console.log(`[Listener] PID: ${process.pid} --- Started: ${SYMBOL} | Threshold: ${PRICE_CHANGE_THRESHOLD}`);
// --- END OF FILE delta_listener.js ---
