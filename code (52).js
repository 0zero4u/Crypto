// --- START OF MODIFIED coindcx_listener.js ---

// Use socket.io-client for CoinDCX and 'ws' for the internal connection
const io = require('socket.io-client');
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
    if (pingIntervalId) clearInterval(pingIntervalId); // Stop the ping on exit
    const clientsToTerminate = [internalWsClient, coindcxSocket];
    clientsToTerminate.forEach(client => {
        if (client && typeof client.disconnect === 'function') {
            try { client.disconnect(); } catch (e) { console.error(`[Listener] Error disconnecting socket.io client: ${e.message}`); }
        } else if (client && typeof client.terminate === 'function') {
            try { client.terminate(); } catch (e) { console.error(`[Listener] Error terminating a WebSocket client: ${e.message}`); }
        }
    });

    setTimeout(() => {
        process.exit(exitCode);
    }, 1000).unref();
}

// --- Listener Configuration ---
const COINDCX_STREAM_URL = 'wss://stream.coindcx.com';
const COINDCX_TRADE_CHANNEL = 'B-BTC_USDT@trades';
const internalReceiverUrl = 'ws://localhost:8082';
const RECONNECT_INTERVAL_MS = 5000;
const PING_INTERVAL_MS = 25000; // Ping every 25 seconds

// --- Listener State Variables ---
let coindcxSocket = null;
let internalWsClient = null;
let pingIntervalId = null; // To hold the ID of our ping interval

// --- Internal Receiver Connection (No changes) ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    internalWsClient = new WebSocket(internalReceiverUrl);
    internalWsClient.on('error', (err) => {
        console.error(`[Listener] Internal receiver WebSocket error: ${err.message}`);
    });
    internalWsClient.on('close', () => {
        console.log('[Listener] Connection to internal receiver closed. Reconnecting...');
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
    internalWsClient.on('open', () => {
        console.log('[Listener] Connected to internal receiver at ws://localhost:8082');
    });
}

// --- Data Extraction & Processing (No changes) ---
function processCoinDCXTradeUpdate(tradeData) {
    if (!tradeData || !tradeData.p) return;
    try {
        const current_trade_price = parseFloat(tradeData.p);
        if (!isNaN(current_trade_price)) {
            const pricePayload = { p: current_trade_price };
            sendToInternalClient(pricePayload);
        }
    } catch (error) {
        console.error(`[Listener] Error processing CoinDCX trade data: ${error.message}`);
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

// --- CoinDCX Stream Connection (UPDATED WITH LOGGING) ---
function connectToCoinDCX() {
    if (coindcxSocket && coindcxSocket.connected) {
        return;
    }
    console.log(`[Listener] Connecting to CoinDCX at ${COINDCX_STREAM_URL}...`);
    coindcxSocket = io(COINDCX_STREAM_URL, { transports: ['websocket'] });

    coindcxSocket.on('connect', () => {
        console.log(`[Listener] Successfully connected to CoinDCX. Subscribing to channel: ${COINDCX_TRADE_CHANNEL}`);
        coindcxSocket.emit('join', { channelName: COINDCX_TRADE_CHANNEL });

        if (pingIntervalId) clearInterval(pingIntervalId);

        pingIntervalId = setInterval(() => {
            if (coindcxSocket && coindcxSocket.connected) {
                coindcxSocket.emit('ping');
                console.log('[Listener] --> Sent ping to CoinDCX server');
            }
        }, PING_INTERVAL_MS);

        // =================================================================
        // === START: NEW DIAGNOSTIC LOGGING ===============================
        // =================================================================

        // CATCH-ALL LISTENER: This will log EVERY event from the server.
        // Look for the real name of the trade event in these logs.
        coindcxSocket.onAny((eventName, ...args) => {
            console.log(`[Listener] <<-- DEBUG: Received event '${eventName}' with data:`, JSON.stringify(args));
        });

        // =================================================================
        // === END: NEW DIAGNOSTIC LOGGING =================================
        // =================================================================
    });

    coindcxSocket.on('pong', () => {
        console.log('[Listener] <-- Received pong from CoinDCX server');
    });
    
    // RAW DATA LOGGER: Added a log here to see the data before processing.
    coindcxSocket.on('new-trade', (data) => {
        console.log('[Listener] <<-- RAW DATA on "new-trade" event:', JSON.stringify(data, null, 2));
        processCoinDCXTradeUpdate(data);
    });

    coindcxSocket.on('connect_error', (err) => {
        console.error(`[Listener] CoinDCX connection error: ${err.message}`);
    });

    coindcxSocket.on('disconnect', (reason) => {
        console.log(`[Listener] Disconnected from CoinDCX: ${reason}.`);
        if (pingIntervalId) {
            clearInterval(pingIntervalId);
            pingIntervalId = null;
        }
        coindcxSocket.disconnect();
        coindcxSocket = null;
        console.log(`[Listener] Reconnecting in ${RECONNECT_INTERVAL_MS / 1000}s...`);
        setTimeout(connectToCoinDCX, RECONNECT_INTERVAL_MS);
    });
}

// --- Start the connections ---
connectToInternalReceiver();
connectToCoinDCX();

// --- END OF FILE coindcx_listener.js ---