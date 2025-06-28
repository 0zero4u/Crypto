const WebSocket = require('ws');

// --- Global Error Handlers ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`);
    console.error(err.stack || err);
    cleanupAndExit(1);
});
process.on('unhandledRejection', (reason, promise) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNHANDLED PROMISE REJECTION`);
    console.error('[Listener] Unhandled Rejection at:', promise);
    console.error('[Listener] Reason:', reason instanceof Error ? reason.stack : reason);
    cleanupAndExit(1);
});

// --- State Management ---
function cleanupAndExit(exitCode = 1) {
    // Terminate only the clients that are still in use.
    const clientsToTerminate = [internalWsClient, spotWsClient];
    
    console.error('[Listener] Initiating cleanup...');
    clientsToTerminate.forEach(client => {
        if (client && (client.readyState === WebSocket.OPEN || client.readyState === WebSocket.CONNECTING)) {
            try { client.terminate(); } catch (e) { console.error(`[Listener] Error during WebSocket termination: ${e.message}`); }
        }
    });
    
    setTimeout(() => {
        console.error(`[Listener] Exiting with code ${exitCode}.`);
        process.exit(exitCode);
    }, 1000).unref();
}

// --- Listener Configuration ---
const SYMBOL = 'btcusdt';
const RECONNECT_INTERVAL_MS = 5000;
const MINIMUM_TICK_SIZE = 0.1;

// --- Connection URLs ---
const internalReceiverUrl = 'ws://instance-20250627-040948.asia-south2-a.c.ace-server-460719-b7.internal:8082';
const SPOT_STREAM_URL = `wss://stream.binance.com:9443/ws/${SYMBOL}@bookTicker`;

// --- Listener State Variables ---
let internalWsClient, spotWsClient;
let last_sent_spot_bid_price = null; // Tracks the last bid price that triggered a signal

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) return;
    internalWsClient = new WebSocket(internalReceiverUrl);
    internalWsClient.on('error', (err) => console.error(`[Internal] WebSocket error: ${err.message}`));
    internalWsClient.on('close', () => {
        console.error('[Internal] Connection closed. Reconnecting...');
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
    internalWsClient.on('open', () => console.log('[Internal] Connection established.'));
}

// --- Data Forwarding ---
function sendToInternalClient(payload) {
    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        try {
            internalWsClient.send(JSON.stringify(payload));
        } catch (e) { console.error(`[Internal] Failed to send message: ${e.message}`); }
    }
}

// --- Spot Exchange Connection ---
function connectToSpot() {
    spotWsClient = new WebSocket(SPOT_STREAM_URL);
    spotWsClient.on('open', () => {
        console.log('[Spot] Connection established.');
        last_sent_spot_bid_price = null; // Reset on new connection
    });
    
    spotWsClient.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());
            const current_spot_bid_price = parseFloat(message.b);
            
            // Ensure we have a valid price to work with
            if (!current_spot_bid_price) {
                return;
            }

            // On the very first message, set the initial baseline price and do nothing else.
            if (last_sent_spot_bid_price === null) {
                last_sent_spot_bid_price = current_spot_bid_price;
                return;
            }

            // Calculate the price change since the last sent signal
            const price_difference = current_spot_bid_price - last_sent_spot_bid_price;

            // Check if the absolute price change meets the minimum tick size requirement
            if (Math.abs(price_difference) >= MINIMUM_TICK_SIZE) {
                // A valid tick has occurred. Prepare and send the payload.
                const payload = { 
                    type: 'S', // 'S' for Spot
                    p: current_spot_bid_price 
                };
                sendToInternalClient(payload);
                
                // Update the last sent price to the current price to set a new baseline
                last_sent_spot_bid_price = current_spot_bid_price;
            }
        } catch (e) { 
            // Ignore JSON parsing errors from potentially incomplete data streams
        }
    });

    spotWsClient.on('error', (err) => console.error('[Spot] Connection error:', err.message));
    
    spotWsClient.on('close', () => {
        console.error('[Spot] Connection closed. Reconnecting...');
        spotWsClient = null;
        setTimeout(connectToSpot, RECONNECT_INTERVAL_MS);
    });
}

// --- Start all connections ---
console.log(`[Listener] Starting... PID: ${process.pid}`);
connectToInternalReceiver();
connectToSpot();
