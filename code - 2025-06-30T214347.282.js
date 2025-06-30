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
    const clientsToTerminate = [internalWsClient, bybitWsClient];
    
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
const SYMBOL = 'BTCUSDT';
const RECONNECT_INTERVAL_MS = 5000;
const MINIMUM_TICK_SIZE = 0.1;

// --- Connection URLs ---
// IMPORTANT: Replace with the INTERNAL IP of your receiver VM ("vm49")
const internalReceiverUrl = 'ws://<IP_OF_YOUR_RECEIVER_VM49>:8082';
const BYBIT_STREAM_URL = 'wss://stream.bybit.com/v5/public/spot';

// --- Listener State Variables ---
let internalWsClient, bybitWsClient;
let last_sent_spot_bid_price = null;

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) return;
    
    // NEW: Log the connection attempt
    console.log(`[Internal] Attempting to connect to: ${internalReceiverUrl}`);
    internalWsClient = new WebSocket(internalReceiverUrl);

    // MODIFIED: Log the full error object for maximum detail
    internalWsClient.on('error', (err) => {
        console.error('[Internal] WebSocket connection error event fired.');
        console.error(err); // This will print the full error, including code like ECONNREFUSED
    });

    // MODIFIED: Log the close code and reason
    internalWsClient.on('close', (code, reason) => {
        const reasonString = reason ? reason.toString() : 'No reason given';
        console.error(`[Internal] Connection closed. Code: ${code}, Reason: ${reasonString}. Reconnecting in ${RECONNECT_INTERVAL_MS}ms...`);
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });

    internalWsClient.on('open', () => console.log('[Internal] Connection established successfully!'));
}

// --- Data Forwarding ---
function sendToInternalClient(payload) {
    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        try {
            const messageString = JSON.stringify(payload);
            // NEW: Log the data being sent
            console.log(`[Internal] Sending data: ${messageString}`);
            internalWsClient.send(messageString);
        } catch (e) { console.error(`[Internal] Failed to send message: ${e.message}`); }
    } else {
        // NEW: Log if we are unable to send
        console.warn(`[Internal] Cannot send data. WebSocket is not open. ReadyState: ${internalWsClient ? internalWsClient.readyState : 'null'}`);
    }
}

// --- Bybit Exchange Connection ---
function connectToBybit() {
    console.log('[Bybit] Attempting to connect...');
    bybitWsClient = new WebSocket(BYBIT_STREAM_URL);
    
    bybitWsClient.on('open', () => {
        console.log('[Bybit] Connection established.');
        last_sent_spot_bid_price = null;
        const subscriptionMessage = { op: "subscribe", args: [`orderbook.1.${SYMBOL}`] };
        bybitWsClient.send(JSON.stringify(subscriptionMessage));
        console.log(`[Bybit] Sent subscription for: ${subscriptionMessage.args[0]}`);
    });
    
    bybitWsClient.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());

            if (message.op === 'ping') {
                bybitWsClient.send(JSON.stringify({ op: 'pong', req_id: message.req_id }));
                return;
            }

            if (message.topic && message.topic.startsWith('orderbook.1') && message.data) {
                const topBid = message.data.b && message.data.b[0];
                if (!topBid) return;

                const current_spot_bid_price = parseFloat(topBid[0]);
                if (!current_spot_bid_price) return;

                if (last_sent_spot_bid_price === null) {
                    last_sent_spot_bid_price = current_spot_bid_price;
                    return;
                }

                const price_difference = current_spot_bid_price - last_sent_spot_bid_price;
                if (Math.abs(price_difference) >= MINIMUM_TICK_SIZE) {
                    const payload = { type: 'S', p: current_spot_bid_price };
                    sendToInternalClient(payload);
                    last_sent_spot_bid_price = current_spot_bid_price;
                }
            }
        } catch (e) { 
            console.error(`[Bybit] Error processing message: ${e.message}`);
        }
    });

    bybitWsClient.on('error', (err) => console.error('[Bybit] Connection error:', err));
    
    bybitWsClient.on('close', (code, reason) => {
        const reasonString = reason ? reason.toString() : 'No reason given';
        console.error(`[Bybit] Connection closed. Code: ${code}, Reason: ${reasonString}. Reconnecting in ${RECONNECT_INTERVAL_MS}ms...`);
        bybitWsClient = null;
        setTimeout(connectToBybit, RECONNECT_INTERVAL_MS);
    });
}

// --- Start all connections ---
console.log('--- [Listener] SCRIPT STARTING ---');
console.log(`[Listener] Target Receiver URL: ${internalReceiverUrl}`);
console.log(`[Listener] Bybit Stream URL: ${BYBIT_STREAM_URL}`);
console.log(`[Listener] Reconnect Interval: ${RECONNECT_INTERVAL_MS}ms`);
console.log('------------------------------------');
console.log(`[Listener] Starting... PID: ${process.pid}`);

connectToInternalReceiver();
connectToBybit();