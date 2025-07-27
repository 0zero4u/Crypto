const WebSocket = require('ws');

// --- Global Error Handlers --- (No changes)
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
let heartbeatInterval = null; // **NEW**: To manage the heartbeat timer
function cleanupAndExit(exitCode = 1) {
    if (heartbeatInterval) clearInterval(heartbeatInterval); // **NEW**: Stop the heartbeat on exit
    const clientsToTerminate = [internalWsClient, binanceWsClient];
    
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
const MINIMUM_TICK_SIZE = 0.2;
const HEARTBEAT_INTERVAL_MS = 25000; // **NEW**: Send a ping every 25 seconds.

// --- Connection URLs --- (No changes)
const internalReceiverUrl = 'ws://instance-20250627-040948.asia-south2-a.c.ace-server-460719-b7.internal:8082/internal';
const BINANCE_FUTURES_STREAM_URL = `wss://fstream.binance.com/ws/${SYMBOL}@trade`;

// --- Listener State Variables ---
let internalWsClient, binanceWsClient;
let last_sent_trade_price = null;
let last_received_trade_price = null;

// **NEW**: Heartbeat function to keep the connection alive
function sendHeartbeat() {
    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        try {
            internalWsClient.send(JSON.stringify({ type: 'ping' }));
        } catch (e) {
            console.error(`[Internal] Failed to send heartbeat: ${e.message}`);
        }
    }
}

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) return;
    
    internalWsClient = new WebSocket(internalReceiverUrl);
    internalWsClient.on('error', (err) => console.error(`[Internal] WebSocket error: ${err.message}`));
    
    internalWsClient.on('close', () => {
        console.error('[Internal] Connection closed. Reconnecting...');
        if (heartbeatInterval) clearInterval(heartbeatInterval); // **NEW**: Stop pings when disconnected
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
    
    internalWsClient.on('open', () => {
        console.log('[Internal] Connection established.');
        // **NEW**: Start sending heartbeats once connected.
        if (heartbeatInterval) clearInterval(heartbeatInterval);
        heartbeatInterval = setInterval(sendHeartbeat, HEARTBEAT_INTERVAL_MS);
    });

    internalWsClient.on('message', (data) => {
        try {
            const command = JSON.parse(data.toString());
            if (command.action === 'get_fresh_price') {
                console.log('[Listener] Received command to send fresh price.');
                if (last_received_trade_price !== null) {
                    const payload = {
                        type: 'S',
                        p: last_received_trade_price,
                        timestamp: Date.now()
                    };
                    sendToInternalClient(payload);
                    last_sent_trade_price = last_received_trade_price;
                } else {
                    console.log('[Listener] Cannot send fresh price, no price has been received from Binance yet.');
                }
            }
        } catch (e) {
            console.error(`[Listener] Invalid command from internal server: ${e.message}`);
        }
    });
}

// --- Data Forwarding --- (No changes)
function sendToInternalClient(payload) {
    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        try {
            internalWsClient.send(JSON.stringify(payload));
        } catch (e) { console.error(`[Internal] Failed to send message: ${e.message}`); }
    }
}

// --- Binance Futures Connection --- (No changes to this function's logic)
function connectToBinance() {
    binanceWsClient = new WebSocket(BINANCE_FUTURES_STREAM_URL);
    
    binanceWsClient.on('open', () => {
        console.log(`[Binance] Connection established. Subscribed to stream: ${SYMBOL}@trade`);
        last_sent_trade_price = null;
        last_received_trade_price = null;
    });
    
    binanceWsClient.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());
            if (message.e === 'trade' && message.p) {
                const current_trade_price = parseFloat(message.p);
                if (isNaN(current_trade_price)) {
                    return;
                }
                last_received_trade_price = current_trade_price;
                const shouldSend = last_sent_trade_price === null || Math.abs(current_trade_price - last_sent_trade_price) >= MINIMUM_TICK_SIZE;
                if (shouldSend) {
                    const payload = {
                        type: 'S',
                        p: current_trade_price,
                        timestamp: Date.now()
                    };
                    if (last_sent_trade_price === null) {
                        console.log(`[Binance] Sending initial price: ${current_trade_price}`);
                    }
                    sendToInternalClient(payload);
                    last_sent_trade_price = current_trade_price;
                }
            }
        } catch (e) { 
            console.error(`[Binance] Error processing message: ${e.message}`);
        }
    });

    binanceWsClient.on('error', (err) => console.error('[Binance] Connection error:', err.message));
    
    binanceWsClient.on('close', () => {
        console.error('[Binance] Connection closed. Reconnecting...');
        binanceWsClient = null;
        setTimeout(connectToBinance, RECONNECT_INTERVAL_MS);
    });
}

// --- Start all connections ---
console.log(`[Listener] Starting... PID: ${process.pid}`);
connectToInternalReceiver();
connectToBinance();
