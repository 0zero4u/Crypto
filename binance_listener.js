const WebSocket = require('ws');

// --- Global Error Handlers ---
// (No changes to this section)
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
// (No changes to this section)
function cleanupAndExit(exitCode = 1) {
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
const MINIMUM_TICK_SIZE = 0.1;

// --- Connection URLs ---
const internalReceiverUrl = 'ws://instance-20250627-040948.asia-south2-a.c.ace-server-460719-b7.internal:8082/internal';
const BINANCE_FUTURES_STREAM_URL = `wss://fstream.binance.com/ws/${SYMBOL}@trade`;

// --- Listener State Variables ---
let internalWsClient, binanceWsClient;
let last_sent_trade_price = null;
let last_received_trade_price = null; // **NEW**: Store the absolute last price received from Binance.

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

    // **NEW**: Handle commands from the data_receiver_server
    internalWsClient.on('message', (data) => {
        try {
            const command = JSON.parse(data.toString());
            if (command.action === 'get_fresh_price') {
                console.log('[Listener] Received command to send fresh price.');
                if (last_received_trade_price !== null) {
                    // Send the last known price immediately, regardless of tick size.
                    const payload = {
                        type: 'S',
                        p: last_received_trade_price,
                        timestamp: Date.now()
                    };
                    sendToInternalClient(payload);
                    // Update the 'last_sent' price to prevent an immediate duplicate broadcast.
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

// --- Data Forwarding ---
function sendToInternalClient(payload) {
    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        try {
            internalWsClient.send(JSON.stringify(payload));
        } catch (e) { console.error(`[Internal] Failed to send message: ${e.message}`); }
    }
}

// --- Binance Futures Connection ---
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

                // **MODIFIED**: Always update the last_received_trade_price.
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
