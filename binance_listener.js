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
const SEND_INTERVAL_MS = 100;

// --- Connection URLs ---
const internalReceiverUrl = 'ws://instance-20250627-040948.asia-south2-a.c.ace-server-460719-b7.internal:8082';
const BYBIT_STREAM_URL = 'wss://stream.bybit.com/v5/public/spot';

// --- Listener State Variables ---
let internalWsClient, bybitWsClient;
let orderBook = { bids: new Map(), asks: new Map() };
let latestPayload = null;

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

// --- Bybit Exchange Connection ---
function connectToBybit() {
    bybitWsClient = new WebSocket(BYBIT_STREAM_URL);
    
    bybitWsClient.on('open', () => {
        console.log('[Bybit] Connection established.');
        orderBook = { bids: new Map(), asks: new Map() }; 

        const subscriptionMessage = {
            op: "subscribe",
            args: [`orderbook.50.${SYMBOL}`]
        };
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

            if (message.topic && message.topic.startsWith('orderbook.50') && message.data) {
                
                const applyUpdates = (bookSide, updates) => {
                    for (const [price, size] of updates) {
                        if (size === '0') {
                            bookSide.delete(price);
                        } else {
                            bookSide.set(price, size);
                        }
                    }
                };

                // Correctly handles the initial full book
                if (message.type === 'snapshot') {
                    orderBook.bids = new Map(message.data.b);
                    orderBook.asks = new Map(message.data.a);
                } 
                // Correctly handles all subsequent changes
                else if (message.type === 'delta') {
                    applyUpdates(orderBook.bids, message.data.b);
                    applyUpdates(orderBook.asks, message.data.a);
                }

                const sortedBids = Array.from(orderBook.bids.entries()).sort((a, b) => parseFloat(b[0]) - parseFloat(a[0]));
                const sortedAsks = Array.from(orderBook.asks.entries()).sort((a, b) => parseFloat(a[0]) - parseFloat(b[0]));
                
                const levels = [];
                const numLevels = Math.max(sortedBids.length, sortedAsks.length);

                for (let i = 0; i < numLevels; i++) {
                    const bid = sortedBids[i] ? { price: parseFloat(sortedBids[i][0]), quantity: parseFloat(sortedBids[i][1]) } : null;
                    const ask = sortedAsks[i] ? { price: parseFloat(sortedAsks[i][0]), quantity: parseFloat(sortedAsks[i][1]) } : null;

                    levels.push({
                        level: i,
                        bid: bid,
                        ask: ask
                    });
                }

                latestPayload = { 
                    type: 'S',
                    levels: levels
                };
            }
        } catch (e) { 
            console.error(`[Bybit] Error processing message: ${e.message}`, data.toString());
        }
    });

    bybitWsClient.on('error', (err) => console.error('[Bybit] Connection error:', err.message));
    
    bybitWsClient.on('close', () => {
        console.error('[Bybit] Connection closed. Reconnecting...');
        bybitWsClient = null;
        setTimeout(connectToBybit, RECONNECT_INTERVAL_MS);
    });
}

// --- Start all connections ---
console.log(`[Listener] Starting... PID: ${process.pid}`);
connectToInternalReceiver();
connectToBybit();

// --- Interval to send data to the client at a fixed rate ---
setInterval(() => {
    if (latestPayload) {
        sendToInternalClient(latestPayload);
        latestPayload = null;
    }
}, SEND_INTERVAL_MS);
