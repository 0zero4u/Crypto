const uWS = require('uWebSockets.js');
const { TextDecoder } = require('util'); // Built-in Node.js module

// --- Global Error Handlers ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`);
    console.error(err.stack || err);
    process.exit(1);
});
process.on('unhandledRejection', (reason, promise) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNHANDLED PROMISE REJECTION`);
    console.error('[Listener] Unhandled Rejection at:', promise);
    console.error('[Listener] Reason:', reason instanceof Error ? reason.stack : reason);
    process.exit(1);
});

// --- Listener Configuration ---
const SYMBOL = 'btcusdt';
const RECONNECT_INTERVAL_MS = 5000;
const MINIMUM_TICK_SIZE = 0.2;
const decoder = new TextDecoder('utf-8');

// --- Connection URLs ---
const internalReceiverHost = 'ws://instance-20250627-040948.asia-south2-a.c.ace-server-460719-b7.internal:8082/internal';
const BINANCE_FUTURES_STREAM_URL = `wss://fstream.binance.com/ws/${SYMBOL}@trade`;

// --- Listener State Variables ---
let internalWs = null;
let last_sent_trade_price = null;
const app = uWS.App({});

// --- PERFORMANCE OPTIMIZATION: Reusable object to avoid GC overhead ---
const reusablePayload = { type: 'S', p: 0 };

// --- Data Forwarding ---
function sendToInternalClient(payload) {
    if (internalWs && internalWs.getBufferedAmount() === 0) {
        try {
            internalWs.send(JSON.stringify(payload));
        } catch (e) {
            console.error(`[Internal] Failed to send message: ${e.message}`);
        }
    }
}

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    console.log(`[Internal] Attempting to connect to ${internalReceiverHost}...`);
    app.ws(internalReceiverHost, {
        open: (ws) => {
            console.log('[Internal] Connection established.');
            internalWs = ws;
        },
        message: (ws, message, isBinary) => { /* Not expecting messages */ },
        close: (ws, code, message) => {
            console.error(`[Internal] Connection closed. Code: ${code}. Reconnecting in ${RECONNECT_INTERVAL_MS}ms...`);
            internalWs = null;
            setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
        },
        error: (ws, err) => {
            console.error(`[Internal] Connection error: ${err}. Will attempt to reconnect on close.`);
        }
    });
}

// --- Binance Futures Connection ---
function connectToBinance() {
    console.log(`[Binance] Attempting to connect to ${BINANCE_FUTURES_STREAM_URL}...`);
    app.ws(BINANCE_FUTURES_STREAM_URL, {
        idleTimeout: 65, // Handles Binance's ping interval
        
        open: (ws) => {
            console.log(`[Binance] Connection established. Subscribed to stream: ${SYMBOL}@trade`);
            last_sent_trade_price = null;
        },
        message: (ws, message, isBinary) => {
            try {
                const trade = JSON.parse(decoder.decode(message));

                if (trade.e === 'trade' && trade.p) {
                    const current_trade_price = parseFloat(trade.p);

                    if (isNaN(current_trade_price)) return;

                    if (last_sent_trade_price === null) {
                        last_sent_trade_price = current_trade_price;
                        return;
                    }

                    const price_difference = current_trade_price - last_sent_trade_price;

                    if (Math.abs(price_difference) >= MINIMUM_TICK_SIZE) {
                        reusablePayload.p = current_trade_price;
                        sendToInternalClient(reusablePayload);
                        last_sent_trade_price = current_trade_price;
                    }
                }
            } catch (e) {
                console.error(`[Binance] Error processing message: ${e.message}`);
            }
        },
        close: (ws, code, message) => {
            console.error(`[Binance] Connection closed. Code: ${code}. Reconnecting in ${RECONNECT_INTERVAL_MS}ms...`);
            setTimeout(connectToBinance, RECONNECT_INTERVAL_MS);
        },
        error: (ws, err) => {
            console.error(`[Binance] Connection error: ${err}. Will attempt to reconnect on close.`);
        }
    });
}

// --- Start all connections ---
console.log(`[Listener] Starting with uWebSockets.js... PID: ${process.pid}`);
connectToInternalReceiver();
connectToBinance();
