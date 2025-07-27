// binance_listener_uw.js
const uWS = require('uWebSockets.js');
const { URL } = require('url');

process.on('uncaughtException', (err, origin) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`, err.stack || err);
    process.exit(1); // Exit immediately on fatal error
});
process.on('unhandledRejection', (reason, promise) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNHANDLED PROMISE REJECTION`, reason);
    process.exit(1); // Exit immediately on fatal error
});

const SYMBOL = 'btcusdt';
const RECONNECT_INTERVAL_MS = 5000;
const MINIMUM_TICK_SIZE = 0.2;

const internalReceiverUrl = 'ws://instance-20250627-040948.asia-south2-a.c.ace-server-460719-b7.internal:8082/internal';
const BINANCE_FUTURES_STREAM_URL = `wss://fstream.binance.com/ws/${SYMBOL}@trade`;

let internalWsClient = null;
let last_sent_trade_price = null;
const app = uWS.App({}); // uWS App can be used for both server and client connections

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient) return; // Connection attempt in progress

    const url = new URL(internalReceiverUrl);
    const host = url.hostname;
    const port = url.port || (url.protocol === 'wss:' ? 443 : 80);
    const ssl = url.protocol === 'wss:';
    
    app.ws('/internal', {
        open: (ws) => {
            internalWsClient = ws;
            console.log('[Internal] Connection established.');
        },
        message: (ws, message, isBinary) => {
            // Not expecting messages from receiver, but good to have
        },
        close: (ws, code, message) => {
            internalWsClient = null;
            console.error(`[Internal] Connection closed. Reconnecting in ${RECONNECT_INTERVAL_MS}ms...`);
            setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
        },
        drain: (ws) => {
             console.log('[Internal] WebSocket backpressure drained.');
        }
    });
    
    // Initiate the client connection
    app.connect(ssl ? 'wss' : 'ws', host, parseInt(port, 10), '/internal');
}

function sendToInternalClient(payload) {
    if (internalWsClient && internalWsClient.getBufferedAmount() === 0) {
        try {
            internalWsClient.send(JSON.stringify(payload));
        } catch (e) {
            console.error(`[Internal] Failed to send message: ${e.message}`);
        }
    }
}

// --- Binance Connection ---
function connectToBinance() {
    const url = new URL(BINANCE_FUTURES_STREAM_URL);
    const host = url.hostname;
    const port = url.port || (url.protocol === 'wss:' ? 443 : 80);
    const path = url.pathname;
    const ssl = url.protocol === 'wss:';

    app.ws(path, {
        open: (ws) => {
            console.log(`[Binance] Connection established to stream: ${SYMBOL}@trade`);
            last_sent_trade_price = null;
        },
        message: (ws, message, isBinary) => {
            try {
                // uWebSockets.js provides an ArrayBuffer, needs conversion
                const messageString = Buffer.from(message).toString();
                const trade = JSON.parse(messageString);

                if (trade.e === 'trade' && trade.p) {
                    const current_trade_price = parseFloat(trade.p);
                    if (isNaN(current_trade_price)) return;

                    const shouldSendPrice = (last_sent_trade_price === null) || (Math.abs(current_trade_price - last_sent_trade_price) >= MINIMUM_TICK_SIZE);

                    if (shouldSendPrice) {
                        const payload = { type: 'S', p: current_trade_price };
                        sendToInternalClient(payload);
                        last_sent_trade_price = current_trade_price;
                    }
                }
            } catch (e) {
                console.error(`[Binance] Error processing message: ${e.message}`);
            }
        },
        close: (ws, code, message) => {
            console.error('[Binance] Connection closed. Reconnecting...');
            // No need to nullify a client handle here, the close event triggers the reconnect
            setTimeout(connectToBinance, RECONNECT_INTERVAL_MS);
        },
        drain: (ws) => {
            console.log('[Binance] WebSocket backpressure drained.');
        }
    });
    
    // Initiate the client connection to Binance
    app.connect(ssl ? 'wss' : 'ws', host, parseInt(port, 10), path);
}

console.log(`[Listener] Starting... PID: ${process.pid}`);
connectToInternalReceiver();
connectToBinance();

// Since uWS.App().run() is for servers, we don't call it. The app object is just used to define client behaviors.
// We need to keep the script running. A simple interval will do.
setInterval(() => {}, 1000 * 60 * 60);
