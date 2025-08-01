// bybit_listener_optimized.js
// --- MODIFICATION: The 'ws' library will now use C++ addons for sending data ---
// By including `bufferutil` and `utf-8-validate` in package.json,
// all send operations from this WebSocket client are C++ accelerated.
const WebSocket = require('ws');

// --- Process-wide Error Handling ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`, err.stack || err);
    cleanupAndExit(1);
});
process.on('unhandledRejection', (reason, promise) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNHANDLED PROMISE REJECTION`, reason);
    cleanupAndExit(1);
});

/**
 * Gracefully terminates WebSocket clients and exits the process.
 * @param {number} [exitCode=1] - The exit code to use.
 */
function cleanupAndExit(exitCode = 1) {
    const clientsToTerminate = [internalWsClient, exchangeWsClient];
    console.error('[Listener] Initiating cleanup...');
    clientsToTerminate.forEach(client => {
        if (client && (client.readyState === WebSocket.OPEN || client.readyState === WebSocket.CONNECTING)) {
            try {
                client.terminate();
            } catch (e) {
                console.error(`[Listener] Error during WebSocket termination: ${e.message}`);
            }
        }
    });
    // Allow time for cleanup before force-exiting
    setTimeout(() => {
        console.error(`[Listener] Exiting with code ${exitCode}.`);
        process.exit(exitCode);
    }, 1000).unref();
}

// --- Configuration ---
const SYMBOL = 'BTCUSDT';
const RECONNECT_INTERVAL_MS = 5000;
const MINIMUM_TICK_SIZE = 0.1;

// Using the correct internal DNS for service-to-service communication in GCP
const internalReceiverUrl = 'ws://instance-20250627-040948.asia-south2-a.c.ace-server-460719-b7.internal:8082/internal';
const EXCHANGE_STREAM_URL = 'wss://stream.bybit.com/v5/public/spot';

// --- WebSocket Clients and State ---
// This client's .send() method will be C++ accelerated.
let internalWsClient;
// This client receives data from the exchange.
let exchangeWsClient;

let last_sent_price = null;
const payload_to_send = { type: 'S', p: 0.0 };

/**
 * Establishes and maintains the connection to the internal WebSocket receiver.
 */
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) return;
    
    // This WebSocket client instance is created from the 'ws' library.
    internalWsClient = new WebSocket(internalReceiverUrl);

    internalWsClient.on('error', (err) => console.error(`[Internal] WebSocket error: ${err.message}`));
    internalWsClient.on('close', () => {
        console.error('[Internal] Connection closed. Reconnecting...');
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
    internalWsClient.on('open', () => console.log('[Internal] Connection established.'));
}

/**
 * Sends a payload to the internal WebSocket client.
 * --- THIS FUNCTION NOW USES C++ UNDER THE HOOD ---
 * Because the 'bufferutil' and 'utf-8-validate' packages are installed,
 * this 'send' call is automatically delegated to high-performance C++ code.
 * @param {object} payload - The data to send.
 */
function sendToInternalClient(payload) {
    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        try {
            internalWsClient.send(JSON.stringify(payload));
        } catch (e) {
            console.error(`[Internal] Failed to send message: ${e.message}`);
        }
    }
}

/**
 * Establishes and maintains the connection to the Bybit WebSocket stream.
 */
function connectToExchange() {
    exchangeWsClient = new WebSocket(EXCHANGE_STREAM_URL);

    exchangeWsClient.on('open', () => {
        console.log(`[Bybit] Connection established to: ${EXCHANGE_STREAM_URL}`);
        const subscriptionMessage = {
            op: "subscribe",
            args: [`orderbook.1.${SYMBOL}`]
        };
        try {
            exchangeWsClient.send(JSON.stringify(subscriptionMessage));
            console.log(`[Bybit] Subscribed to ${subscriptionMessage.args[0]}`);
        } catch(e) {
            console.error(`[Bybit] Failed to send subscription message: ${e.message}`);
        }
        last_sent_price = null;
    });

    exchangeWsClient.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());
            if (message.topic && message.topic.startsWith('orderbook.1') && message.data) {
                const bids = message.data.b;
                if (bids && bids.length > 0 && bids[0].length > 0) {
                    const bestBidPrice = parseFloat(bids[0][0]);
                    if (isNaN(bestBidPrice)) return;

                    const shouldSendPrice = (last_sent_price === null) || (Math.abs(bestBidPrice - last_sent_price) >= MINIMUM_TICK_SIZE);
                    if (shouldSendPrice) {
                        payload_to_send.p = bestBidPrice;
                        // This call now leverages C++ for sending data.
                        sendToInternalClient(payload_to_send);
                        last_sent_price = bestBidPrice;
                    }
                }
            }
        } catch (e) {
            console.error(`[Bybit] Error processing message: ${e.message}`);
        }
    });

    exchangeWsClient.on('error', (err) => console.error('[Bybit] Connection error:', err.message));
    exchangeWsClient.on('close', () => {
        console.error('[Bybit] Connection closed. Reconnecting...');
        exchangeWsClient = null;
        setTimeout(connectToExchange, RECONNECT_INTERVAL_MS);
    });
    
    const heartbeatInterval = setInterval(() => {
        if (exchangeWsClient && exchangeWsClient.readyState === WebSocket.OPEN) {
            try {
                exchangeWsClient.send(JSON.stringify({ op: 'ping' }));
            } catch (e) {
                console.error(`[Bybit] Failed to send ping: ${e.message}`);
            }
        } else {
            clearInterval(heartbeatInterval);
        }
    }, 20000);
}

// --- Script Entry Point ---
console.log(`[Listener] Starting... PID: ${process.pid}`);
connectToInternalReceiver();
connectToExchange();
