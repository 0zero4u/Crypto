// binance_listener_updated.js
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
    const clientsToTerminate = [internalWsClient, spotWsClient, futuresWsClient];
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
const RECONNECT_INTERVAL_MS = 5000;
const BASIS_FILTER_THRESHOLD = 1.0;

// Using the correct internal DNS for service-to-service communication
const internalReceiverUrl = 'ws://instance-20250627-040948.asia-south2-a.c.ace-server-460719-b7.internal:8082/internal';

// --- MODIFIED: Updated URLs to Binance Spot and Futures streams ---
const SPOT_STREAM_URL = 'wss://stream.binance.com:9443/ws/btcusdt@trade';
const FUTURES_STREAM_URL = 'wss://fstream.binance.com/ws/btcusdt@trade';

// --- WebSocket Clients and State ---
let internalWsClient, spotWsClient, futuresWsClient;

let current_spot_price = null;
let current_futures_price = null;
let previous_basis = null;

// Optimization: Reusable payload object to prevent GC pressure.
const payload_to_send = { type: 'S', p: 0.0 };

/**
 * Establishes and maintains the connection to the internal WebSocket receiver.
 */
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) return;

    internalWsClient = new WebSocket(internalReceiverUrl);

    internalWsClient.on('close', () => {
        internalWsClient = null; // Important to allow reconnection
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
}

/**
 * Sends a payload to the internal WebSocket client.
 * @param {object} payload - The data to send.
 */
function sendToInternalClient(payload) {
    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        try {
            internalWsClient.send(JSON.stringify(payload));
        } catch (e) {
            // Error logging eliminated for performance
        }
    }
}

/**
 * Processes price updates and calculates the basis change.
 */
function onPriceUpdate() {
    if (current_spot_price === null || current_futures_price === null) {
        return; // Need both prices to calculate basis
    }

    const current_basis = current_futures_price - current_spot_price;

    if (previous_basis !== null) {
        const delta_basis = current_basis - previous_basis;

        if (Math.abs(delta_basis) >= BASIS_FILTER_THRESHOLD) {
            payload_to_send.p = delta_basis;
            sendToInternalClient(payload_to_send);
        }
    }
    
    // Always update the previous basis for the next calculation
    previous_basis = current_basis;
}

/**
 * Establishes connection to the Binance Spot WebSocket stream.
 */
function connectToSpot() {
    spotWsClient = new WebSocket(SPOT_STREAM_URL);

    spotWsClient.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());
            if (message && message.p) {
                current_spot_price = parseFloat(message.p);
                if (!isNaN(current_spot_price)) {
                    onPriceUpdate();
                }
            }
        } catch (e) {
            // Error logging eliminated
        }
    });

    spotWsClient.on('close', () => {
        spotWsClient = null; // Allow reconnection
        current_spot_price = null; // Reset price on disconnect
        setTimeout(connectToSpot, RECONNECT_INTERVAL_MS);
    });

    spotWsClient.on('error', () => { 
        // Errors will also trigger the 'close' event, handling reconnection there.
    });
}

/**
 * Establishes connection to the Binance Futures WebSocket stream.
 */
function connectToFutures() {
    futuresWsClient = new WebSocket(FUTURES_STREAM_URL);

    futuresWsClient.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());
            if (message && message.p) {
                current_futures_price = parseFloat(message.p);
                if(!isNaN(current_futures_price)) {
                    onPriceUpdate();
                }
            }
        } catch (e) {
            // Error logging eliminated
        }
    });

    futuresWsClient.on('close', () => {
        futuresWsClient = null; // Allow reconnection
        current_futures_price = null; // Reset price on disconnect
        setTimeout(connectToFutures, RECONNECT_INTERVAL_MS);
    });

    futuresWsClient.on('error', () => {
        // Errors will also trigger the 'close' event, handling reconnection there.
    });
}

// --- Script Entry Point ---
connectToInternalReceiver();
connectToSpot();
connectToFutures();
