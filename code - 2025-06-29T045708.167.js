const WebSocket = require('ws');
const kcp = require('kcp');
const dgram = require('dgram');

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
    console.error('[Listener] Initiating cleanup...');

    if (spotWsClient && (spotWsClient.readyState === WebSocket.OPEN || spotWsClient.readyState === WebSocket.CONNECTING)) {
        try { spotWsClient.terminate(); } catch (e) { console.error(`[Listener] Error during WebSocket termination: ${e.message}`); }
    }
    if (udpSocket) {
        try { udpSocket.close(); } catch (e) { console.error(`[Listener] Error closing UDP socket: ${e.message}`); }
    }
    if (kcpUpdateInterval) {
        clearInterval(kcpUpdateInterval);
    }
    
    setTimeout(() => {
        console.error(`[Listener] Exiting with code ${exitCode}.`);
        process.exit(exitCode);
    }, 1000).unref();
}

// --- Listener Configuration ---
const SYMBOL = 'btcusdt';
const RECONNECT_INTERVAL_MS = 5000;
const MINIMUM_TICK_SIZE = 0.1;

// --- KCP/UDP Configuration ---
const internalReceiverHost = 'INTERNAL_IP_OF_DELHI_VM'; // IMPORTANT: Replace with the private IP of your Delhi VM
const internalReceiverPort = 8082;
const KCP_CONV_ID = 42; // "Conversation ID", must match on both ends.

// --- Listener State Variables ---
let spotWsClient;
let last_sent_spot_bid_price = null;
let kcpClient, udpSocket, kcpUpdateInterval;

// --- Internal KCP Client Setup ---
function setupInternalKcpClient() {
    udpSocket = dgram.createSocket('udp4');

    const outputCallback = (data, size, context) => {
        udpSocket.send(data, 0, size, internalReceiverPort, internalReceiverHost);
    };

    const context = {}; // Context can be empty for the client side
    kcpClient = new kcp.KCP(KCP_CONV_ID, context);
    
    // Optimal settings for premium-tier, low-loss, high-latency link
    kcpClient.nodelay(true, 10, 2, true); // (nodelay, interval, resend, nc)

    kcpClient.output(outputCallback);

    console.log(`[Internal-KCP] KCP client configured to send data to ${internalReceiverHost}:${internalReceiverPort}`);
    
    // KCP engine heartbeat
    kcpUpdateInterval = setInterval(() => {
        if (kcpClient) {
            kcpClient.update(Date.now());
        }
    }, 10); // 10ms interval
}

// --- Data Forwarding via KCP ---
function sendToInternalClient(payload) {
    if (kcpClient) {
        try {
            const dataBuffer = Buffer.from(JSON.stringify(payload));
            kcpClient.send(dataBuffer);
        } catch (e) {
            console.error(`[Internal-KCP] Failed to send message: ${e.message}`);
        }
    }
}

// --- Spot Exchange Connection ---
function connectToSpot() {
    const SPOT_STREAM_URL = `wss://stream.binance.com:9443/ws/${SYMBOL}@bookTicker`;
    spotWsClient = new WebSocket(SPOT_STREAM_URL);
    
    spotWsClient.on('open', () => {
        console.log('[Spot] Connection established.');
        last_sent_spot_bid_price = null;
    });
    
    spotWsClient.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());
            const current_spot_bid_price = parseFloat(message.b);
            
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
        } catch (e) { 
            // Ignore JSON parsing errors
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
setupInternalKcpClient();
connectToSpot();