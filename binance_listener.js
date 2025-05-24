
const WebSocket = require('ws');

const binanceStreamUrl = 'wss://stream.binance.com:9443/ws/btcusdt@depth5@100ms';
const internalReceiverUrl = 'ws://localhost:8082'; // Sends data TO data_receiver_server.js's internal port

let binancePingInterval;
let internalWsClient;

function connectToInternalReceiver() {
    console.log(`[Listener] Attempting to connect to internal data receiver: ${internalReceiverUrl}`);
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) {
        // console.log('[Listener] Already connected or connecting to internal receiver.'); // Reduced
        return;
    }

    internalWsClient = new WebSocket(internalReceiverUrl);

    internalWsClient.on('open', () => {
        console.log('[Listener] SUCCESS: Connected to internal data receiver.');
    });

    internalWsClient.on('error', (err) => {
        console.error('[Listener] Internal receiver WebSocket error:', err.message);
    });

    internalWsClient.on('close', (code, reason) => {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] Internal receiver WebSocket closed. Code: ${code}, Reason: ${reasonStr}`);
        internalWsClient = null;
        console.log('[Listener] Attempting to reconnect to internal receiver in 5 seconds...');
        setTimeout(connectToInternalReceiver, 5000);
    });
}

function connectToBinance() {
    console.log(`[Listener] Attempting to connect to Binance stream: ${binanceStreamUrl}`);
    const binanceWs = new WebSocket(binanceStreamUrl);

    binanceWs.on('open', function open() {
        console.log('[Listener] SUCCESS: Connected to Binance stream (btcusdt@depth5@100ms).');
        if (binancePingInterval) clearInterval(binancePingInterval);
        binancePingInterval = setInterval(() => {
            if (binanceWs.readyState === WebSocket.OPEN) {
                // console.log('[Listener] Sending PING to Binance'); // REMOVED - Too frequent
                binanceWs.ping(() => {});
            }
        }, 3 * 60 * 1000);
    });

    binanceWs.on('message', function incoming(data) {
        const messageString = data.toString();
        // console.log('[Listener] Received raw from Binance:', messageString.substring(0,100)); // REMOVED - Too frequent

        try {
            const depthData = JSON.parse(messageString);
            if (depthData.bids && depthData.asks) {
                const extractedData = {
                    lastUpdateId: depthData.lastUpdateId,
                    bids: depthData.bids,
                    asks: depthData.asks,
                    timestamp: Date.now()
                };

                if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                    internalWsClient.send(JSON.stringify(extractedData));
                    // console.log('[Listener] Sent data to internal receiver.'); // REMOVED - Too frequent
                } else {
                    // console.warn('[Listener] Internal receiver not connected. Data not sent.'); // COMMENTED - Can be noisy if receiver is down
                }
            }
        } catch (error) {
            console.error('[Listener] Error parsing Binance message or processing data:', error);
            // console.error('[Listener] Problematic message:', messageString); // Keep if debugging parsing errors
        }
    });

    binanceWs.on('pong', () => {
        // console.log('[Listener] Received PONG from Binance'); // REMOVED - Too frequent
    });

    binanceWs.on('error', function error(err) {
        console.error('[Listener] Binance WebSocket error:', err.message);
    });

    binanceWs.on('close', function close(code, reason) {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] Binance WebSocket closed. Code: ${code}, Reason: ${reasonStr}`);
        if (binancePingInterval) clearInterval(binancePingInterval);
        console.log('[Listener] Attempting to reconnect to Binance in 5 seconds...');
        setTimeout(connectToBinance, 5000);
    });
}

// Start the connections
console.log('[Listener] Binance listener (modified) script starting...');
connectToBinance();
connectToInternalReceiver();
