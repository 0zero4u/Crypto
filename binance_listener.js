
const WebSocket = require('ws');

const binanceStreamUrl = 'wss://stream.binance.com:9443/ws/btcusdt@depth5@100ms';
let pingInterval; // Declare here to clear it on close

function connectToBinance() {
    console.log(`Attempting to connect to Binance stream: ${binanceStreamUrl}`);
    const binanceWs = new WebSocket(binanceStreamUrl);

    binanceWs.on('open', function open() {
        console.log('SUCCESS: Connected to Binance stream (btcusdt@depth5@100ms).');

        // Keep alive: Binance might close connections if inactive.
        // Send a ping frame periodically. The 'ws' library handles pong responses automatically.
        if (pingInterval) clearInterval(pingInterval); // Clear previous interval if reconnecting
        pingInterval = setInterval(() => {
            if (binanceWs.readyState === WebSocket.OPEN) {
                console.log('Sending PING to Binance');
                binanceWs.ping(() => {}); // Pass a no-op callback if required by library version
            }
        }, 3 * 60 * 1000); // Send a ping every 3 minutes
    });

    binanceWs.on('message', function incoming(data) {
        const messageString = data.toString();
        console.log('Received from Binance:', messageString);
        // You can add more processing here later
    });

    binanceWs.on('pong', () => {
        console.log('Received PONG from Binance');
    });

    binanceWs.on('error', function error(err) {
        console.error('Binance WebSocket error:', err.message);
        // The 'close' event will usually follow, triggering a reconnect attempt.
    });

    binanceWs.on('close', function close(code, reason) {
        console.log(`Binance WebSocket closed. Code: ${code}, Reason: ${reason ? reason.toString() : 'N/A'}`);
        if (pingInterval) clearInterval(pingInterval); // Stop pinging
        console.log('Attempting to reconnect in 5 seconds...');
        setTimeout(connectToBinance, 5000); // Reconnect after 5 seconds
    });
}

// Start the connection
connectToBinance();

console.log('Binance listener script started. Waiting for connection...');
