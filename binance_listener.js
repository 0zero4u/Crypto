const WebSocket = require('ws');
const https = require('https'); // New: For REST API fetch

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

// --- Connection URLs ---

const internalReceiverUrl = 'ws://instance-20250627-040948.asia-south2-a.c.ace-server-460719-b7.internal:8082/internal';
const BINANCE_FUTURES_STREAM_URL = `wss://fstream.binance.com/ws/${SYMBOL}@trade`;
const BINANCE_REST_API_URL = `https://fapi.binance.com/fapi/v1/ticker/price?symbol=${SYMBOL.toUpperCase()}`; // New: REST endpoint for current price

// --- Listener State Variables ---

let internalWsClient, binanceWsClient;
let last_sent_trade_price = null;

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

// --- Binance Futures Connection ---

function connectToBinance() {
  binanceWsClient = new WebSocket(BINANCE_FUTURES_STREAM_URL);
  binanceWsClient.on('open', () => {
    console.log(`[Binance] Connection established. Subscribed to stream: ${SYMBOL}@trade`);
    last_sent_trade_price = null;

    // New: Fetch initial price from REST API and send if it's the first time
    https.get(BINANCE_REST_API_URL, (res) => {
      let data = '';
      res.on('data', (chunk) => { data += chunk; });
      res.on('end', () => {
        try {
          const response = JSON.parse(data);
          const initialPrice = parseFloat(response.price);
          if (!isNaN(initialPrice) && last_sent_trade_price === null) {
            const payload = {
              type: 'S',
              p: initialPrice
            };
            sendToInternalClient(payload);
            last_sent_trade_price = initialPrice;
            console.log(`[Binance] Sent initial price from REST API: ${initialPrice}`);
          }
        } catch (e) {
          console.error(`[Binance] Error parsing initial price: ${e.message}`);
        }
      });
    }).on('error', (err) => {
      console.error(`[Binance] Error fetching initial price: ${err.message}`);
    });
  });

  binanceWsClient.on('message', (data) => {
    try {
      const message = JSON.parse(data.toString());
      if (message.e === 'trade' && message.p) {
        const current_trade_price = parseFloat(message.p);
        if (isNaN(current_trade_price)) {
          return; // Ignore if price is not a valid number
        }

        // MODIFIED LOGIC: Determine if the price should be sent.
        let shouldSend = false;
        if (last_sent_trade_price === null) {
          // Case 1: First valid trade price received. Always send.
          shouldSend = true;
        } else {
          // Case 2: A price has been sent before. Check the difference.
          const price_difference = current_trade_price - last_sent_trade_price;
          if (Math.abs(price_difference) >= MINIMUM_TICK_SIZE) {
            shouldSend = true;
          }
        }

        if (shouldSend) {
          const payload = {
            type: 'S',
            p: current_trade_price
          };
          sendToInternalClient(payload);
          // Update the last sent price *only* when we send it.
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
