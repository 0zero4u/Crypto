
const WebSocket = require('ws');

// --- Configuration ---
const SPOT_SYMBOL = 'btcusdt';
const FUTURES_SYMBOL = 'btcusdt';
const SPOT_STREAM_URL = `wss://stream.binance.com:9443/ws/${SPOT_SYMBOL.toLowerCase()}@depth5@100ms`;
const FUTURES_STREAM_URL = `wss://fstream.binance.com/ws/${FUTURES_SYMBOL.toLowerCase()}@depth5@100ms`;
const DATA_RECEIVER_INTERNAL_URL = 'ws://localhost:8082';

const MIN_PROFIT_THRESHOLD_USD = 20.00;
const SPOT_FEE_RATE = 0.001;
const FUTURES_FEE_RATE = 0.0004;
const RECONNECT_INTERVAL_MS = 5000;
const RECEIVER_PING_INTERVAL_MS = 30 * 1000;

const DECIMAL_PLACES_PRICE = 2;
const DECIMAL_PLACES_PROFIT = 4;

// --- State Variables ---
let spotPrice = { bid: null, ask: null, timestamp: 0, eventTime: 0 };
let futuresPrice = { bid: null, ask: null, timestamp: 0, eventTime: 0 };
let spotWsClient = null;
let futuresWsClient = null;
let receiverClient = null;
let receiverPingIntervalId = null;
let isReceiverReconnecting = false; // Track reconnect state for receiver
let isSpotReconnecting = false;     // Track reconnect state for spot
let isFuturesReconnecting = false;  // Track reconnect state for futures


// --- Logging Function (Control verbosity here) ---
const VERBOSE_LOGGING = false; // SET TO TRUE FOR DEBUGGING
function log(message, level = 'INFO') {
    // Only log ARB_OP and specified INFO/CONNECTION on startup if not verbose
    // Errors are handled by logError
    if (VERBOSE_LOGGING || level === 'ARB_OP' || (level === 'STARTUP' && !VERBOSE_LOGGING)) {
        console.log(`[ArbitrageBot] ${level === 'STARTUP' ? 'INFO' : level}: ${message}`);
    }
}
function logError(message, errorObj = null) {
    console.error(`[ArbitrageBot] ERROR: ${message}`);
    if (errorObj && (VERBOSE_LOGGING || !(errorObj instanceof WebSocket.ErrorEvent))) { // Don't log full WS ErrorEvent stack unless verbose
        console.error(errorObj.stack || errorObj);
    } else if (errorObj && VERBOSE_LOGGING) {
        console.error(errorObj);
    }
}
function logReconnect(serviceName) {
    // Log only the first time it starts reconnecting for a given service
    if (serviceName === "Data Receiver" && !isReceiverReconnecting) {
        console.log(`[ArbitrageBot] WARN: Data Receiver disconnected. Attempting to reconnect...`);
        isReceiverReconnecting = true;
    } else if (serviceName === "Spot" && !isSpotReconnecting) {
        console.log(`[ArbitrageBot] WARN: Binance Spot disconnected. Attempting to reconnect...`);
        isSpotReconnecting = true;
    } else if (serviceName === "Futures" && !isFuturesReconnecting) {
        console.log(`[ArbitrageBot] WARN: Binance Futures disconnected. Attempting to reconnect...`);
        isFuturesReconnecting = true;
    }
}


// --- Global Error Handlers ---
process.on('uncaughtException', (err, origin) => {
    logError(`PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION. Origin: ${origin}`, err);
    cleanupAndExit();
});
process.on('unhandledRejection', (reason, promise) => {
    logError(`PID: ${process.pid} --- FATAL: UNHANDLED PROMISE REJECTION. Reason: ${reason instanceof Error ? reason.message : reason}`, promise);
    cleanupAndExit();
});

function cleanupAndExit() {
    logError(`PID: ${process.pid} --- Exiting due to fatal error...`);
    if (receiverPingIntervalId) { try { clearInterval(receiverPingIntervalId); } catch(e) {/*ignore*/} }
    if (receiverClient) { try { receiverClient.terminate(); } catch (e) { /* ignore */ } }
    if (spotWsClient) { try { spotWsClient.terminate(); } catch (e) { /* ignore */ } }
    if (futuresWsClient) { try { futuresWsClient.terminate(); } catch (e) { /* ignore */ } }
    setTimeout(() => process.exit(1), 500).unref();
}

// --- Connection to data_receiver_server ---
function connectToDataReceiver() {
    if (receiverClient && (receiverClient.readyState === WebSocket.OPEN || receiverClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    log(`Connecting to Data Receiver Server: ${DATA_RECEIVER_INTERNAL_URL}`, 'VERBOSE_CONNECTION');
    receiverClient = new WebSocket(DATA_RECEIVER_INTERNAL_URL);

    receiverClient.on('open', () => {
        log('Connected to Data Receiver Server.', 'VERBOSE_CONNECTION');
        if (isReceiverReconnecting) { // Log if it was previously reconnecting
            console.log('[ArbitrageBot] INFO: Reconnected to Data Receiver Server.');
            isReceiverReconnecting = false;
        }
        if (receiverPingIntervalId) clearInterval(receiverPingIntervalId);
        receiverPingIntervalId = setInterval(() => {
            if (receiverClient && receiverClient.readyState === WebSocket.OPEN) {
                try { receiverClient.ping(() => {}); } catch (e) { logError('Error pinging receiver', e); }
            }
        }, RECEIVER_PING_INTERVAL_MS);
    });

    receiverClient.on('message', (message) => {
        log(`Received message from Data Receiver (unexpected): ${message.toString().substring(0,100)}`, 'VERBOSE_WARN');
    });

    receiverClient.on('error', (err) => { // These are often followed by 'close'
        logError(`Data Receiver WebSocket error: ${err.message}`);
    });

    receiverClient.on('close', () => {
        logReconnect("Data Receiver");
        if (receiverPingIntervalId) { clearInterval(receiverPingIntervalId); receiverPingIntervalId = null; }
        receiverClient = null;
        setTimeout(connectToDataReceiver, RECONNECT_INTERVAL_MS);
    });
}

function sendOpportunityToReceiver(payload) {
    if (receiverClient && receiverClient.readyState === WebSocket.OPEN) {
        try {
            receiverClient.send(JSON.stringify(payload));
        } catch (e) {
            logError('Error sending opportunity to Data Receiver', e);
        }
    } else {
        log('Data Receiver not connected. Opportunity not sent.', 'WARN'); // This is a potentially important warning
    }
}

// --- Binance WebSocket Connection Logic ---
function connectToBinanceStream(name, url, onMessageCallback, statePriceObject) {
    let ws = new WebSocket(url);

    ws.on('open', () => {
        log(`Connected to Binance ${name}.`, 'VERBOSE_CONNECTION');
        if (name === "Spot" && isSpotReconnecting) {
            console.log(`[ArbitrageBot] INFO: Reconnected to Binance Spot.`);
            isSpotReconnecting = false;
        } else if (name === "Futures" && isFuturesReconnecting) {
            console.log(`[ArbitrageBot] INFO: Reconnected to Binance Futures.`);
            isFuturesReconnecting = false;
        }
        statePriceObject.bid = null; statePriceObject.ask = null;
        statePriceObject.timestamp = 0; statePriceObject.eventTime = 0;
    });

    ws.on('message', (data) => { // No logging here by default
        try {
            const message = JSON.parse(data.toString());
            let newBid = null; let newAsk = null;
            if (message.b && message.b.length > 0 && message.a && message.a.length > 0) {
                newBid = parseFloat(message.b[0][0]); newAsk = parseFloat(message.a[0][0]);
            } else if (message.bids && message.bids.length > 0 && message.asks && message.asks.length > 0) {
                newBid = parseFloat(message.bids[0][0]); newAsk = parseFloat(message.asks[0][0]);
            }
            if (newBid !== null && newAsk !== null && !isNaN(newBid) && !isNaN(newAsk)) {
                statePriceObject.bid = newBid; statePriceObject.ask = newAsk;
                statePriceObject.timestamp = Date.now();
                if (message.E) { statePriceObject.eventTime = message.E; }
                onMessageCallback();
            }
        } catch (e) {
            logError(`MsgProcErr ${name[0]}: ${e.message.substring(0,30)}`, VERBOSE_LOGGING ? e : null); // Short error for msg proc
        }
    });

    ws.on('error', (err) => { // These are often followed by 'close'
        logError(`Binance ${name} WebSocket error: ${err.message}`);
    });

    ws.on('close', () => {
        logReconnect(name);
        if (name === "Spot") spotWsClient = null; else futuresWsClient = null;
        setTimeout(() => {
            if (name === "Spot") spotWsClient = connectToBinanceStream(name, url, onMessageCallback, statePriceObject);
            else futuresWsClient = connectToBinanceStream(name, url, onMessageCallback, statePriceObject);
        }, RECONNECT_INTERVAL_MS);
    });
    return ws;
}

// --- Arbitrage Check Logic ---
function checkForArbitrage() {
    if (spotPrice.bid === null || spotPrice.ask === null || 
        futuresPrice.bid === null || futuresPrice.ask === null) {
        return;
    }

    const S_ask = spotPrice.ask; const S_bid = spotPrice.bid;
    const F_ask = futuresPrice.ask; const F_bid = futuresPrice.bid;

    const spotBuyCostWithFee = S_ask * (1 + SPOT_FEE_RATE);
    const futuresSellRevenueAfterFee = F_bid * (1 - FUTURES_FEE_RATE);
    const profit1 = futuresSellRevenueAfterFee - spotBuyCostWithFee;

    if (profit1 > MIN_PROFIT_THRESHOLD_USD) {
        const payloadToReceiver = {
            "type": "ARBITRAGE_OP", "opportunityType": "FUTURES_OVERPRICED", "spotAction": "BUY",
            "netDifference": (F_bid - S_ask).toFixed(DECIMAL_PLACES_PRICE),
            "estimatedProfit": profit1.toFixed(DECIMAL_PLACES_PROFIT)
        };
        sendOpportunityToReceiver(payloadToReceiver);
        log(`F_OVERPRICED | Profit: ${profit1.toFixed(DECIMAL_PLACES_PROFIT)} | Sent: ${JSON.stringify(payloadToReceiver)}`, 'ARB_OP');
    }

    const spotSellRevenueAfterFee = S_bid * (1 - SPOT_FEE_RATE);
    const futuresBuyCostWithFee = F_ask * (1 + FUTURES_FEE_RATE);
    const profit2 = spotSellRevenueAfterFee - futuresBuyCostWithFee;

    if (profit2 > MIN_PROFIT_THRESHOLD_USD) {
        const payloadToReceiver = {
            "type": "ARBITRAGE_OP", "opportunityType": "FUTURES_UNDERPRICED", "spotAction": "SELL",
            "netDifference": (S_bid - F_ask).toFixed(DECIMAL_PLACES_PRICE),
            "estimatedProfit": profit2.toFixed(DECIMAL_PLACES_PROFIT)
        };
        sendOpportunityToReceiver(payloadToReceiver);
        log(`F_UNDERPRICED | Profit: ${profit2.toFixed(DECIMAL_PLACES_PROFIT)} | Sent: ${JSON.stringify(payloadToReceiver)}`, 'ARB_OP');
    }
}

// --- Main Application Start ---
function main() {
    console.log(`[ArbitrageBot] STARTUP: PID: ${process.pid} --- Arbitrage Bot (Client to Data Receiver Mode)`); // Always log this
    console.log(`[ArbitrageBot] STARTUP: Config - Spot: ${SPOT_SYMBOL}, Futures: ${FUTURES_SYMBOL}, Min Profit: $${MIN_PROFIT_THRESHOLD_USD.toFixed(2)}`); // Always log this

    connectToDataReceiver();
    spotWsClient = connectToBinanceStream("Spot", SPOT_STREAM_URL, checkForArbitrage, spotPrice);
    futuresWsClient = connectToBinanceStream("Futures", FUTURES_STREAM_URL, checkForArbitrage, futuresPrice);
    // log("Initial connections initiated. Bot is running...", 'STARTUP'); // Can be removed for even less noise
}

main();
