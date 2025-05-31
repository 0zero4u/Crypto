// binance_listener.js (Combined BookTicker & AggTrade Logic - Simplified Payload & Reduced Logging)

const WebSocket = require('ws');

// --- Global Error Handlers ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`);
    console.error(err.stack || err);
    console.error(`[Listener] Exception origin: ${origin}`);
    console.error(`[Listener] PID: ${process.pid} --- Exiting due to uncaught exception...`);
    cleanupAndExit(1);
});

process.on('unhandledRejection', (reason, promise) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNHANDLED PROMISE REJECTION`);
    console.error('[Listener] Unhandled Rejection at:', promise);
    console.error('[Listener] Reason:', reason instanceof Error ? reason.stack : reason);
    console.error(`[Listener] PID: ${process.pid} --- Exiting due to unhandled promise rejection...`);
    cleanupAndExit(1);
});

function cleanupAndExit(exitCode = 1) {
    console.log(`[Listener] PID: ${process.pid} --- Initiating cleanup...`); // Keep this log for shutdown
    const clientsToTerminate = [internalWsClient, bookTickerWsClient, aggTradeWsClient];
    clientsToTerminate.forEach(client => {
        if (client && typeof client.terminate === 'function') {
            try { client.terminate(); } catch (e) { console.error(`[Listener] Error terminating a WebSocket client: ${e.message}`); }
        }
    });

    const intervalsToClear = [bookTickerPingIntervalId, aggTradePingIntervalId, flagExpiryCheckIntervalId];
    intervalsToClear.forEach(intervalId => {
        if (intervalId) { try { clearInterval(intervalId); } catch(e) { /* ignore */ } }
    });

    setTimeout(() => {
        console.log(`[Listener] PID: ${process.pid} --- Exiting with code ${exitCode}.`); // Keep this log for shutdown
        process.exit(exitCode);
    }, 1000).unref();
}


// --- Listener Configuration ---
const SYMBOL = 'btcusdt';
const bookTickerStreamUrl = `wss://stream.binance.com:9443/ws/${SYMBOL}@bookTicker`;
const aggTradeStreamUrl = `wss://stream.binance.com:9443/ws/${SYMBOL}@aggTrade`;
const internalReceiverUrl = 'ws://localhost:8082';
const RECONNECT_INTERVAL_MS = 5000;
const BINANCE_PING_INTERVAL_MS = 3 * 60 * 1000;
const FLAG_EXPIRY_CHECK_INTERVAL_MS = 20;

// --- Tunable Parameters ---
const MIN_STABLE_TICKS_FOR_ALERT_BT = 2;
const CRITICAL_LOW_ASK_QTY_THRESHOLD_BT_BTC = 0.005;
const CRITICAL_LOW_BID_QTY_THRESHOLD_BT_BTC = 0.005;
const MIN_DEPLETION_PERCENT_FROM_INITIAL_BT = 0.60;
const MIN_AGG_TRADE_CONFIRMATION_QTY_BTC = 0.02;
const MAX_TIME_WINDOW_FOR_CONFIRMATION_MS = 100;

// --- Listener State Variables ---
let bookTickerWsClient = null;
let aggTradeWsClient = null;
let internalWsClient = null;

let bookTickerPingIntervalId = null;
let aggTradePingIntervalId = null;
let flagExpiryCheckIntervalId = null;

let prev_b_bt = null, prev_B_bt = null, prev_a_bt = null, prev_A_bt = null;
let ask_price_stable_count_bt = 0;
let initial_A_at_stable_price_bt = null;
let bid_price_stable_count_bt = 0;
let initial_B_at_stable_price_bt = null;

let potential_lift_level_bt = null;
let potential_lift_flag_time = null;
let potential_sweep_level_bt = null;
let potential_sweep_flag_time = null;

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    // console.log(`[Listener] PID: ${process.pid} --- Connecting to internal receiver at ${internalReceiverUrl}...`); // Reduced
    internalWsClient = new WebSocket(internalReceiverUrl);

    internalWsClient.on('open', () => {
        // console.log(`[Listener] PID: ${process.pid} --- Connected to internal receiver.`); // Reduced
    });

    internalWsClient.on('error', (err) => {
        console.error(`[Listener] PID: ${process.pid} --- Internal receiver WebSocket error:`, err.message);
    });

    internalWsClient.on('close', (code, reason) => {
        // console.log(`[Listener] PID: ${process.pid} --- Disconnected from internal receiver. Code: ${code}, Reason: ${reason ? reason.toString() : 'N/A'}`); // Reduced
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
}

// --- Data Extraction Functions ---
function extractBookTickerData(messageString) {
    try {
        const data = JSON.parse(messageString);
        if (!data || typeof data.b !== 'string' || typeof data.B !== 'string' || typeof data.a !== 'string' || typeof data.A !== 'string') {
            console.warn(`[Listener] Invalid bookTicker data structure: ${messageString.substring(0, 100)}...`); // Keep warning
            return null;
        }
        return {
            price_bid: parseFloat(data.b),
            qty_bid: parseFloat(data.B),
            price_ask: parseFloat(data.a),
            qty_ask: parseFloat(data.A),
            update_id: data.u,
            event_time: data.E || Date.now()
        };
    } catch (error) {
        console.error(`[Listener] Error parsing bookTicker JSON: ${error.message}. Data: ${messageString.substring(0,100)}...`);
        return null;
    }
}

function extractAggTradeData(messageString) {
    try {
        const data = JSON.parse(messageString);
        if (!data || typeof data.p !== 'string' || typeof data.q !== 'string' || typeof data.m !== 'boolean') {
            console.warn(`[Listener] Invalid aggTrade data structure: ${messageString.substring(0, 100)}...`); // Keep warning
            return null;
        }
        return {
            price: parseFloat(data.p),
            quantity: parseFloat(data.q),
            isBuyerMaker: data.m,
            trade_time: data.T,
            event_time: data.E
        };
    } catch (error) {
        console.error(`[Listener] Error parsing aggTrade JSON: ${error.message}. Data: ${messageString.substring(0,100)}...`);
        return null;
    }
}

// --- Signal Processing ---
function processBookTickerUpdate(curr_bt) {
    if (!curr_bt) return;
    const currentTime = Date.now();

    // --- Ask Side Logic (Potential Lift) ---
    if (prev_a_bt !== null) {
        if (curr_bt.price_ask === prev_a_bt) {
            ask_price_stable_count_bt++;
        } else {
            ask_price_stable_count_bt = 1;
            initial_A_at_stable_price_bt = curr_bt.qty_ask;
        }

        if (ask_price_stable_count_bt >= MIN_STABLE_TICKS_FOR_ALERT_BT && curr_bt.qty_ask < prev_A_bt) {
            const depletion_ratio_bt = (initial_A_at_stable_price_bt - curr_bt.qty_ask) / initial_A_at_stable_price_bt;

            if (depletion_ratio_bt >= MIN_DEPLETION_PERCENT_FROM_INITIAL_BT || curr_bt.qty_ask <= CRITICAL_LOW_ASK_QTY_THRESHOLD_BT_BTC) {
                potential_lift_level_bt = curr_bt.price_ask;
                potential_lift_flag_time = currentTime;
            }
        }
    } else {
        ask_price_stable_count_bt = 1;
        initial_A_at_stable_price_bt = curr_bt.qty_ask;
    }
    prev_a_bt = curr_bt.price_ask;
    prev_A_bt = curr_bt.qty_ask;

    // --- Bid Side Logic (Potential Sweep) ---
    if (prev_b_bt !== null) {
        if (curr_bt.price_bid === prev_b_bt) {
            bid_price_stable_count_bt++;
        } else {
            bid_price_stable_count_bt = 1;
            initial_B_at_stable_price_bt = curr_bt.qty_bid;
        }

        if (bid_price_stable_count_bt >= MIN_STABLE_TICKS_FOR_ALERT_BT && curr_bt.qty_bid < prev_B_bt) {
            const depletion_ratio_bt = (initial_B_at_stable_price_bt - curr_bt.qty_bid) / initial_B_at_stable_price_bt;

            if (depletion_ratio_bt >= MIN_DEPLETION_PERCENT_FROM_INITIAL_BT || curr_bt.qty_bid <= CRITICAL_LOW_BID_QTY_THRESHOLD_BT_BTC) {
                potential_sweep_level_bt = curr_bt.price_bid;
                potential_sweep_flag_time = currentTime;
            }
        }
    } else {
        bid_price_stable_count_bt = 1;
        initial_B_at_stable_price_bt = curr_bt.qty_bid;
    }
    prev_b_bt = curr_bt.price_bid;
    prev_B_bt = curr_bt.qty_bid;
}

function processAggTradeUpdate(curr_at) {
    if (!curr_at) return;
    const currentTime = Date.now();

    // --- Check for Lift Confirmation ---
    if (potential_lift_level_bt !== null &&
        (currentTime - potential_lift_flag_time) <= MAX_TIME_WINDOW_FOR_CONFIRMATION_MS) {

        if (curr_at.price === potential_lift_level_bt &&
            !curr_at.isBuyerMaker &&
            curr_at.quantity >= MIN_AGG_TRADE_CONFIRMATION_QTY_BTC) {

            const signal = {
                type: "CONFIRMED_LIFT",
                price: potential_lift_level_bt,
                quantity: curr_at.quantity,
                symbol: SYMBOL
            };
            // console.log(`[Listener] STRONG SIGNAL: ${signal.type} at ${signal.price} (AggQty: ${signal.quantity})`); // Reduced
            sendToInternalClient(signal);
            potential_lift_level_bt = null;
            potential_lift_flag_time = null;
        }
    }

    // --- Check for Sweep Confirmation ---
    if (potential_sweep_level_bt !== null &&
        (currentTime - potential_sweep_flag_time) <= MAX_TIME_WINDOW_FOR_CONFIRMATION_MS) {

        if (curr_at.price === potential_sweep_level_bt &&
            curr_at.isBuyerMaker &&
            curr_at.quantity >= MIN_AGG_TRADE_CONFIRMATION_QTY_BTC) {

            const signal = {
                type: "CONFIRMED_SWEEP",
                price: potential_sweep_level_bt,
                quantity: curr_at.quantity,
                symbol: SYMBOL
            };
            // console.log(`[Listener] STRONG SIGNAL: ${signal.type} at ${signal.price} (AggQty: ${signal.quantity})`); // Reduced
            sendToInternalClient(signal);
            potential_sweep_level_bt = null;
            potential_sweep_flag_time = null;
        }
    }
}

function sendToInternalClient(payload) {
    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        try {
            internalWsClient.send(JSON.stringify(payload));
        } catch (sendError) {
            console.error(`[Listener] PID: ${process.pid} --- Error sending data to internal receiver:`, sendError.message);
        }
    } else {
        console.warn(`[Listener] PID: ${process.pid} --- Internal client not open, cannot send signal: ${JSON.stringify(payload)}`); // Keep warning
    }
}

function checkAndExpireFlags() {
    const currentTime = Date.now();
    if (potential_lift_level_bt !== null && (currentTime - potential_lift_flag_time) > MAX_TIME_WINDOW_FOR_CONFIRMATION_MS) {
        // console.log(`[Listener] Expired potential_lift_level_bt: ${potential_lift_level_bt} (flagged at ${new Date(potential_lift_flag_time).toISOString()})`); // Reduced
        potential_lift_level_bt = null;
        potential_lift_flag_time = null;
    }
    if (potential_sweep_level_bt !== null && (currentTime - potential_sweep_flag_time) > MAX_TIME_WINDOW_FOR_CONFIRMATION_MS) {
        // console.log(`[Listener] Expired potential_sweep_level_bt: ${potential_sweep_level_bt} (flagged at ${new Date(potential_sweep_flag_time).toISOString()})`); // Reduced
        potential_sweep_level_bt = null;
        potential_sweep_flag_time = null;
    }
}

// --- Binance Stream Connection (BookTicker) ---
function connectToBookTickerStream() {
    if (bookTickerWsClient && (bookTickerWsClient.readyState === WebSocket.OPEN || bookTickerWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    // console.log(`[Listener] PID: ${process.pid} --- Connecting to Binance BookTicker stream (${bookTickerStreamUrl})...`); // Reduced
    bookTickerWsClient = new WebSocket(bookTickerStreamUrl);

    bookTickerWsClient.on('open', function open() {
        // console.log(`[Listener] PID: ${process.pid} --- Connected to Binance BookTicker stream.`); // Reduced
        prev_b_bt = null; prev_B_bt = null; prev_a_bt = null; prev_A_bt = null;
        ask_price_stable_count_bt = 0; initial_A_at_stable_price_bt = null;
        bid_price_stable_count_bt = 0; initial_B_at_stable_price_bt = null;

        if (bookTickerPingIntervalId) clearInterval(bookTickerPingIntervalId);
        bookTickerPingIntervalId = setInterval(() => {
            if (bookTickerWsClient && bookTickerWsClient.readyState === WebSocket.OPEN) {
                try {
                    bookTickerWsClient.ping(() => {});
                } catch (pingError) {
                    console.error(`[Listener] PID: ${process.pid} --- Error sending ping to BookTicker:`, pingError.message);
                }
            }
        }, BINANCE_PING_INTERVAL_MS);
    });

    bookTickerWsClient.on('message', function incoming(data) {
        try {
            const messageString = data.toString();
            const parsedData = extractBookTickerData(messageString);
            if (parsedData) {
                processBookTickerUpdate(parsedData);
            }
        } catch (e) {
            console.error(`[Listener] PID: ${process.pid} --- CRITICAL ERROR in BookTicker message handler:`, e.message, e.stack);
        }
    });

    bookTickerWsClient.on('pong', () => {});

    bookTickerWsClient.on('error', function error(err) {
        console.error(`[Listener] PID: ${process.pid} --- Binance BookTicker WebSocket error:`, err.message);
    });

    bookTickerWsClient.on('close', function close(code, reason) {
        // console.log(`[Listener] PID: ${process.pid} --- Binance BookTicker WebSocket closed. Code: ${code}, Reason: ${reason ? reason.toString() : 'N/A'}`); // Reduced
        if (bookTickerPingIntervalId) { clearInterval(bookTickerPingIntervalId); bookTickerPingIntervalId = null; }
        bookTickerWsClient = null;
        setTimeout(connectToBookTickerStream, RECONNECT_INTERVAL_MS);
    });
}

// --- Binance Stream Connection (AggTrade) ---
function connectToAggTradeStream() {
    if (aggTradeWsClient && (aggTradeWsClient.readyState === WebSocket.OPEN || aggTradeWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    // console.log(`[Listener] PID: ${process.pid} --- Connecting to Binance AggTrade stream (${aggTradeStreamUrl})...`); // Reduced
    aggTradeWsClient = new WebSocket(aggTradeStreamUrl);

    aggTradeWsClient.on('open', function open() {
        // console.log(`[Listener] PID: ${process.pid} --- Connected to Binance AggTrade stream.`); // Reduced
        if (aggTradePingIntervalId) clearInterval(aggTradePingIntervalId);
        aggTradePingIntervalId = setInterval(() => {
            if (aggTradeWsClient && aggTradeWsClient.readyState === WebSocket.OPEN) {
                try {
                    aggTradeWsClient.ping(() => {});
                } catch (pingError) {
                    console.error(`[Listener] PID: ${process.pid} --- Error sending ping to AggTrade:`, pingError.message);
                }
            }
        }, BINANCE_PING_INTERVAL_MS);
    });

    aggTradeWsClient.on('message', function incoming(data) {
        try {
            const messageString = data.toString();
            const parsedData = extractAggTradeData(messageString);
            if (parsedData) {
                processAggTradeUpdate(parsedData);
            }
        } catch (e) {
            console.error(`[Listener] PID: ${process.pid} --- CRITICAL ERROR in AggTrade message handler:`, e.message, e.stack);
        }
    });

    aggTradeWsClient.on('pong', () => {});

    aggTradeWsClient.on('error', function error(err) {
        console.error(`[Listener] PID: ${process.pid} --- Binance AggTrade WebSocket error:`, err.message);
    });

    aggTradeWsClient.on('close', function close(code, reason) {
        // console.log(`[Listener] PID: ${process.pid} --- Binance AggTrade WebSocket closed. Code: ${code}, Reason: ${reason ? reason.toString() : 'N/A'}`); // Reduced
        if (aggTradePingIntervalId) { clearInterval(aggTradePingIntervalId); aggTradePingIntervalId = null; }
        aggTradeWsClient = null;
        setTimeout(connectToAggTradeStream, RECONNECT_INTERVAL_MS);
    });
}


// --- Start the connections ---
// Initial startup logs reduced
// console.log(`[Listener] PID: ${process.pid} --- Binance Listener (BookTicker + AggTrade Combined Logic) starting...`);
// console.log(`[Listener] SYMBOL: ${SYMBOL}`);
// console.log(`[Listener] Tunable Parameters: ... `);


connectToInternalReceiver();
connectToBookTickerStream();
connectToAggTradeStream();

if (flagExpiryCheckIntervalId) clearInterval(flagExpiryCheckIntervalId);
flagExpiryCheckIntervalId = setInterval(checkAndExpireFlags, FLAG_EXPIRY_CHECK_INTERVAL_MS);
