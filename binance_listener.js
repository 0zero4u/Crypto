// binance_listener.js (Modified for @aggTrade, @bookTicker, and Nuanced Formula-based Signals)

const WebSocket = require('ws');

// --- Global Error Handlers ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[Listener] FATAL: UNCAUGHT EXCEPTION`);
    console.error(err.stack || err);
    console.error(`Exception origin: ${origin}`);
    console.error(`[Listener] Exiting due to uncaught exception...`);
    setTimeout(() => process.exit(1), 1000).unref();
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('[Listener] FATAL: UNHANDLED PROMISE REJECTION');
    console.error('Unhandled Rejection at:', promise);
    console.error('Reason:', reason.stack || reason);
});

// --- Configuration ---
const SYMBOL = 'btcusdt'; // Use lowercase for stream names
const binanceAggTradeStreamUrl = `wss://stream.binance.com:9443/ws/${SYMBOL}@aggTrade`;
const binanceBookTickerStreamUrl = `wss://stream.binance.com:9443/ws/${SYMBOL}@bookTicker`;
const internalReceiverUrl = 'ws://localhost:8082';
const RECONNECT_INTERVAL = 5000; // ms
const BINANCE_PING_INTERVAL_MS = 3 * 60 * 1000; // 3 minutes

// --- Formula Parameters ---
// ** CRITICAL: These parameters need careful tuning! **
const TICK_SIZE = 0.01; // For BTCUSDT. Adjust for other pairs.
const EPSILON = 1e-9; // To prevent division by zero

// F1: Offer Lift
const F1_MIN_ASK_PRICE_JUMP_TICKS = 3;
const F1_MAX_NEW_ASK_QTY_AFTER_JUMP = 0.5;
const F1_MIN_PREVIOUS_ASK_QTY_BEFORE_JUMP = 1.0;
const F1_MIN_SPREAD_BEFORE_EVENT_TICKS = 5; // Max spread in ticks

// F2: Bid Drop
const F2_MIN_BID_PRICE_DROP_TICKS = 3;
const F2_MAX_NEW_BID_QTY_AFTER_DROP = 0.5;
const F2_MIN_PREVIOUS_BID_QTY_BEFORE_DROP = 1.0;
const F2_MIN_SPREAD_BEFORE_EVENT_TICKS = 5; // Max spread in ticks

// F3: Market Churn
const F3_CHURN_WINDOW_UPDATES = 50; // Number of BBO updates in history
const F3_MAX_PRICE_DEVIATION_CHURN_TICKS = 10;
const F3_MIN_SPREAD_CHURN_TICKS = 1;
const F3_MAX_SPREAD_CHURN_TICKS = 10;
const F3_MIN_BBO_QTY_CHURN = 0.1;

// F4: Thinning Ask Setup
const F4_MIN_SPREAD_PRECONDITION_TICKS = 3;
const F4_MAX_ASK_QTY_PRECONDITION = 0.2;
const F4_MIN_BID_QTY_PRECONDITION = 1.0;
// const F4_MIN_BID_ASK_QTY_RATIO_PRECONDITION = 5.0; // Optional

// F5: Thinning Bid Setup
const F5_MIN_SPREAD_PRECONDITION_TICKS = 3;
const F5_MAX_BID_QTY_PRECONDITION = 0.2;
const F5_MIN_ASK_QTY_PRECONDITION = 1.0;
// const F5_MIN_ASK_BID_QTY_RATIO_PRECONDITION = 5.0; // Optional

// F6: Recent Aggregated Trade Flow Imbalance
const F6_AGG_TRADE_WINDOW_SECONDS = 2;
const F6_TRADE_FLOW_IMBALANCE_THRESHOLD_UP = 0.3; // Min imbalance for F1 confirmation
const F6_TRADE_FLOW_IMBALANCE_THRESHOLD_DOWN = -0.3; // Max imbalance for F2 confirmation

// --- State Variables ---
let internalWsClient;

let binanceAggTradeWsClient;
let binanceAggTradePingIntervalId;

let binanceBookTickerWsClient;
let binanceBookTickerPingIntervalId;

// BBO State
let current_b = null, current_B = null, current_a = null, current_A = null;
let previous_b = null, previous_B = null, previous_a = null, previous_A = null;
let bbo_update_time = null;

// Formula 3 State (Churn)
let bbo_history_deque = [];

// Formula 6 State (Trade Flow)
let agg_trade_history = [];
let aggressive_buy_volume_in_window = 0;
let aggressive_sell_volume_in_window = 0;
let current_trade_flow_imbalance = 0;

// Signal Cooldown & Type Tracking (Nuanced Logic)
let last_signal_sent_time = 0;
let last_signal_type_sent = null; // "BUY", "SELL", or null
const SAME_SIGNAL_AWARENESS_INTERVAL_MS = 5000; // 5 seconds for resending same signal type if conditions persist

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    console.log(`[Listener] Connecting to internal receiver: ${internalReceiverUrl}`);
    internalWsClient = new WebSocket(internalReceiverUrl);

    internalWsClient.on('open', () => {
        console.log('[Listener] Connected to internal receiver.');
    });

    internalWsClient.on('error', (err) => {
        console.error('[Listener] Internal receiver WebSocket error:', err.message);
    });

    internalWsClient.on('close', (code, reason) => {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] Internal receiver closed. Code: ${code}, Reason: ${reasonStr}. Reconnecting in ${RECONNECT_INTERVAL / 1000}s...`);
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL);
    });
}

// --- Signal Sending Function ---
function sendSignal(signalType, formulaName, price, eventTime, details = {}) {
    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        const signalPayload = {
            type: "SIGNAL",
            signal: signalType,
            price: price,
            timestamp: eventTime,
            formula: formulaName,
            details: {
                ...details,
                currentTradeFlowImbalance: parseFloat(current_trade_flow_imbalance.toFixed(4))
            }
        };
        try {
            const signalJsonString = JSON.stringify(signalPayload);
            internalWsClient.send(signalJsonString);
            console.log(`[Listener] SENT SIGNAL: ${signalType} via ${formulaName} at ${price}. Imbalance: ${current_trade_flow_imbalance.toFixed(4)}`);
            last_signal_sent_time = Date.now();
            last_signal_type_sent = signalType; // Store the type of signal sent
        } catch (stringifyError) {
            console.error('[Listener] CRITICAL: Error stringifying signal data:', stringifyError.message, stringifyError.stack);
        }
    } else {
        console.warn('[Listener] Internal receiver not open. Signal NOT sent.');
    }
}

// --- Formula Implementations & Message Handlers ---

function handleBookTickerMessage(jsonDataString) {
    try {
        const bbo = JSON.parse(jsonDataString);
        const eventTime = Date.now();

        previous_b = current_b; previous_B = current_B; previous_a = current_a; previous_A = current_A;

        current_b = parseFloat(bbo.b);
        current_B = parseFloat(bbo.B);
        current_a = parseFloat(bbo.a);
        current_A = parseFloat(bbo.A);
        bbo_update_time = eventTime;

        if (previous_a === null) {
            console.log(`[Listener] Initial BBO received: A ${current_a} (${current_A}) / B ${current_b} (${current_B})`);
            return;
        }

        bbo_history_deque.push({ b: current_b, B: current_B, a: current_a, A: current_A, E: eventTime });
        if (bbo_history_deque.length > F3_CHURN_WINDOW_UPDATES) {
            bbo_history_deque.shift();
        }

        // --- Informational Formula Evaluations (F3, F4, F5) - these don't send BUY/SELL signals ---
        if (bbo_history_deque.length === F3_CHURN_WINDOW_UPDATES) {
            // ... (F3 Churn logic as before, potentially logging or setting internal state)
            // console.log(`[Listener] F3 CHURN potential...`);
        }
        // ... (F4 Thinning Ask Setup logic as before)
        // console.log(`[Listener] F4 THINNING ASK potential...`);
        // ... (F5 Thinning Bid Setup logic as before)
        // console.log(`[Listener] F5 THINNING BID potential...`);


        // --- Signal-Generating Formula Evaluations (F1, F2) with Nuanced Cooldown ---
        const timeSinceLastSignal = Date.now() - last_signal_sent_time;

        // F1: Offer Lift (Potential BUY Signal)
        const askPriceJumpTicks = (current_a - previous_a) / TICK_SIZE;
        const spreadBeforeTicksF1 = (previous_a - previous_b) / TICK_SIZE;

        if (askPriceJumpTicks >= F1_MIN_ASK_PRICE_JUMP_TICKS &&
            current_A <= F1_MAX_NEW_ASK_QTY_AFTER_JUMP &&
            previous_A >= F1_MIN_PREVIOUS_ASK_QTY_BEFORE_JUMP &&
            spreadBeforeTicksF1 > 0 && spreadBeforeTicksF1 <= F1_MIN_SPREAD_BEFORE_EVENT_TICKS &&
            current_trade_flow_imbalance >= F6_TRADE_FLOW_IMBALANCE_THRESHOLD_UP) {
            
            // Conditions for BUY signal met. Now check cooldown logic:
            if (last_signal_type_sent !== "BUY" || // Opposing signal (or first signal)
                (last_signal_type_sent === "BUY" && timeSinceLastSignal >= SAME_SIGNAL_AWARENESS_INTERVAL_MS)) { // Same signal, but awareness interval passed
                
                sendSignal("BUY", "F1_OfferLift", current_a, eventTime, {
                    askPriceJumpTicks: parseFloat(askPriceJumpTicks.toFixed(2)),
                    newAskQty: current_A,
                    prevAskQty: previous_A,
                    spreadBeforeTicks: parseFloat(spreadBeforeTicksF1.toFixed(2))
                });
                return; // Signal sent, process next BBO update
            } else if (last_signal_type_sent === "BUY") {
                // console.log("[Listener] F1 BUY condition met, but in awareness cooldown for same signal type.");
            }
        }

        // F2: Bid Drop (Potential SELL Signal)
        // This is checked IF F1 did not trigger OR F1 triggered but was in same-signal cooldown
        const bidPriceDropTicks = (current_b - previous_b) / TICK_SIZE;
        const spreadBeforeTicksF2 = (previous_a - previous_b) / TICK_SIZE;

        if (bidPriceDropTicks <= -F2_MIN_BID_PRICE_DROP_TICKS &&
            current_B <= F2_MAX_NEW_BID_QTY_AFTER_DROP &&
            previous_B >= F2_MIN_PREVIOUS_BID_QTY_BEFORE_DROP &&
            spreadBeforeTicksF2 > 0 && spreadBeforeTicksF2 <= F2_MIN_SPREAD_BEFORE_EVENT_TICKS &&
            current_trade_flow_imbalance <= F6_TRADE_FLOW_IMBALANCE_THRESHOLD_DOWN) {
            
            // Conditions for SELL signal met. Now check cooldown logic:
            if (last_signal_type_sent !== "SELL" || // Opposing signal (or first signal)
                (last_signal_type_sent === "SELL" && timeSinceLastSignal >= SAME_SIGNAL_AWARENESS_INTERVAL_MS)) { // Same signal, but awareness interval passed
                
                sendSignal("SELL", "F2_BidDrop", current_b, eventTime, {
                    bidPriceDropTicks: parseFloat(bidPriceDropTicks.toFixed(2)),
                    newBidQty: current_B,
                    prevBidQty: previous_B,
                    spreadBeforeTicks: parseFloat(spreadBeforeTicksF2.toFixed(2))
                });
                return; // Signal sent, process next BBO update
            } else if (last_signal_type_sent === "SELL") {
                // console.log("[Listener] F2 SELL condition met, but in awareness cooldown for same signal type.");
            }
        }

    } catch (error) {
        console.error('[Listener] Error in handleBookTickerMessage:', error.message, error.stack);
        console.error('[Listener] Offending BBO data string:', jsonDataString.substring(0, 200));
    }
}

function handleAggTradeMessage(jsonDataString) {
    try {
        const trade = JSON.parse(jsonDataString);
        const tradeTime = trade.T;
        const tradeQty = parseFloat(trade.q);
        const aggressor = trade.m ? "SELL" : "BUY";

        agg_trade_history.push({ T: tradeTime, q: tradeQty, aggressor: aggressor });

        if (aggressor === "BUY") {
            aggressive_buy_volume_in_window += tradeQty;
        } else {
            aggressive_sell_volume_in_window += tradeQty;
        }

        const windowStartTime = tradeTime - (F6_AGG_TRADE_WINDOW_SECONDS * 1000);
        while (agg_trade_history.length > 0 && agg_trade_history[0].T < windowStartTime) {
            const oldTrade = agg_trade_history.shift();
            if (oldTrade.aggressor === "BUY") {
                aggressive_buy_volume_in_window -= oldTrade.q;
            } else {
                aggressive_sell_volume_in_window -= oldTrade.q;
            }
        }
        aggressive_buy_volume_in_window = Math.max(0, aggressive_buy_volume_in_window);
        aggressive_sell_volume_in_window = Math.max(0, aggressive_sell_volume_in_window);

        const totalVolume = aggressive_buy_volume_in_window + aggressive_sell_volume_in_window;
        if (totalVolume > EPSILON) {
            current_trade_flow_imbalance = (aggressive_buy_volume_in_window - aggressive_sell_volume_in_window) / totalVolume;
        } else {
            current_trade_flow_imbalance = 0;
        }

    } catch (error) {
        console.error('[Listener] Error in handleAggTradeMessage:', error.message, error.stack);
        console.error('[Listener] Offending aggTrade data string:', jsonDataString.substring(0, 200));
    }
}

// --- Binance @aggTrade Stream Connection ---
function connectToBinanceAggTrade() {
    if (binanceAggTradeWsClient && (binanceAggTradeWsClient.readyState === WebSocket.OPEN || binanceAggTradeWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    console.log(`[Listener] Connecting to Binance @aggTrade: ${binanceAggTradeStreamUrl}`);
    binanceAggTradeWsClient = new WebSocket(binanceAggTradeStreamUrl);

    binanceAggTradeWsClient.on('open', function open() {
        console.log('[Listener] Connected to Binance @aggTrade stream.');
        if (binanceAggTradePingIntervalId) clearInterval(binanceAggTradePingIntervalId);
        binanceAggTradePingIntervalId = setInterval(() => {
            if (binanceAggTradeWsClient && binanceAggTradeWsClient.readyState === WebSocket.OPEN) {
                binanceAggTradeWsClient.ping(() => {});
            }
        }, BINANCE_PING_INTERVAL_MS);
    });

    binanceAggTradeWsClient.on('message', function incoming(data) {
        handleAggTradeMessage(data.toString());
    });

    binanceAggTradeWsClient.on('pong', () => {});

    binanceAggTradeWsClient.on('error', function error(err) {
        console.error('[Listener] Binance @aggTrade WebSocket error:', err.message);
    });

    binanceAggTradeWsClient.on('close', function close(code, reason) {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] Binance @aggTrade WebSocket closed. Code: ${code}, Reason: ${reasonStr}. Reconnecting in ${RECONNECT_INTERVAL / 1000}s...`);
        if (binanceAggTradePingIntervalId) { clearInterval(binanceAggTradePingIntervalId); binanceAggTradePingIntervalId = null; }
        binanceAggTradeWsClient = null;
        setTimeout(connectToBinanceAggTrade, RECONNECT_INTERVAL);
    });
}

// --- Binance @bookTicker Stream Connection ---
function connectToBinanceBookTicker() {
    if (binanceBookTickerWsClient && (binanceBookTickerWsClient.readyState === WebSocket.OPEN || binanceBookTickerWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    console.log(`[Listener] Connecting to Binance @bookTicker: ${binanceBookTickerStreamUrl}`);
    binanceBookTickerWsClient = new WebSocket(binanceBookTickerStreamUrl);

    binanceBookTickerWsClient.on('open', function open() {
        console.log('[Listener] Connected to Binance @bookTicker stream.');
        current_b = null; current_B = null; current_a = null; current_A = null;
        previous_b = null; previous_B = null; previous_a = null; previous_A = null;
        bbo_history_deque = [];
        // Reset signal state on new BBO connection, as context is lost
        last_signal_sent_time = 0;
        last_signal_type_sent = null;

        if (binanceBookTickerPingIntervalId) clearInterval(binanceBookTickerPingIntervalId);
        binanceBookTickerPingIntervalId = setInterval(() => {
            if (binanceBookTickerWsClient && binanceBookTickerWsClient.readyState === WebSocket.OPEN) {
                binanceBookTickerWsClient.ping(() => {});
            }
        }, BINANCE_PING_INTERVAL_MS);
    });

    binanceBookTickerWsClient.on('message', function incoming(data) {
        handleBookTickerMessage(data.toString());
    });

    binanceBookTickerWsClient.on('pong', () => {});

    binanceBookTickerWsClient.on('error', function error(err) {
        console.error('[Listener] Binance @bookTicker WebSocket error:', err.message);
    });

    binanceBookTickerWsClient.on('close', function close(code, reason) {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] Binance @bookTicker WebSocket closed. Code: ${code}, Reason: ${reasonStr}. Reconnecting in ${RECONNECT_INTERVAL / 1000}s...`);
        if (binanceBookTickerPingIntervalId) { clearInterval(binanceBookTickerPingIntervalId); binanceBookTickerPingIntervalId = null; }
        binanceBookTickerWsClient = null;
        setTimeout(connectToBinanceBookTicker, RECONNECT_INTERVAL);
    });
}

// --- Start the connections ---
console.log(`[Listener] PID: ${process.pid} --- Binance Signal Listener starting for ${SYMBOL.toUpperCase()}`);
console.log(`[Listener] Tick Size: ${TICK_SIZE}, Same Signal Awareness Interval: ${SAME_SIGNAL_AWARENESS_INTERVAL_MS / 1000}s`);

connectToInternalReceiver();
connectToBinanceAggTrade();
connectToBinanceBookTicker();

console.log(`[Listener] PID: ${process.pid} --- Initial connection attempts initiated.`);
