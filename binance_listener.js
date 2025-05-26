// binance_listener.js (Modified for @aggTrade, @bookTicker, and Formula-based Signals - Middle Ground Approach)

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
const TICK_SIZE = 0.01; // For BTCUSDT. Adjust for other pairs.
const EPSILON = 1e-9; // To prevent division by zero

// --- New Cooldown Parameters ---
const MIN_OVERALL_SIGNAL_INTERVAL_MS = 5 * 1000;   // Minimum time between ANY signal (e.g., 5 seconds)
const SAME_SIGNAL_COOLDOWN_MS = 15 * 1000;      // Cooldown for sending the SAME type of signal (e.g., 15 seconds)

// --- Relaxed Formula Parameters (TUNE THESE CAREFULLY!) ---
// F1: Offer Lift - Relaxed
const F1_MIN_ASK_PRICE_JUMP_TICKS_RELAXED = 1;
const F1_MAX_NEW_ASK_QTY_AFTER_JUMP_RELAXED = 1.0; // Original: 0.5
const F1_MIN_PREVIOUS_ASK_QTY_BEFORE_JUMP_RELAXED = 0.5; // Original: 1.0
const F1_MIN_SPREAD_BEFORE_EVENT_TICKS_RELAXED = 8; // Original: 5
const F6_TRADE_FLOW_IMBALANCE_THRESHOLD_UP_RELAXED = 0.15; // Original: 0.3

// F2: Bid Drop - Relaxed
const F2_MIN_BID_PRICE_DROP_TICKS_RELAXED = 1;
const F2_MAX_NEW_BID_QTY_AFTER_DROP_RELAXED = 1.0; // Original: 0.5
const F2_MIN_PREVIOUS_BID_QTY_BEFORE_DROP_RELAXED = 0.5; // Original: 1.0
const F2_MIN_SPREAD_BEFORE_EVENT_TICKS_RELAXED = 8; // Original: 5
const F6_TRADE_FLOW_IMBALANCE_THRESHOLD_DOWN_RELAXED = -0.15; // Original: -0.3

// F3: Market Churn (Informational - parameters unchanged unless desired)
const F3_CHURN_WINDOW_UPDATES = 50;
const F3_MAX_PRICE_DEVIATION_CHURN_TICKS = 10;
const F3_MIN_SPREAD_CHURN_TICKS = 1;
const F3_MAX_SPREAD_CHURN_TICKS = 10;
const F3_MIN_BBO_QTY_CHURN = 0.1;

// F4: Thinning Ask Setup (Informational - parameters unchanged unless desired)
const F4_MIN_SPREAD_PRECONDITION_TICKS = 3;
const F4_MAX_ASK_QTY_PRECONDITION = 0.2;
const F4_MIN_BID_QTY_PRECONDITION = 1.0;

// F5: Thinning Bid Setup (Informational - parameters unchanged unless desired)
const F5_MIN_SPREAD_PRECONDITION_TICKS = 3;
const F5_MAX_BID_QTY_PRECONDITION = 0.2;
const F5_MIN_ASK_QTY_PRECONDITION = 1.0;

// F6: Recent Aggregated Trade Flow Imbalance (Window - unchanged unless desired)
const F6_AGG_TRADE_WINDOW_SECONDS = 2;


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

// Signal Cooldown State
let last_signal_sent_time = 0;
let last_signal_sent_type = null; // "BUY" or "SELL"

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
            console.log(`[Listener] SENT SIGNAL: ${signalType} via ${formulaName} at ${price}. Imbalance: ${current_trade_flow_imbalance.toFixed(4)}. Details: ${JSON.stringify(details)}`);
            last_signal_sent_time = Date.now(); // Update time of the last signal
            last_signal_sent_type = signalType; // Update type of the last signal
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

        // --- Modified Cooldown Check ---
        const timeSinceLastSignal = Date.now() - last_signal_sent_time;
        let canSendBuySignal = true;
        let canSendSellSignal = true;

        if (timeSinceLastSignal < MIN_OVERALL_SIGNAL_INTERVAL_MS) {
            canSendBuySignal = false;
            canSendSellSignal = false;
        } else {
            if (last_signal_sent_type === "BUY" && timeSinceLastSignal < SAME_SIGNAL_COOLDOWN_MS) {
                canSendBuySignal = false;
            }
            if (last_signal_sent_type === "SELL" && timeSinceLastSignal < SAME_SIGNAL_COOLDOWN_MS) {
                canSendSellSignal = false;
            }
        }

        // --- Evaluate Informational Formulas (F3, F4, F5) ---
        // F3: Market Churn
        if (bbo_history_deque.length === F3_CHURN_WINDOW_UPDATES) {
            let minHistB = Infinity, maxHistB = -Infinity, minHistA = Infinity, maxHistA = -Infinity;
            let sumHistBQty = 0, sumHistAQty = 0;
            let allSpreadsInTicksGood = true;

            for (const entry of bbo_history_deque) {
                minHistB = Math.min(minHistB, entry.b); maxHistB = Math.max(maxHistB, entry.b);
                minHistA = Math.min(minHistA, entry.a); maxHistA = Math.max(maxHistA, entry.a);
                sumHistBQty += entry.B; sumHistAQty += entry.A;
                const spreadTicks = (entry.a - entry.b) / TICK_SIZE;
                if (spreadTicks < F3_MIN_SPREAD_CHURN_TICKS || spreadTicks > F3_MAX_SPREAD_CHURN_TICKS) {
                    allSpreadsInTicksGood = false;
                }
            }
            const priceDevBTicks = (maxHistB - minHistB) / TICK_SIZE;
            const priceDevATicks = (maxHistA - minHistA) / TICK_SIZE;
            const avgHistBQty = sumHistBQty / F3_CHURN_WINDOW_UPDATES;
            const avgHistAQty = sumHistAQty / F3_CHURN_WINDOW_UPDATES;

            if (priceDevBTicks <= F3_MAX_PRICE_DEVIATION_CHURN_TICKS &&
                priceDevATicks <= F3_MAX_PRICE_DEVIATION_CHURN_TICKS &&
                allSpreadsInTicksGood &&
                avgHistBQty >= F3_MIN_BBO_QTY_CHURN && avgHistAQty >= F3_MIN_BBO_QTY_CHURN) {
                // console.log(`[Listener] F3 CHURN detected. Imbalance: ${current_trade_flow_imbalance.toFixed(4)}`);
            }
        }

        // F4: Thinning Ask Setup
        const currentSpreadTicksF4 = (current_a - current_b) / TICK_SIZE;
        if (currentSpreadTicksF4 > 0 && currentSpreadTicksF4 <= F4_MIN_SPREAD_PRECONDITION_TICKS &&
            current_A <= F4_MAX_ASK_QTY_PRECONDITION &&
            current_B >= F4_MIN_BID_QTY_PRECONDITION) {
            // console.log(`[Listener] F4 THINNING ASK SETUP detected. Imbalance: ${current_trade_flow_imbalance.toFixed(4)}`);
        }

        // F5: Thinning Bid Setup
        const currentSpreadTicksF5 = (current_a - current_b) / TICK_SIZE;
        if (currentSpreadTicksF5 > 0 && currentSpreadTicksF5 <= F5_MIN_SPREAD_PRECONDITION_TICKS &&
            current_B <= F5_MAX_BID_QTY_PRECONDITION &&
            current_A >= F5_MIN_ASK_QTY_PRECONDITION) {
            // console.log(`[Listener] F5 THINNING BID SETUP detected. Imbalance: ${current_trade_flow_imbalance.toFixed(4)}`);
        }

        // --- Evaluate Actionable Formulas (F1, F2) with Relaxed Params & New Cooldown ---

        // F1: Offer Lift (Relaxed)
        if (canSendBuySignal) {
            const askPriceJumpTicks = (current_a - previous_a) / TICK_SIZE;
            const spreadBeforeTicksF1 = (previous_a - previous_b) / TICK_SIZE;

            if (askPriceJumpTicks >= F1_MIN_ASK_PRICE_JUMP_TICKS_RELAXED &&
                current_A <= F1_MAX_NEW_ASK_QTY_AFTER_JUMP_RELAXED &&
                previous_A >= F1_MIN_PREVIOUS_ASK_QTY_BEFORE_JUMP_RELAXED &&
                spreadBeforeTicksF1 > 0 && spreadBeforeTicksF1 <= F1_MIN_SPREAD_BEFORE_EVENT_TICKS_RELAXED &&
                current_trade_flow_imbalance >= F6_TRADE_FLOW_IMBALANCE_THRESHOLD_UP_RELAXED) {

                sendSignal("BUY", "F1_OfferLift_Relaxed", current_a, eventTime, {
                    askPriceJumpTicks: parseFloat(askPriceJumpTicks.toFixed(2)),
                    newAskQty: current_A,
                    prevAskQty: previous_A,
                    spreadBeforeTicks: parseFloat(spreadBeforeTicksF1.toFixed(2))
                });
                return; // Signal sent, skip F2 for this BBO update
            }
        }

        // F2: Bid Drop (Relaxed)
        if (canSendSellSignal) {
            const bidPriceDropTicks = (current_b - previous_b) / TICK_SIZE; // Will be negative for a drop
            const spreadBeforeTicksF2 = (previous_a - previous_b) / TICK_SIZE;

            if (bidPriceDropTicks <= -F2_MIN_BID_PRICE_DROP_TICKS_RELAXED && // Note the negative
                current_B <= F2_MAX_NEW_BID_QTY_AFTER_DROP_RELAXED &&
                previous_B >= F2_MIN_PREVIOUS_BID_QTY_BEFORE_DROP_RELAXED &&
                spreadBeforeTicksF2 > 0 && spreadBeforeTicksF2 <= F2_MIN_SPREAD_BEFORE_EVENT_TICKS_RELAXED &&
                current_trade_flow_imbalance <= F6_TRADE_FLOW_IMBALANCE_THRESHOLD_DOWN_RELAXED) {

                sendSignal("SELL", "F2_BidDrop_Relaxed", current_b, eventTime, {
                    bidPriceDropTicks: parseFloat(bidPriceDropTicks.toFixed(2)),
                    newBidQty: current_B,
                    prevBidQty: previous_B,
                    spreadBeforeTicks: parseFloat(spreadBeforeTicksF2.toFixed(2))
                });
                return; // Signal sent
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
        // console.log(`[DEBUG] Imbalance: ${current_trade_flow_imbalance.toFixed(3)}, BuyVol: ${aggressive_buy_volume_in_window.toFixed(3)}, SellVol: ${aggressive_sell_volume_in_window.toFixed(3)}`);

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
        // Reset cooldown state on new connection as well
        last_signal_sent_time = 0;
        last_signal_sent_type = null;


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
console.log(`[Listener] Tick Size: ${TICK_SIZE}`);
console.log(`[Listener] Min Overall Signal Interval: ${MIN_OVERALL_SIGNAL_INTERVAL_MS / 1000}s, Same Signal Cooldown: ${SAME_SIGNAL_COOLDOWN_MS / 1000}s`);

connectToInternalReceiver();
connectToBinanceAggTrade();
connectToBinanceBookTicker();

console.log(`[Listener] PID: ${process.pid} --- Initial connection attempts initiated.`);
