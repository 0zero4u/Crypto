// binance_listener.js (Modified for @aggTrade, @bookTicker, and Formula-based Signals)

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
const PREDICTION_COOLDOWN_MS = 30 * 1000; // Cooldown between sending signals
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
// const F6_SOFT_BUY_IMBALANCE_THRESHOLD = 0.1; // For F4 enhancement (optional)
// const F6_SOFT_SELL_IMBALANCE_THRESHOLD = -0.1; // For F5 enhancement (optional)


// --- State Variables ---
let internalWsClient;

let binanceAggTradeWsClient;
let binanceAggTradePingIntervalId;

let binanceBookTickerWsClient;
let binanceBookTickerPingIntervalId;

// BBO State
let current_b = null, current_B = null, current_a = null, current_A = null; // Best bid, bid qty, best ask, ask qty
let previous_b = null, previous_B = null, previous_a = null, previous_A = null;
let bbo_update_time = null;

// Formula 3 State (Churn)
let bbo_history_deque = []; // Stores {b, B, a, A, E: timestamp}

// Formula 6 State (Trade Flow)
let agg_trade_history = []; // Stores {T: timestamp, q: quantity, aggressor: "BUY"|"SELL"}
let aggressive_buy_volume_in_window = 0;
let aggressive_sell_volume_in_window = 0;
let current_trade_flow_imbalance = 0; // From -1 to 1

// Signal Cooldown
let last_signal_sent_time = 0;

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
            signal: signalType, // "BUY" or "SELL"
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
            console.log(`[Listener] SENT SIGNAL: ${signalType} via ${formulaName} at ${price}. Details: ${JSON.stringify(details)} Imbalance: ${current_trade_flow_imbalance.toFixed(4)}`);
            last_signal_sent_time = Date.now();
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
        // bbo: {"u":updateId,"s":symbol,"b":bestBidPrice,"B":bestBidQty,"a":bestAskPrice,"A":bestAskQty, "T": timestamp (Binance custom for bookTicker stream), "E": event time}
        // For @bookTicker, Binance uses "E" for event time in some contexts, but the stream itself might provide 'T' as transaction time or 'E'.
        // The example structure in prompt used "E". Standard @bookTicker has b,B,a,A. Let's assume E or T is available.
        // The common stream format for individual symbol book ticker is {"u":<update_id>,"s":<symbol>,"b":<best_bid_price>,"B":<best_bid_qty>,"a":<best_ask_price>,"A":<best_ask_qty>}
        // It does NOT include 'E' (event time) directly in this message. We should use Date.now() or rely on 'u' for sequencing if needed.
        // Let's use Date.now() for event time for internal consistency if bbo.E is not present.
        // However, if the user's formulas imply using Binance's event time, we need to ensure it's there.
        // The prompt's formula examples used `Date.now()` for F3 history and `trade.T` for F6.
        // Let's use `bbo.u` as a proxy for event progression, and `Date.now()` for actual timestamping for signals.
        // The text mentioned "E: Date.now()" for bbo_history_deque, so this seems fine.
        // The sample @aggTrade uses "E" for event time, "T" for trade time. @bookTicker from stream.binance.com/ws/btcusdt@bookTicker does not have E or T.
        // It has "u" (updateId). Let's use Date.now() for simplicity in this version.

        const eventTime = Date.now(); // Using local time as event time for BBO messages

        previous_b = current_b; previous_B = current_B; previous_a = current_a; previous_A = current_A;

        current_b = parseFloat(bbo.b);
        current_B = parseFloat(bbo.B);
        current_a = parseFloat(bbo.a);
        current_A = parseFloat(bbo.A);
        bbo_update_time = eventTime;


        if (previous_a === null) { // First BBO update, cannot calculate changes yet
            console.log(`[Listener] Initial BBO received: A ${current_a} (${current_A}) / B ${current_b} (${current_B})`);
            return;
        }

        // --- Update BBO History Deque (for F3 Churn) ---
        bbo_history_deque.push({ b: current_b, B: current_B, a: current_a, A: current_A, E: eventTime });
        if (bbo_history_deque.length > F3_CHURN_WINDOW_UPDATES) {
            bbo_history_deque.shift();
        }

        // --- Cooldown Check ---
        const timeSinceLastSignal = Date.now() - last_signal_sent_time;
        const cooldownActive = timeSinceLastSignal < PREDICTION_COOLDOWN_MS;

        // --- Evaluate Formulas (only if not in cooldown for new signals) ---

        // F3: Market Churn (Informational, does not send BUY/SELL signal directly)
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
                // Could set a market state: current_market_state = "CHURN";
            }
        }

        // F4: Thinning Ask Setup (Informational)
        const currentSpreadTicksF4 = (current_a - current_b) / TICK_SIZE;
        if (currentSpreadTicksF4 > 0 && currentSpreadTicksF4 <= F4_MIN_SPREAD_PRECONDITION_TICKS &&
            current_A <= F4_MAX_ASK_QTY_PRECONDITION &&
            current_B >= F4_MIN_BID_QTY_PRECONDITION) {
            // Optional: && (current_B / (current_A + EPSILON)) >= F4_MIN_BID_ASK_QTY_RATIO_PRECONDITION
            // console.log(`[Listener] F4 THINNING ASK SETUP detected. Imbalance: ${current_trade_flow_imbalance.toFixed(4)}`);
            // current_market_state = "POTENTIAL_LIFT_SETUP";
        }

        // F5: Thinning Bid Setup (Informational)
        const currentSpreadTicksF5 = (current_a - current_b) / TICK_SIZE;
        if (currentSpreadTicksF5 > 0 && currentSpreadTicksF5 <= F5_MIN_SPREAD_PRECONDITION_TICKS &&
            current_B <= F5_MAX_BID_QTY_PRECONDITION &&
            current_A >= F5_MIN_ASK_QTY_PRECONDITION) {
            // Optional: && (current_A / (current_B + EPSILON)) >= F5_MIN_ASK_BID_QTY_RATIO_PRECONDITION
            // console.log(`[Listener] F5 THINNING BID SETUP detected. Imbalance: ${current_trade_flow_imbalance.toFixed(4)}`);
            // current_market_state = "POTENTIAL_DROP_SETUP";
        }


        if (cooldownActive) {
            return; // Don't evaluate F1/F2 if in cooldown
        }

        // F1: Offer Lift
        const askPriceJumpTicks = (current_a - previous_a) / TICK_SIZE;
        const spreadBeforeTicksF1 = (previous_a - previous_b) / TICK_SIZE;

        if (askPriceJumpTicks >= F1_MIN_ASK_PRICE_JUMP_TICKS &&
            current_A <= F1_MAX_NEW_ASK_QTY_AFTER_JUMP &&
            previous_A >= F1_MIN_PREVIOUS_ASK_QTY_BEFORE_JUMP &&
            spreadBeforeTicksF1 > 0 && spreadBeforeTicksF1 <= F1_MIN_SPREAD_BEFORE_EVENT_TICKS &&
            current_trade_flow_imbalance >= F6_TRADE_FLOW_IMBALANCE_THRESHOLD_UP) {

            sendSignal("BUY", "F1_OfferLift", current_a, eventTime, {
                askPriceJumpTicks: parseFloat(askPriceJumpTicks.toFixed(2)),
                newAskQty: current_A,
                prevAskQty: previous_A,
                spreadBeforeTicks: parseFloat(spreadBeforeTicksF1.toFixed(2))
            });
            return; // Signal sent, skip F2
        }

        // F2: Bid Drop
        const bidPriceDropTicks = (current_b - previous_b) / TICK_SIZE; // Will be negative for a drop
        const spreadBeforeTicksF2 = (previous_a - previous_b) / TICK_SIZE;

        if (bidPriceDropTicks <= -F2_MIN_BID_PRICE_DROP_TICKS && // Note the negative
            current_B <= F2_MAX_NEW_BID_QTY_AFTER_DROP &&
            previous_B >= F2_MIN_PREVIOUS_BID_QTY_BEFORE_DROP &&
            spreadBeforeTicksF2 > 0 && spreadBeforeTicksF2 <= F2_MIN_SPREAD_BEFORE_EVENT_TICKS &&
            current_trade_flow_imbalance <= F6_TRADE_FLOW_IMBALANCE_THRESHOLD_DOWN) {

            sendSignal("SELL", "F2_BidDrop", current_b, eventTime, {
                bidPriceDropTicks: parseFloat(bidPriceDropTicks.toFixed(2)),
                newBidQty: current_B,
                prevBidQty: previous_B,
                spreadBeforeTicks: parseFloat(spreadBeforeTicksF2.toFixed(2))
            });
            return; // Signal sent
        }

    } catch (error) {
        console.error('[Listener] Error in handleBookTickerMessage:', error.message, error.stack);
        console.error('[Listener] Offending BBO data string:', jsonDataString.substring(0, 200));
    }
}

function handleAggTradeMessage(jsonDataString) {
    try {
        const trade = JSON.parse(jsonDataString);
        // aggTrade: {"e":"aggTrade","E":eventTime,"s":symbol,"a":aggId,"p":price,"q":qty,"f":firstId,"l":lastId,"T":tradeTime,"m":isBuyerMaker,"M":ignore}

        const tradeTime = trade.T;
        const tradeQty = parseFloat(trade.q);
        const aggressor = trade.m ? "SELL" : "BUY"; // trade.m true if buyer is maker -> seller is aggressor (SELL)

        agg_trade_history.push({ T: tradeTime, q: tradeQty, aggressor: aggressor });

        if (aggressor === "BUY") {
            aggressive_buy_volume_in_window += tradeQty;
        } else {
            aggressive_sell_volume_in_window += tradeQty;
        }

        // Prune old trades from history and window sums
        const windowStartTime = tradeTime - (F6_AGG_TRADE_WINDOW_SECONDS * 1000);
        while (agg_trade_history.length > 0 && agg_trade_history[0].T < windowStartTime) {
            const oldTrade = agg_trade_history.shift();
            if (oldTrade.aggressor === "BUY") {
                aggressive_buy_volume_in_window -= oldTrade.q;
            } else {
                aggressive_sell_volume_in_window -= oldTrade.q;
            }
        }
        aggressive_buy_volume_in_window = Math.max(0, aggressive_buy_volume_in_window); // Ensure non-negative
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
        // Reset relevant states if necessary, e.g., trade flow might be reset if desired on new connection.
        // agg_trade_history = []; aggressive_buy_volume_in_window = 0; aggressive_sell_volume_in_window = 0; current_trade_flow_imbalance = 0;

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
        // Reset BBO state on new connection
        current_b = null; current_B = null; current_a = null; current_A = null;
        previous_b = null; previous_B = null; previous_a = null; previous_A = null;
        bbo_history_deque = [];

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
console.log(`[Listener] Tick Size: ${TICK_SIZE}, Cooldown: ${PREDICTION_COOLDOWN_MS / 1000}s`);

connectToInternalReceiver();
connectToBinanceAggTrade();
connectToBinanceBookTicker();

console.log(`[Listener] PID: ${process.pid} --- Initial connection attempts initiated.`);