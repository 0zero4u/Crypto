// binance_listener.js (With F1/F2 Confirmed and FEB/FES Early Signals)

const WebSocket = require('ws');

// --- Global Error Handlers ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[Listener] FATAL: UNCAUGHT EXCEPTION`); console.error(err.stack || err);
    console.error(`Exception origin: ${origin}`);
    console.error(`[Listener] Exiting due to uncaught exception...`);
    setTimeout(() => process.exit(1), 1000).unref();
});
process.on('unhandledRejection', (reason, promise) => {
    console.error('[Listener] FATAL: UNHANDLED PROMISE REJECTION'); console.error('Unhandled Rejection at:', promise);
    console.error('Reason:', reason.stack || reason);
});

// --- Configuration ---
const SYMBOL = 'btcusdt';
const binanceAggTradeStreamUrl = `wss://stream.binance.com:9443/ws/${SYMBOL}@aggTrade`;
const binanceBookTickerStreamUrl = `wss://stream.binance.com:9443/ws/${SYMBOL}@bookTicker`;
const internalReceiverUrl = 'ws://localhost:8082';
const RECONNECT_INTERVAL = 5000;
const BINANCE_PING_INTERVAL_MS = 3 * 60 * 1000;

// --- General Formula Parameters ---
const TICK_SIZE = 0.01;
const EPSILON = 1e-9;

// --- F1: Offer Lift (Confirmed BUY) ---
const F1_MIN_ASK_PRICE_JUMP_TICKS = 3;
const F1_MAX_NEW_ASK_QTY_AFTER_JUMP = 0.5;
const F1_MIN_PREVIOUS_ASK_QTY_BEFORE_JUMP = 1.0;
const F1_MIN_SPREAD_BEFORE_EVENT_TICKS = 5;

// --- F2: Bid Drop (Confirmed SELL) ---
const F2_MIN_BID_PRICE_DROP_TICKS = 3;
const F2_MAX_NEW_BID_QTY_AFTER_DROP = 0.5;
const F2_MIN_PREVIOUS_BID_QTY_BEFORE_DROP = 1.0;
const F2_MIN_SPREAD_BEFORE_EVENT_TICKS = 5; // Can be same as F1

// --- F6: Recent Aggregated Trade Flow Imbalance (Used by F1, F2, FEB, FES) ---
const F6_AGG_TRADE_WINDOW_SECONDS = 2;
// For F1/F2 (Confirmed Signals)
const F6_TRADE_FLOW_IMBALANCE_THRESHOLD_UP = 0.3;
const F6_TRADE_FLOW_IMBALANCE_THRESHOLD_DOWN = -0.3;
// For FEB/FES (Early Signals)
const F6_EARLY_IMBALANCE_THRESHOLD_BUY = 0.10; // Softer threshold for early buy
const F6_EARLY_IMBALANCE_THRESHOLD_SELL = -0.10; // Softer threshold for early sell

// --- FEB: Early Bullish (Early BUY) ---
const FEB_TIGHT_SPREAD_TICKS = 3;
const FEB_MIN_BID_QTY = 0.3;          // Minimum bid quantity supporting the move
const FEB_MAX_ASK_QTY = 0.3;          // Maximum (thin) ask quantity
const FEB_MIN_CONSECUTIVE_UPDATES = 2; // How many BBO updates conditions must hold

// --- FES: Early Bearish (Early SELL) ---
const FES_TIGHT_SPREAD_TICKS = 3;    // Can be same as FEB
const FES_MIN_ASK_QTY = 0.3;           // Minimum ask quantity resisting
const FES_MAX_BID_QTY = 0.3;           // Maximum (thin) bid quantity
const FES_MIN_CONSECUTIVE_UPDATES = 2; // How many BBO updates conditions must hold

// --- Informational Formulas (F3, F4, F5 - not directly sending BUY/SELL yet) ---
const F3_CHURN_WINDOW_UPDATES = 50; // ... F3 params ...
// ... (other F3, F4, F5 params if you use them for logging/internal state)

// --- State Variables ---
let internalWsClient;
let binanceAggTradeWsClient, binanceAggTradePingIntervalId;
let binanceBookTickerWsClient, binanceBookTickerPingIntervalId;

// BBO State
let current_b = null, current_B = null, current_a = null, current_A = null;
let previous_b = null, previous_B = null, previous_a = null, previous_A = null;

// F6 Trade Flow State
let agg_trade_history = [];
let aggressive_buy_volume_in_window = 0, aggressive_sell_volume_in_window = 0;
let current_trade_flow_imbalance = 0;

// FEB/FES State
let feb_consecutive_conditions_met_count = 0;
let fes_consecutive_conditions_met_count = 0;

// Signal Cooldown & Type Tracking
let last_signal_sent_time = 0;
let last_signal_type_sent = null; // "BUY", "SELL", "EARLY_BUY", "EARLY_SELL", or null
const SAME_SIGNAL_AWARENESS_INTERVAL_MS = 5000; // Applies to all signal types for now

// (Other state vars: bbo_history_deque for F3 etc.)
let bbo_history_deque = [];


// --- Internal Receiver Connection ---
function connectToInternalReceiver() { /* ... (same as before) ... */
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) return;
    console.log(`[Listener] Connecting to internal receiver: ${internalReceiverUrl}`);
    internalWsClient = new WebSocket(internalReceiverUrl);
    internalWsClient.on('open', () => console.log('[Listener] Connected to internal receiver.'));
    internalWsClient.on('error', (err) => console.error('[Listener] Internal receiver WebSocket error:', err.message));
    internalWsClient.on('close', (code, reason) => {
        const r = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] Internal receiver closed. Code: ${code}, Reason: ${r}. Reconnecting in ${RECONNECT_INTERVAL / 1000}s...`);
        internalWsClient = null; setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL);
    });
}

// --- Signal Sending Function ---
function sendSignal(signalType, formulaName, price, eventTime, details = {}) { /* ... (same as before, ensures last_signal_type_sent is updated) ... */
    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        const signalPayload = {
            type: "SIGNAL", signal: signalType, price: price, timestamp: eventTime, formula: formulaName,
            details: { ...details, currentTradeFlowImbalance: parseFloat(current_trade_flow_imbalance.toFixed(4)) }
        };
        try {
            const signalJsonString = JSON.stringify(signalPayload);
            internalWsClient.send(signalJsonString);
            console.log(`[Listener] SENT SIGNAL: ${signalType} via ${formulaName} at ${price}. Imbalance: ${current_trade_flow_imbalance.toFixed(4)}. Details: ${JSON.stringify(details)}`);
            last_signal_sent_time = Date.now();
            last_signal_type_sent = signalType; // CRITICAL: Store the specific type sent
        } catch (e) { console.error('[Listener] CRITICAL: Error stringifying signal data:', e.message, e.stack); }
    } else { console.warn('[Listener] Internal receiver not open. Signal NOT sent.'); }
}

// --- Message Handlers ---
function handleAggTradeMessage(jsonDataString) { /* ... (same as before, calculates current_trade_flow_imbalance) ... */
    try {
        const trade = JSON.parse(jsonDataString);
        const tradeTime = trade.T, tradeQty = parseFloat(trade.q), aggressor = trade.m ? "SELL" : "BUY";
        agg_trade_history.push({ T: tradeTime, q: tradeQty, aggressor: aggressor });
        if (aggressor === "BUY") aggressive_buy_volume_in_window += tradeQty; else aggressive_sell_volume_in_window += tradeQty;
        const windowStartTime = tradeTime - (F6_AGG_TRADE_WINDOW_SECONDS * 1000);
        while (agg_trade_history.length > 0 && agg_trade_history[0].T < windowStartTime) {
            const oldTrade = agg_trade_history.shift();
            if (oldTrade.aggressor === "BUY") aggressive_buy_volume_in_window -= oldTrade.q; else aggressive_sell_volume_in_window -= oldTrade.q;
        }
        aggressive_buy_volume_in_window = Math.max(0, aggressive_buy_volume_in_window);
        aggressive_sell_volume_in_window = Math.max(0, aggressive_sell_volume_in_window);
        const totalVolume = aggressive_buy_volume_in_window + aggressive_sell_volume_in_window;
        current_trade_flow_imbalance = totalVolume > EPSILON ? (aggressive_buy_volume_in_window - aggressive_sell_volume_in_window) / totalVolume : 0;
    } catch (e) {
        console.error('[Listener] Error in handleAggTradeMessage:', e.message, e.stack);
        console.error('[Listener] Offending aggTrade data string:', jsonDataString.substring(0, 250));
    }
}

function handleBookTickerMessage(jsonDataString) {
    try {
        const bbo = JSON.parse(jsonDataString);
        const eventTime = Date.now();

        previous_b = current_b; previous_B = current_B; previous_a = current_a; previous_A = current_A;
        current_b = parseFloat(bbo.b); current_B = parseFloat(bbo.B);
        current_a = parseFloat(bbo.a); current_A = parseFloat(bbo.A);

        if (previous_a === null) { /* console.log(`[Listener] Initial BBO...`); */ return; }

        // --- Informational F3 (Churn), F4 (Thinning Ask Setup), F5 (Thinning Bid Setup) ---
        // Can be added here for logging or advanced state management if needed
        // Example: bbo_history_deque.push(...); if (bbo_history_deque.length > F3_CHURN_WINDOW_UPDATES) bbo_history_deque.shift();
        // Then evaluate F3, F4, F5 conditions if (bbo_history_deque.length === F3_CHURN_WINDOW_UPDATES) { ... }

        const timeSinceLastSignal = Date.now() - last_signal_sent_time;
        const currentSpreadTicks = (current_a - current_b) / TICK_SIZE;

        // --- Evaluate FEB (Early Bullish) ---
        let feb_conditions_met_this_tick = false;
        if (currentSpreadTicks > 0 && currentSpreadTicks <= FEB_TIGHT_SPREAD_TICKS &&
            current_B >= FEB_MIN_BID_QTY && current_A <= FEB_MAX_ASK_QTY &&
            current_trade_flow_imbalance >= F6_EARLY_IMBALANCE_THRESHOLD_BUY) {
            let initiating_action = (current_a === previous_a && current_A < previous_A) || // Ask absorption
                                  (current_b > previous_b && (current_a === previous_a || (current_a - previous_a) / TICK_SIZE <= 1)) || // Bid stepping up
                                  ((current_a - previous_a) / TICK_SIZE === 1); // Micro ask jump
            if (initiating_action) feb_conditions_met_this_tick = true;
        }
        if (feb_conditions_met_this_tick) feb_consecutive_conditions_met_count++; else feb_consecutive_conditions_met_count = 0;

        if (feb_consecutive_conditions_met_count >= FEB_MIN_CONSECUTIVE_UPDATES) {
            if (last_signal_type_sent !== "EARLY_BUY" && last_signal_type_sent !== "BUY" ||
                ((last_signal_type_sent === "EARLY_BUY" || last_signal_type_sent === "BUY") && timeSinceLastSignal >= SAME_SIGNAL_AWARENESS_INTERVAL_MS)) {
                sendSignal("EARLY_BUY", "FEB_EarlyBullish", current_a, eventTime, { spreadTicks: currentSpreadTicks, askQty: current_A, bidQty: current_B });
                feb_consecutive_conditions_met_count = 0; // Reset after sending
                return; // Sent an early signal, process next BBO
            }
        }

        // --- Evaluate FES (Early Bearish) ---
        let fes_conditions_met_this_tick = false;
        if (currentSpreadTicks > 0 && currentSpreadTicks <= FES_TIGHT_SPREAD_TICKS &&
            current_A >= FES_MIN_ASK_QTY && current_B <= FES_MAX_BID_QTY &&
            current_trade_flow_imbalance <= F6_EARLY_IMBALANCE_THRESHOLD_SELL) {
            let initiating_action = (current_b === previous_b && current_B < previous_B) || // Bid absorption
                                  (current_a < previous_a && (current_b === previous_b || (previous_b - current_b) / TICK_SIZE <= 1)) || // Ask stepping down
                                  ((previous_b - current_b) / TICK_SIZE === 1); // Micro bid drop
            if (initiating_action) fes_conditions_met_this_tick = true;
        }
        if (fes_conditions_met_this_tick) fes_consecutive_conditions_met_count++; else fes_consecutive_conditions_met_count = 0;

        if (fes_consecutive_conditions_met_count >= FES_MIN_CONSECUTIVE_UPDATES) {
            if (last_signal_type_sent !== "EARLY_SELL" && last_signal_type_sent !== "SELL" ||
                ((last_signal_type_sent === "EARLY_SELL" || last_signal_type_sent === "SELL") && timeSinceLastSignal >= SAME_SIGNAL_AWARENESS_INTERVAL_MS)) {
                sendSignal("EARLY_SELL", "FES_EarlyBearish", current_b, eventTime, { spreadTicks: currentSpreadTicks, askQty: current_A, bidQty: current_B });
                fes_consecutive_conditions_met_count = 0; // Reset after sending
                return; // Sent an early signal, process next BBO
            }
        }

        // --- Evaluate F1 (Confirmed Offer Lift - BUY) ---
        const askPriceJumpTicks = (current_a - previous_a) / TICK_SIZE;
        if (askPriceJumpTicks >= F1_MIN_ASK_PRICE_JUMP_TICKS &&
            current_A <= F1_MAX_NEW_ASK_QTY_AFTER_JUMP && previous_A >= F1_MIN_PREVIOUS_ASK_QTY_BEFORE_JUMP &&
            currentSpreadTicks > 0 && currentSpreadTicks <= F1_MIN_SPREAD_BEFORE_EVENT_TICKS && // Using currentSpreadTicks for F1 as well
            current_trade_flow_imbalance >= F6_TRADE_FLOW_IMBALANCE_THRESHOLD_UP) {
            // Cooldown for F1: Allow if last signal was not BUY/EARLY_BUY, or if awareness interval passed for same types.
            if (last_signal_type_sent !== "BUY" && last_signal_type_sent !== "EARLY_BUY" ||
                ((last_signal_type_sent === "BUY" || last_signal_type_sent === "EARLY_BUY") && timeSinceLastSignal >= SAME_SIGNAL_AWARENESS_INTERVAL_MS)) {
                sendSignal("BUY", "F1_OfferLift", current_a, eventTime, {
                    askPriceJumpTicks: parseFloat(askPriceJumpTicks.toFixed(2)), newAskQty: current_A,
                    prevAskQty: previous_A, spreadBeforeTicks: parseFloat(((previous_a - previous_b)/TICK_SIZE).toFixed(2)) // F1 needs spread *before*
                });
                return;
            }
        }

        // --- Evaluate F2 (Confirmed Bid Drop - SELL) ---
        const bidPriceDropTicks = (current_b - previous_b) / TICK_SIZE;
        if (bidPriceDropTicks <= -F2_MIN_BID_PRICE_DROP_TICKS &&
            current_B <= F2_MAX_NEW_BID_QTY_AFTER_DROP && previous_B >= F2_MIN_PREVIOUS_BID_QTY_BEFORE_DROP &&
            currentSpreadTicks > 0 && currentSpreadTicks <= F2_MIN_SPREAD_BEFORE_EVENT_TICKS && // Using currentSpreadTicks for F2 as well
            current_trade_flow_imbalance <= F6_TRADE_FLOW_IMBALANCE_THRESHOLD_DOWN) {
            // Cooldown for F2: Allow if last signal was not SELL/EARLY_SELL, or if awareness interval passed for same types.
            if (last_signal_type_sent !== "SELL" && last_signal_type_sent !== "EARLY_SELL" ||
                ((last_signal_type_sent === "SELL" || last_signal_type_sent === "EARLY_SELL") && timeSinceLastSignal >= SAME_SIGNAL_AWARENESS_INTERVAL_MS)) {
                sendSignal("SELL", "F2_BidDrop", current_b, eventTime, {
                    bidPriceDropTicks: parseFloat(bidPriceDropTicks.toFixed(2)), newBidQty: current_B,
                    prevBidQty: previous_B, spreadBeforeTicks: parseFloat(((previous_a - previous_b)/TICK_SIZE).toFixed(2)) // F2 needs spread *before*
                });
                return;
            }
        }
    } catch (e) {
        console.error('[Listener] Error in handleBookTickerMessage:', e.message, e.stack);
        console.error('[Listener] Offending BBO data string:', jsonDataString.substring(0, 200));
    }
}

// --- Binance @aggTrade Stream Connection ---
function connectToBinanceAggTrade() { /* ... (same as previous "fixed" version) ... */
    if (binanceAggTradeWsClient && (binanceAggTradeWsClient.readyState === WebSocket.OPEN || binanceAggTradeWsClient.readyState === WebSocket.CONNECTING)) return;
    console.log(`[Listener] Connecting to Binance @aggTrade: ${binanceAggTradeStreamUrl}`);
    binanceAggTradeWsClient = new WebSocket(binanceAggTradeStreamUrl);
    binanceAggTradeWsClient.on('open', () => {
        console.log('[Listener] Connected to Binance @aggTrade stream.');
        if (binanceAggTradePingIntervalId) clearInterval(binanceAggTradePingIntervalId);
        binanceAggTradePingIntervalId = setInterval(() => { if (binanceAggTradeWsClient && binanceAggTradeWsClient.readyState === WebSocket.OPEN) binanceAggTradeWsClient.ping(() => {}); }, BINANCE_PING_INTERVAL_MS);
    });
    binanceAggTradeWsClient.on('message', (data) => {
        const msgStr = data.toString();
        if (msgStr.includes('"e":"aggTrade"')) handleAggTradeMessage(msgStr);
        else if (!msgStr.includes('"ping"') && !msgStr.includes('"pong"')) console.warn(`[Listener] Unexpected message on @aggTrade stream: ${msgStr.substring(0,100)}`);
    });
    binanceAggTradeWsClient.on('pong', () => {});
    binanceAggTradeWsClient.on('error', (err) => console.error('[Listener] Binance @aggTrade WebSocket error:', err.message));
    binanceAggTradeWsClient.on('close', (code, reason) => {
        const r = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] Binance @aggTrade WebSocket closed. Code: ${code}, Reason: ${r}. Reconnecting in ${RECONNECT_INTERVAL / 1000}s...`);
        if (binanceAggTradePingIntervalId) { clearInterval(binanceAggTradePingIntervalId); binanceAggTradePingIntervalId = null; }
        binanceAggTradeWsClient = null; agg_trade_history = []; aggressive_buy_volume_in_window = 0; aggressive_sell_volume_in_window = 0; current_trade_flow_imbalance = 0;
        setTimeout(connectToBinanceAggTrade, RECONNECT_INTERVAL);
    });
}

// --- Binance @bookTicker Stream Connection ---
function connectToBinanceBookTicker() { /* ... (same as previous "fixed" version, ensures signal state reset) ... */
    if (binanceBookTickerWsClient && (binanceBookTickerWsClient.readyState === WebSocket.OPEN || binanceBookTickerWsClient.readyState === WebSocket.CONNECTING)) return;
    console.log(`[Listener] Connecting to Binance @bookTicker: ${binanceBookTickerStreamUrl}`);
    binanceBookTickerWsClient = new WebSocket(binanceBookTickerStreamUrl);
    binanceBookTickerWsClient.on('open', () => {
        console.log('[Listener] Connected to Binance @bookTicker stream.');
        current_b = null; current_B = null; current_a = null; current_A = null; previous_b = null; previous_B = null; previous_a = null; previous_A = null;
        bbo_history_deque = []; feb_consecutive_conditions_met_count = 0; fes_consecutive_conditions_met_count = 0;
        last_signal_sent_time = 0; last_signal_type_sent = null; // Reset all relevant signal state
        if (binanceBookTickerPingIntervalId) clearInterval(binanceBookTickerPingIntervalId);
        binanceBookTickerPingIntervalId = setInterval(() => { if (binanceBookTickerWsClient && binanceBookTickerWsClient.readyState === WebSocket.OPEN) binanceBookTickerWsClient.ping(() => {}); }, BINANCE_PING_INTERVAL_MS);
    });
    binanceBookTickerWsClient.on('message', (data) => handleBookTickerMessage(data.toString()));
    binanceBookTickerWsClient.on('pong', () => {});
    binanceBookTickerWsClient.on('error', (err) => console.error('[Listener] Binance @bookTicker WebSocket error:', err.message));
    binanceBookTickerWsClient.on('close', (code, reason) => {
        const r = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] Binance @bookTicker WebSocket closed. Code: ${code}, Reason: ${r}. Reconnecting in ${RECONNECT_INTERVAL / 1000}s...`);
        if (binanceBookTickerPingIntervalId) { clearInterval(binanceBookTickerPingIntervalId); binanceBookTickerPingIntervalId = null; }
        binanceBookTickerWsClient = null; setTimeout(connectToBinanceBookTicker, RECONNECT_INTERVAL);
    });
}

// --- Start the connections ---
console.log(`[Listener] PID: ${process.pid} --- Binance Signal Listener starting for ${SYMBOL.toUpperCase()}`);
console.log(`[Listener] Tick Size: ${TICK_SIZE}, Same Signal Awareness Interval: ${SAME_SIGNAL_AWARENESS_INTERVAL_MS / 1000}s`);
console.log(`[Listener] F1/F2 Imbalance Thresh: ${F6_TRADE_FLOW_IMBALANCE_THRESHOLD_UP}/${F6_TRADE_FLOW_IMBALANCE_THRESHOLD_DOWN}. FEB/FES Imbalance Thresh: ${F6_EARLY_IMBALANCE_THRESHOLD_BUY}/${F6_EARLY_IMBALANCE_THRESHOLD_SELL}`);
connectToInternalReceiver();
connectToBinanceAggTrade();
connectToBinanceBookTicker();
console.log(`[Listener] PID: ${process.pid} --- Initial connection attempts initiated.`);