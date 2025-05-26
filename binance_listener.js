// binance_listener.js (Refined based on detailed formula specification)

const WebSocket = require('ws');

// --- Global Error Handlers (keep as is) ---
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
const binanceAggTradeStreamUrl = 'wss://stream.binance.com:9443/ws/btcusdt@aggTrade';
const binanceBookTickerStreamUrl = 'wss://stream.binance.com:9443/ws/btcusdt@bookTicker';
const internalReceiverUrl = 'ws://localhost:8082';
const RECONNECT_INTERVAL = 5000;
const BINANCE_PING_INTERVAL_MS = 3 * 60 * 1000;
const PRICE_CHANGE_THRESHOLD = 1.2; // Send data if price changes by this much or more (for non-signal updates)

// --- Core State & Parameters (Global/Shared) ---
const TICK_SIZE = 0.01; // CRITICAL for BTCUSDT
const PREDICTION_COOLDOWN_SECONDS = 10; // Cooldown for signals
const SIGNAL_COOLDOWN_MS = PREDICTION_COOLDOWN_SECONDS * 1000;
const EPSILON = 1e-9; // To avoid division by zero

// --- Formula 1: "Offer Lift" Parameters ---
const F1_MIN_ASK_PRICE_JUMP_TICKS = 10;
const F1_MAX_NEW_ASK_QTY_AFTER_JUMP = 5;
const F1_MIN_PREVIOUS_ASK_QTY_BEFORE_JUMP = 10;
const F1_MAX_SPREAD_BEFORE_EVENT_TICKS = 50; // Max spread (in ticks) before the event for it to be valid
const F1_MIN_SPREAD_VALUE_BEFORE_EVENT = 0.01; // Minimum absolute spread value (e.g. previous_a - previous_b > 0)
// Threshold for Formula 6 enhancement for F1
const F1_F6_TRADE_FLOW_IMBALANCE_THRESHOLD_UP = 0.6; // e.g., 60% buy flow needed

// --- Formula 2: "Bid Drop" Parameters ---
const F2_MIN_BID_PRICE_DROP_TICKS = 10;
const F2_MAX_NEW_BID_QTY_AFTER_DROP = 5;
const F2_MIN_PREVIOUS_BID_QTY_BEFORE_DROP = 10;
const F2_MAX_SPREAD_BEFORE_EVENT_TICKS = 50; // Max spread (in ticks) before the event
const F2_MIN_SPREAD_VALUE_BEFORE_EVENT = 0.01; // Minimum absolute spread value
// Threshold for Formula 6 enhancement for F2
const F2_F6_TRADE_FLOW_IMBALANCE_THRESHOLD_DOWN = -0.6; // e.g., 60% sell flow needed

// --- Formula 3: "Market Churn" Parameters ---
const F3_CHURN_WINDOW_UPDATES = 50; // Number of BBO updates for history
const F3_MAX_PRICE_DEVIATION_CHURN_TICKS = 20; // Max BBO price change (b or a) in window
const F3_MIN_SPREAD_CHURN_TICKS = 1;  // Min spread in ticks during churn
const F3_MAX_SPREAD_CHURN_TICKS = 15; // Max spread in ticks during churn
const F3_MIN_BBO_QTY_CHURN = 0.5;    // Min average liquidity on both sides (B and A) in window
// Thresholds for Formula 6 if F3 churn conditions met, to decide direction
const F3_F6_IMBALANCE_THRESHOLD_UP = 0.5;
const F3_F6_IMBALANCE_THRESHOLD_DOWN = -0.5;

// --- Formula 4: "Precondition for Offer Lift - Thinning Ask" Parameters ---
const F4_MAX_SPREAD_PRECONDITION_TICKS = 5; // Max spread (in ticks) for this setup
const F4_MIN_SPREAD_VALUE_PRECONDITION = 0.01; // Min absolute spread value
const F4_MAX_ASK_QTY_PRECONDITION = 0.5;
const F4_MIN_BID_QTY_PRECONDITION = 2;
const F4_MIN_BID_ASK_QTY_RATIO_PRECONDITION = 3.0; // Optional: current_B / current_A
// Threshold for Formula 6 enhancement (soft imbalance)
const F4_F6_SOFT_BUY_IMBALANCE_THRESHOLD = 0.3;

// --- Formula 5: "Precondition for Bid Drop - Thinning Bid" Parameters ---
const F5_MAX_SPREAD_PRECONDITION_TICKS = 5; // Max spread (in ticks) for this setup
const F5_MIN_SPREAD_VALUE_PRECONDITION = 0.01; // Min absolute spread value
const F5_MAX_BID_QTY_PRECONDITION = 0.5;
const F5_MIN_ASK_QTY_PRECONDITION = 2;
const F5_MIN_ASK_BID_QTY_RATIO_PRECONDITION = 3.0; // Optional: current_A / current_B
// Threshold for Formula 6 enhancement (soft imbalance)
const F5_F6_SOFT_SELL_IMBALANCE_THRESHOLD = -0.3;

// --- Formula 6: "Recent Aggregated Trade Flow Imbalance" Parameters ---
const F6_AGG_TRADE_WINDOW_SECONDS = 2; // Time window for trade aggregation

// --- State Variables ---
let aggTradeWsClient;
let bookTickerWsClient;
let internalWsClient;
let aggTradePingIntervalId;
let bookTickerPingIntervalId;

let lastSentPrice = null;
let lastSignalTime = 0; // Renamed from last_prediction_time

// BBO State (Hot Variables)
let current_b = 0, current_B = 0, current_a = 0, current_A = 0;
let previous_b = 0, previous_B = 0, previous_a = 0, previous_A = 0;

// Deques for Formulas
const bbo_history_deque = []; // Stores {b, B, a, A, E: timestamp}
const agg_trade_history = []; // Stores {T: tradeTime, q: qty, agg: 'BUY'/'SELL', E: eventTime, p: price}

// F6 State (Hot Variables)
let aggressive_buy_volume_in_window = 0;
let aggressive_sell_volume_in_window = 0;
let current_trade_flow_imbalance = 0; // Output of Formula 6, used by other formulas

// --- Internal Receiver Connection (no changes from previous version) ---
function connectToInternalReceiver() { /* ... same as before ... */ }
// --- Manual Data Extraction (manualExtractMinimalAggTrade, manualExtractBookTicker - same as before) ---
function manualExtractMinimalAggTrade(messageString) { /* ... same as before ... */ }
function manualExtractBookTicker(messageString) { /* ... same as before ... */ }


// --- Formula 6: Update Trade Flow Imbalance (from @aggTrade) ---
function updateTradeFlowImbalance(trade) { // trade is {E, p, T, q, m} from manualExtractMinimalAggTrade
    const tradeTime = trade.T; // Trade execution time from Binance
    const tradeQty = parseFloat(trade.q);
    if (isNaN(tradeQty) || tradeQty <= 0) return;

    // aggressor: true if m=false (buyer is taker/aggressor), false if m=true (seller is taker/aggressor)
    const aggressor = trade.m ? 'SELL' : 'BUY';

    // Store necessary info for pruning and volume calculation
    agg_trade_history.push({ T: tradeTime, q: tradeQty, agg: aggressor, E: trade.E, p: trade.p });

    if (aggressor === 'BUY') {
        aggressive_buy_volume_in_window += tradeQty;
    } else { // SELL
        aggressive_sell_volume_in_window += tradeQty;
    }

    // Prune old trades from history and volume sum
    // Use Date.now() as the reference for the window, or trade.E if available and consistent
    const windowStartTime = (trade.E || Date.now()) - (F6_AGG_TRADE_WINDOW_SECONDS * 1000);

    while (agg_trade_history.length > 0 && agg_trade_history[0].T < windowStartTime) {
        const oldTrade = agg_trade_history.shift();
        if (oldTrade.agg === 'BUY') {
            aggressive_buy_volume_in_window -= oldTrade.q;
        } else { // SELL
            aggressive_sell_volume_in_window -= oldTrade.q;
        }
    }
    // Ensure volumes don't go negative due to float precision or timing issues
    aggressive_buy_volume_in_window = Math.max(0, aggressive_buy_volume_in_window);
    aggressive_sell_volume_in_window = Math.max(0, aggressive_sell_volume_in_window);

    const totalVolume = aggressive_buy_volume_in_window + aggressive_sell_volume_in_window;
    if (totalVolume > EPSILON) {
        current_trade_flow_imbalance = (aggressive_buy_volume_in_window - aggressive_sell_volume_in_window) / totalVolume;
    } else {
        current_trade_flow_imbalance = 0;
    }
    // console.log(`[F6] Imbalance: ${current_trade_flow_imbalance.toFixed(3)} (B: ${aggressive_buy_volume_in_window.toFixed(2)}, S: ${aggressive_sell_volume_in_window.toFixed(2)}) Trades: ${agg_trade_history.length}`);
}


// --- Check Formulas (BBO-Driven Formulas F1-F5, enhanced by F6) ---
function checkFormulas() {
    const now = Date.now();
    if (now - lastSignalTime < SIGNAL_COOLDOWN_MS) {
        return null; // Cooldown active
    }

    let signal = null;
    let formulaSource = null;
    let signalPrice = null; // Price at the moment of signal generation

    // Ensure we have previous BBO data to compare against
    if (previous_a <= 0 || previous_b <= 0 || current_a <= 0 || current_b <= 0) {
        return null;
    }

    // --- Formula 1: "Offer Lift" ---
    const askPriceJumpTicks = (current_a - previous_a) / TICK_SIZE;
    const spreadBefore_F1_abs = previous_a - previous_b;
    const spreadBefore_F1_ticks = spreadBefore_F1_abs / TICK_SIZE;

    if (askPriceJumpTicks >= F1_MIN_ASK_PRICE_JUMP_TICKS &&
        current_A <= F1_MAX_NEW_ASK_QTY_AFTER_JUMP &&
        previous_A >= F1_MIN_PREVIOUS_ASK_QTY_BEFORE_JUMP &&
        spreadBefore_F1_abs >= F1_MIN_SPREAD_VALUE_BEFORE_EVENT && // Spread must be positive
        spreadBefore_F1_ticks <= F1_MAX_SPREAD_BEFORE_EVENT_TICKS &&
        current_trade_flow_imbalance >= F1_F6_TRADE_FLOW_IMBALANCE_THRESHOLD_UP) {
        signal = 'BUY'; // Predicts UP
        formulaSource = 'F1_OfferLift';
        signalPrice = current_a; // Signal based on current ask price after jump
    }

    // --- Formula 2: "Bid Drop" (check only if F1 didn't trigger) ---
    if (!signal) {
        const bidPriceDropTicks = (previous_b - current_b) / TICK_SIZE; // Positive if current_b < previous_b
        const spreadBefore_F2_abs = previous_a - previous_b;
        const spreadBefore_F2_ticks = spreadBefore_F2_abs / TICK_SIZE;

        if (bidPriceDropTicks >= F2_MIN_BID_PRICE_DROP_TICKS && // previous_b significantly > current_b
            current_B <= F2_MAX_NEW_BID_QTY_AFTER_DROP &&
            previous_B >= F2_MIN_PREVIOUS_BID_QTY_BEFORE_DROP &&
            spreadBefore_F2_abs >= F2_MIN_SPREAD_VALUE_BEFORE_EVENT && // Spread must be positive
            spreadBefore_F2_ticks <= F2_MAX_SPREAD_BEFORE_EVENT_TICKS &&
            current_trade_flow_imbalance <= F2_F6_TRADE_FLOW_IMBALANCE_THRESHOLD_DOWN) {
            signal = 'SELL'; // Predicts DOWN
            formulaSource = 'F2_BidDrop';
            signalPrice = current_b; // Signal based on current bid price after drop
        }
    }
    
    // --- Formula 3: "Market Churn" (then check imbalance for direction) ---
    if (!signal && bbo_history_deque.length >= F3_CHURN_WINDOW_UPDATES) {
        let minPriceInWindow_b = Infinity, maxPriceInWindow_b = 0;
        let minPriceInWindow_a = Infinity, maxPriceInWindow_a = 0;
        let sumSpreadTicks = 0, sumQtyB = 0, sumQtyA = 0;
        let validSpreadsInChurn = 0;

        const churnWindowSlice = bbo_history_deque.slice(-F3_CHURN_WINDOW_UPDATES);

        for (const entry of churnWindowSlice) {
            minPriceInWindow_b = Math.min(minPriceInWindow_b, entry.b);
            maxPriceInWindow_b = Math.max(maxPriceInWindow_b, entry.b);
            minPriceInWindow_a = Math.min(minPriceInWindow_a, entry.a);
            maxPriceInWindow_a = Math.max(maxPriceInWindow_a, entry.a);
            
            const spreadTicks = (entry.a - entry.b) / TICK_SIZE;
            if (spreadTicks >= F3_MIN_SPREAD_CHURN_TICKS && spreadTicks <= F3_MAX_SPREAD_CHURN_TICKS) {
                sumSpreadTicks += spreadTicks;
                validSpreadsInChurn++;
            }
            sumQtyB += entry.B;
            sumQtyA += entry.A;
        }

        const priceDeviation_b_ticks = (maxPriceInWindow_b - minPriceInWindow_b) / TICK_SIZE;
        const priceDeviation_a_ticks = (maxPriceInWindow_a - minPriceInWindow_a) / TICK_SIZE;
        const avgQtyB = sumQtyB / churnWindowSlice.length;
        const avgQtyA = sumQtyA / churnWindowSlice.length;
        // const avgSpreadTicksInChurn = validSpreadsInChurn > 0 ? sumSpreadTicks / validSpreadsInChurn : Infinity;

        // All BBO updates in window must have spread within F3_MIN/MAX_SPREAD_CHURN_TICKS
        const allSpreadsOk = churnWindowSlice.every(e => {
            const s = (e.a - e.b) / TICK_SIZE;
            return s >= F3_MIN_SPREAD_CHURN_TICKS && s <= F3_MAX_SPREAD_CHURN_TICKS;
        });

        if (priceDeviation_b_ticks <= F3_MAX_PRICE_DEVIATION_CHURN_TICKS &&
            priceDeviation_a_ticks <= F3_MAX_PRICE_DEVIATION_CHURN_TICKS &&
            allSpreadsOk && // Check if all spreads in window were within range
            avgQtyB >= F3_MIN_BBO_QTY_CHURN &&
            avgQtyA >= F3_MIN_BBO_QTY_CHURN) {
            
            // Churn condition met. Now check Formula 6 for direction.
            if (current_trade_flow_imbalance >= F3_F6_IMBALANCE_THRESHOLD_UP) {
                signal = 'BUY';
                formulaSource = 'F3_ChurnBuy';
                signalPrice = current_a;
            } else if (current_trade_flow_imbalance <= F3_F6_IMBALANCE_THRESHOLD_DOWN) {
                signal = 'SELL';
                formulaSource = 'F3_ChurnSell';
                signalPrice = current_b;
            } else {
                // Churn detected, but no strong imbalance. Could be a "STAY" or "NEUTRAL_CHURN" state.
                // console.log(`[F3] Market Churn detected, neutral imbalance. Price: ${current_a}/${current_b}`);
            }
        }
    }

    // --- Formula 4: "Precondition for Offer Lift - Thinning Ask" ---
    if (!signal) {
        const currentSpread_F4_abs = current_a - current_b;
        const currentSpread_F4_ticks = currentSpread_F4_abs / TICK_SIZE;
        const bidAskQtyRatio_F4 = current_A > EPSILON ? current_B / current_A : Infinity;

        if (currentSpread_F4_abs >= F4_MIN_SPREAD_VALUE_PRECONDITION && // Spread must be positive
            currentSpread_F4_ticks <= F4_MAX_SPREAD_PRECONDITION_TICKS &&
            current_A <= F4_MAX_ASK_QTY_PRECONDITION &&
            current_B >= F4_MIN_BID_QTY_PRECONDITION &&
            bidAskQtyRatio_F4 >= F4_MIN_BID_ASK_QTY_RATIO_PRECONDITION && // Optional Ratio Check
            current_trade_flow_imbalance >= F4_F6_SOFT_BUY_IMBALANCE_THRESHOLD) {
            signal = 'BUY'; // Predicts UP
            formulaSource = 'F4_ThinAskSetup';
            signalPrice = current_a;
        }
    }

    // --- Formula 5: "Precondition for Bid Drop - Thinning Bid" ---
    if (!signal) {
        const currentSpread_F5_abs = current_a - current_b;
        const currentSpread_F5_ticks = currentSpread_F5_abs / TICK_SIZE;
        const askBidQtyRatio_F5 = current_B > EPSILON ? current_A / current_B : Infinity;

        if (currentSpread_F5_abs >= F5_MIN_SPREAD_VALUE_PRECONDITION && // Spread must be positive
            currentSpread_F5_ticks <= F5_MAX_SPREAD_PRECONDITION_TICKS &&
            current_B <= F5_MAX_BID_QTY_PRECONDITION &&
            current_A >= F5_MIN_ASK_QTY_PRECONDITION &&
            askBidQtyRatio_F5 >= F5_MIN_ASK_BID_QTY_RATIO_PRECONDITION && // Optional Ratio Check
            current_trade_flow_imbalance <= F5_F6_SOFT_SELL_IMBALANCE_THRESHOLD) {
            signal = 'SELL'; // Predicts DOWN
            formulaSource = 'F5_ThinBidSetup';
            signalPrice = current_b;
        }
    }

    if (signal) {
        lastSignalTime = now;
        console.log(`[Listener] SIGNAL: ${signal} from ${formulaSource} at price ${signalPrice.toFixed(TICK_SIZE === 0.01 ? 2 : (TICK_SIZE === 0.1 ? 1 : 4))}, Imbalance: ${current_trade_flow_imbalance.toFixed(3)}, BBO: ${current_b.toFixed(2)} (${current_B.toFixed(2)}) / ${current_a.toFixed(2)} (${current_A.toFixed(2)})`);
        return { signal, source: formulaSource, price: signalPrice, eventTime: now };
    }
    return null;
}

// --- Send Data to Internal Receiver (no changes from previous version) ---
function sendDataToInternalReceiver(eventData) { /* ... same as before ... */ }
// --- Binance AggTrade Stream Connection (no changes from previous version, uses updateTradeFlowImbalance) ---
function connectToAggTradeStream() { /* ... same as before ... */ }
// --- Binance BookTicker Stream Connection (no changes from previous version, uses checkFormulas) ---
function connectToBookTickerStream() { /* ... same as before ... */ }


// --- Start the connections ---
console.log(`[Listener] PID: ${process.pid} --- Binance listener starting (aggTrade & bookTicker, Price Threshold: ${PRICE_CHANGE_THRESHOLD}, Signal Cooldown: ${SIGNAL_COOLDOWN_MS/1000}s)`);
// Initialize BBO state to prevent NaN issues on first checkFormulas if no bookTicker message received yet
previous_b = 0; previous_B = 0; previous_a = 0; previous_A = 0;
current_b = 0; current_B = 0; current_a = 0; current_A = 0;

connectToAggTradeStream();
connectToBookTickerStream();
connectToInternalReceiver(); // Ensure this is also called
console.log(`[Listener] PID: ${process.pid} --- Initial connection attempts initiated.`);


// --- Helper: Fill in unchanged functions for completeness ---

function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    console.log(`[Listener] Connecting to internal receiver: ${internalReceiverUrl}`);
    internalWsClient = new WebSocket(internalReceiverUrl);

    internalWsClient.on('open', () => {
        console.log('[Listener] Connected to internal receiver.');
        lastSentPrice = null; 
        lastSignalTime = 0; 
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

function manualExtractMinimalAggTrade(messageString) {
    try {
        const outputEventTypeStr = "aggTrade"; 
        let eventTimeNum = null; // E
        let priceStr = null;     // p
        let tradeTimeNum = null; // T
        let quantityStr = null;  // q
        let isBuyerMakerBool = null; // m

        const aggTradeEventTypeKey = '"e":"aggTrade"';
        let currentIndex = messageString.indexOf(aggTradeEventTypeKey);
        if (currentIndex === -1) return null;
        let searchStartIndex = currentIndex + aggTradeEventTypeKey.length;

        const eventTimeKey = '"E":';
        currentIndex = messageString.indexOf(eventTimeKey, searchStartIndex);
        if (currentIndex === -1) return null;
        currentIndex += eventTimeKey.length;
        let valueEndIndex = currentIndex;
        while (valueEndIndex < messageString.length && messageString[valueEndIndex] >= '0' && messageString[valueEndIndex] <= '9') {
            valueEndIndex++;
        }
        if (currentIndex === valueEndIndex) return null;
        eventTimeNum = parseInt(messageString.substring(currentIndex, valueEndIndex), 10);
        if (isNaN(eventTimeNum)) return null;
        searchStartIndex = valueEndIndex;

        const priceKey = '"p":"';
        let priceStartIndex = messageString.indexOf(priceKey, searchStartIndex);
        if (priceStartIndex === -1) return null;
        priceStartIndex += priceKey.length;
        let priceEndIndex = messageString.indexOf('"', priceStartIndex);
        if (priceEndIndex === -1) return null;
        priceStr = messageString.substring(priceStartIndex, priceEndIndex);
        if (priceStr.length === 0 || isNaN(parseFloat(priceStr))) return null;
        searchStartIndex = priceEndIndex;

        const tradeTimeKey = '"T":';
        currentIndex = messageString.indexOf(tradeTimeKey, searchStartIndex);
        if (currentIndex === -1) return null;
        currentIndex += tradeTimeKey.length;
        valueEndIndex = currentIndex;
        while (valueEndIndex < messageString.length && messageString[valueEndIndex] >= '0' && messageString[valueEndIndex] <= '9') {
            valueEndIndex++;
        }
        if (currentIndex === valueEndIndex) return null;
        tradeTimeNum = parseInt(messageString.substring(currentIndex, valueEndIndex), 10);
        if (isNaN(tradeTimeNum)) return null;
        searchStartIndex = valueEndIndex;

        const quantityKey = '"q":"';
        priceStartIndex = messageString.indexOf(quantityKey, searchStartIndex);
        if (priceStartIndex === -1) return null;
        priceStartIndex += quantityKey.length;
        priceEndIndex = messageString.indexOf('"', priceStartIndex);
        if (priceEndIndex === -1) return null;
        quantityStr = messageString.substring(priceStartIndex, priceEndIndex);
        if (quantityStr.length === 0 || isNaN(parseFloat(quantityStr))) return null;
        searchStartIndex = priceEndIndex;

        const buyerMakerKey = '"m":';
        currentIndex = messageString.indexOf(buyerMakerKey, searchStartIndex);
        if (currentIndex === -1) return null;
        currentIndex += buyerMakerKey.length;
        if (messageString.substring(currentIndex, currentIndex + 4) === "true") {
            isBuyerMakerBool = true;
        } else if (messageString.substring(currentIndex, currentIndex + 5) === "false") {
            isBuyerMakerBool = false;
        } else {
            return null;
        }

        return {
            e: outputEventTypeStr, E: eventTimeNum, p: priceStr,
            T: tradeTimeNum, q: quantityStr, m: isBuyerMakerBool
        };
    } catch (error) { return null; }
}

function manualExtractBookTicker(messageString) {
    try {
        if (!messageString.includes('"s":"BTCUSDT"') || !messageString.includes('"b":')) return null;
        let bestBidPriceStr = null, bestBidQtyStr = null, bestAskPriceStr = null, bestAskQtyStr = null;

        const bidPriceKey = '"b":"';
        let startIndex = messageString.indexOf(bidPriceKey);
        if (startIndex === -1) return null;
        startIndex += bidPriceKey.length;
        let endIndex = messageString.indexOf('"', startIndex);
        if (endIndex === -1) return null;
        bestBidPriceStr = messageString.substring(startIndex, endIndex);

        const bidQtyKey = '"B":"';
        startIndex = messageString.indexOf(bidQtyKey, endIndex);
        if (startIndex === -1) return null;
        startIndex += bidQtyKey.length;
        endIndex = messageString.indexOf('"', startIndex);
        if (endIndex === -1) return null;
        bestBidQtyStr = messageString.substring(startIndex, endIndex);

        const askPriceKey = '"a":"';
        startIndex = messageString.indexOf(askPriceKey, endIndex);
        if (startIndex === -1) return null;
        startIndex += askPriceKey.length;
        endIndex = messageString.indexOf('"', startIndex);
        if (endIndex === -1) return null;
        bestAskPriceStr = messageString.substring(startIndex, endIndex);

        const askQtyKey = '"A":"';
        startIndex = messageString.indexOf(askQtyKey, endIndex);
        if (startIndex === -1) return null;
        startIndex += askQtyKey.length;
        endIndex = messageString.indexOf('"', startIndex);
        if (endIndex === -1) return null;
        bestAskQtyStr = messageString.substring(startIndex, endIndex);

        if (isNaN(parseFloat(bestBidPriceStr)) || isNaN(parseFloat(bestBidQtyStr)) ||
            isNaN(parseFloat(bestAskPriceStr)) || isNaN(parseFloat(bestAskQtyStr))) return null;

        return { b: bestBidPriceStr, B: bestBidQtyStr, a: bestAskPriceStr, A: bestAskQtyStr };
    } catch (error) { return null; }
}

function sendDataToInternalReceiver(eventData) {
    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        try {
            const jsonString = JSON.stringify(eventData);
            internalWsClient.send(jsonString);
            if (!eventData.s) { 
                lastSentPrice = parseFloat(eventData.p);
            }
        } catch (stringifyError) {
            console.error('[Listener] CRITICAL: Error stringifying data for internal receiver:', stringifyError.message, eventData);
        }
    } else {
        console.warn('[Listener] Internal receiver not open. Data NOT sent:', eventData);
    }
}

function connectToAggTradeStream() {
    if (aggTradeWsClient && (aggTradeWsClient.readyState === WebSocket.OPEN || aggTradeWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    console.log(`[Listener] Connecting to Binance AggTrade: ${binanceAggTradeStreamUrl}`);
    aggTradeWsClient = new WebSocket(binanceAggTradeStreamUrl);

    aggTradeWsClient.on('open', function open() {
        console.log('[Listener] Connected to Binance aggTrade stream.');
        if (aggTradePingIntervalId) clearInterval(aggTradePingIntervalId);
        aggTradePingIntervalId = setInterval(() => {
            if (aggTradeWsClient && aggTradeWsClient.readyState === WebSocket.OPEN) {
                aggTradeWsClient.ping(() => {});
            }
        }, BINANCE_PING_INTERVAL_MS);
    });

    aggTradeWsClient.on('message', function incoming(data) {
        const messageString = data.toString();
        const tradeData = manualExtractMinimalAggTrade(messageString); 

        if (tradeData) {
            updateTradeFlowImbalance(tradeData); 
            const currentPrice = parseFloat(tradeData.p);
            if (isNaN(currentPrice)) {
                console.warn('[Listener] Invalid price in aggTrade data:', tradeData.p);
                return;
            }

            let shouldSendData = false;
            if (Date.now() - lastSignalTime > SIGNAL_COOLDOWN_MS) { 
                if (lastSentPrice === null) {
                    shouldSendData = true;
                } else {
                    const priceDifference = Math.abs(currentPrice - lastSentPrice);
                    if (priceDifference >= PRICE_CHANGE_THRESHOLD) {
                        shouldSendData = true;
                    }
                }
            }

            if (shouldSendData) {
                sendDataToInternalReceiver({
                    e: "trade", E: tradeData.E, p: tradeData.p
                });
            }
        } else {
             if (messageString && !messageString.includes('"ping"') && !messageString.includes('"pong"')) {
                 console.warn('[Listener] Failed to extract data from aggTrade or unexpected aggTrade message format. Snippet:', messageString.substring(0, 100));
             }
        }
    });

    aggTradeWsClient.on('pong', () => {});
    aggTradeWsClient.on('error', (err) => console.error('[Listener] Binance AggTrade WebSocket error:', err.message));
    aggTradeWsClient.on('close', (code, reason) => {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] Binance AggTrade WebSocket closed. Code: ${code}, Reason: ${reasonStr}. Reconnecting in ${RECONNECT_INTERVAL / 1000}s...`);
        if (aggTradePingIntervalId) { clearInterval(aggTradePingIntervalId); aggTradePingIntervalId = null; }
        aggTradeWsClient = null;
        setTimeout(connectToAggTradeStream, RECONNECT_INTERVAL);
    });
}

function connectToBookTickerStream() {
    if (bookTickerWsClient && (bookTickerWsClient.readyState === WebSocket.OPEN || bookTickerWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    console.log(`[Listener] Connecting to Binance BookTicker: ${binanceBookTickerStreamUrl}`);
    bookTickerWsClient = new WebSocket(binanceBookTickerStreamUrl);

    bookTickerWsClient.on('open', function open() {
        console.log('[Listener] Connected to Binance bookTicker stream.');
        previous_b = 0; previous_B = 0; previous_a = 0; previous_A = 0;
        current_b = 0; current_B = 0; current_a = 0; current_A = 0;
        bbo_history_deque.length = 0; 

        if (bookTickerPingIntervalId) clearInterval(bookTickerPingIntervalId);
        bookTickerPingIntervalId = setInterval(() => {
            if (bookTickerWsClient && bookTickerWsClient.readyState === WebSocket.OPEN) {
                bookTickerWsClient.ping(() => {});
            }
        }, BINANCE_PING_INTERVAL_MS);
    });

    bookTickerWsClient.on('message', function incoming(data) {
        const messageString = data.toString();
        const bboData = manualExtractBookTicker(messageString); 

        if (bboData) {
            previous_b = current_b; previous_B = current_B;
            previous_a = current_a; previous_A = current_A;

            current_b = parseFloat(bboData.b); current_B = parseFloat(bboData.B);
            current_a = parseFloat(bboData.a); current_A = parseFloat(bboData.A);

            if (isNaN(current_b) || isNaN(current_B) || isNaN(current_a) || isNaN(current_A)) {
                console.warn('[Listener] Invalid BBO data parsed:', bboData);
                current_b = previous_b; current_B = previous_B;
                current_a = previous_a; current_A = previous_A;
                return;
            }

            bbo_history_deque.push({ b: current_b, B: current_B, a: current_a, A: current_A, E: Date.now() });
            if (bbo_history_deque.length > F3_CHURN_WINDOW_UPDATES * 1.5) { // Keep a bit more than strictly needed for F3, then trim precisely
                bbo_history_deque.splice(0, bbo_history_deque.length - F3_CHURN_WINDOW_UPDATES);
            }
            
            const signalInfo = checkFormulas(); 

            if (signalInfo) {
                sendDataToInternalReceiver({
                    e: "trade", E: signalInfo.eventTime, 
                    p: signalInfo.price.toFixed(TICK_SIZE === 0.01 ? 2 : (TICK_SIZE === 0.1 ? 1 : 4)), 
                    s: signalInfo.signal, src: signalInfo.source
                });
            }
        } else {
             if (messageString && !messageString.includes('"ping"') && !messageString.includes('"pong"')) {
                 console.warn('[Listener] Failed to extract data from bookTicker or unexpected bookTicker message format. Snippet:', messageString.substring(0, 100));
             }
        }
    });

    bookTickerWsClient.on('pong', () => {});
    bookTickerWsClient.on('error', (err) => console.error('[Listener] Binance BookTicker WebSocket error:', err.message));
    bookTickerWsClient.on('close', (code, reason) => {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] Binance BookTicker WebSocket closed. Code: ${code}, Reason: ${reasonStr}. Reconnecting in ${RECONNECT_INTERVAL / 1000}s...`);
        if (bookTickerPingIntervalId) { clearInterval(bookTickerPingIntervalId); bookTickerPingIntervalId = null; }
        bookTickerWsClient = null;
        setTimeout(connectToBookTickerStream, RECONNECT_INTERVAL);
    });
}
