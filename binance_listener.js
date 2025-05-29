// binance_listener.js

const WebSocket = require('ws');

// --- Global Error Handlers ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`);
    console.error(err.stack || err);
    console.error(`[Listener] Exception origin: ${origin}`);
    // Give a brief moment for logs to flush, then exit.
    setTimeout(() => {
        if (internalWsClient && typeof internalWsClient.terminate === 'function') {
            try { internalWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (binanceWsClient && typeof binanceWsClient.terminate === 'function') {
            try { binanceWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (bybitWsClient && typeof bybitWsClient.terminate === 'function') {
            try { bybitWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (binancePingIntervalId) clearInterval(binancePingIntervalId);
        if (bybitPingIntervalId) clearInterval(bybitPingIntervalId);
        if (bybitMetricsSendTimer) clearTimeout(bybitMetricsSendTimer);
        process.exit(1);
    }, 1000).unref();
});

process.on('unhandledRejection', (reason, promise) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNHANDLED PROMISE REJECTION`);
    console.error('[Listener] Unhandled Rejection at:', promise);
    console.error('[Listener] Reason:', reason instanceof Error ? reason.stack : reason);
    setTimeout(() => {
        if (internalWsClient && typeof internalWsClient.terminate === 'function') {
            try { internalWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (binanceWsClient && typeof binanceWsClient.terminate === 'function') {
            try { binanceWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (bybitWsClient && typeof bybitWsClient.terminate === 'function') {
            try { bybitWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (binancePingIntervalId) clearInterval(binancePingIntervalId);
        if (bybitPingIntervalId) clearInterval(bybitPingIntervalId);
        if (bybitMetricsSendTimer) clearTimeout(bybitMetricsSendTimer);
        process.exit(1);
    }, 1000).unref();
});

// --- Configuration ---
const internalReceiverUrl = 'ws://localhost:8082';
const RECONNECT_INTERVAL_MS = 5000;

// --- Binance Configuration ---
const binanceStreamUrl = 'wss://stream.binance.com:9443/ws/btcusdt@aggTrade';
const BINANCE_PING_INTERVAL_MS = 3 * 60 * 1000;
const PRICE_CHANGE_THRESHOLD = 1.2; // For Binance aggTrade

// --- Bybit Configuration ---
const BYBIT_STREAM_URL = 'wss://stream.bybit.com/v5/public/spot'; // For SPOT market
// const BYBIT_STREAM_URL = 'wss://stream.bybit.com/v5/public/linear'; // For USDT Perpetual
const BYBIT_SYMBOL = process.env.BYBIT_SYMBOL || 'BTCUSDT';
const BYBIT_ORDERBOOK_TOPIC = `orderbook.50.${BYBIT_SYMBOL}`;
const BYBIT_PING_INTERVAL_MS = 18 * 1000;
const BYBIT_PRICE_CHANGE_THRESHOLD_USD = 0.5;
const BYBIT_METRIC_SEND_INTERVAL_MS = 250;


// --- State Variables ---
let internalWsClient = null;
let binanceWsClient = null;
let binancePingIntervalId = null;
let lastSentPrice = null;

let bybitWsClient = null;
let bybitPingIntervalId = null;
let localBids = new Map();
let localAsks = new Map();
let lastBybitUpdateId = null;
let bybitSnapshotReceived = false;
let lastSentBybitMicroPrice = null;
let bybitMetrics = null;
let bybitMetricsSendTimer = null;


// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) {
        // console.log('[Listener] Internal receiver connection attempt skipped: already open or connecting.');
        return;
    }
    console.log(`[Listener] PID: ${process.pid} --- Connecting to internal receiver: ${internalReceiverUrl}`);
    internalWsClient = new WebSocket(internalReceiverUrl);

    internalWsClient.on('open', () => {
        console.log(`[Listener] PID: ${process.pid} --- Connected to internal receiver.`);
        lastSentPrice = null;
        lastSentBybitMicroPrice = null;
    });

    internalWsClient.on('error', (err) => {
        console.error(`[Listener] PID: ${process.pid} --- Internal receiver WebSocket error: ${err.message}`);
    });

    internalWsClient.on('close', (code, reason) => {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] PID: ${process.pid} --- Internal receiver closed. Code: ${code}, Reason: ${reasonStr}. Reconnecting...`);
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
}

// --- Binance: Manual Data Extraction for aggTrade ---
function manualExtractMinimalData(messageString) {
    try {
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
        const eventTimeNum = parseInt(messageString.substring(currentIndex, valueEndIndex), 10);
        if (isNaN(eventTimeNum)) return null;

        const priceKey = '"p":"';
        let priceStartIndex = messageString.indexOf(priceKey, valueEndIndex);
        if (priceStartIndex === -1) return null;
        priceStartIndex += priceKey.length;
        let priceEndIndex = messageString.indexOf('"', priceStartIndex);
        if (priceEndIndex === -1) return null;
        const priceStr = messageString.substring(priceStartIndex, priceEndIndex);
        if (priceStr.length === 0 || isNaN(parseFloat(priceStr))) return null;

        return { e: "trade", E: eventTimeNum, p: priceStr, source: "binance_aggtrade" };
    } catch (error) {
        return null;
    }
}

// --- Binance Stream Connection ---
function connectToBinance() {
    if (binanceWsClient && (binanceWsClient.readyState === WebSocket.OPEN || binanceWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    // console.log(`[Listener] PID: ${process.pid} --- Connecting to Binance: ${binanceStreamUrl}`);
    binanceWsClient = new WebSocket(binanceStreamUrl);

    binanceWsClient.on('open', function open() {
        console.log(`[Listener] PID: ${process.pid} --- Connected to Binance stream.`);
        lastSentPrice = null;
        if (binancePingIntervalId) clearInterval(binancePingIntervalId);
        binancePingIntervalId = setInterval(() => {
            if (binanceWsClient && binanceWsClient.readyState === WebSocket.OPEN) {
                try { binanceWsClient.ping(() => {}); } catch (e) { /* console.error(`[Listener] Error sending ping to Binance: ${e.message}`); */ }
            }
        }, BINANCE_PING_INTERVAL_MS);
    });

    binanceWsClient.on('message', function incoming(data) {
        try {
            const messageString = data.toString();
            const minimalData = manualExtractMinimalData(messageString);

            if (minimalData) {
                const currentPrice = parseFloat(minimalData.p);
                if (isNaN(currentPrice)) {
                    console.warn(`[Listener] Invalid price in Binance aggTrade data: ${minimalData.p}`);
                    return;
                }
                if (lastSentPrice === null || Math.abs(currentPrice - lastSentPrice) >= PRICE_CHANGE_THRESHOLD) {
                    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                        try {
                            internalWsClient.send(JSON.stringify(minimalData));
                            lastSentPrice = currentPrice;
                        } catch (sendError) {
                            console.error(`[Listener] Error sending Binance data to internal receiver: ${sendError.message}`);
                        }
                    }
                }
            }
        } catch (e) {
            console.error(`[Listener] CRITICAL ERROR in Binance message handler: ${e.message}`, e.stack);
        }
    });

    binanceWsClient.on('error', function error(err) {
        console.error(`[Listener] PID: ${process.pid} --- Binance WebSocket error: ${err.message}`);
    });

    binanceWsClient.on('close', function close(code, reason) {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] PID: ${process.pid} --- Binance WebSocket closed. Code: ${code}, Reason: ${reasonStr}. Reconnecting...`);
        if (binancePingIntervalId) { clearInterval(binancePingIntervalId); binancePingIntervalId = null; }
        binanceWsClient = null;
        setTimeout(connectToBinance, RECONNECT_INTERVAL_MS);
    });
}

// --- Bybit Helper Functions for Interpretation ---
function getImbalanceInterpretation(value, type = "Pressure") {
    const strength = Math.abs(value);
    let level = "";
    if (strength > 0.4) level = "Very Strong ";
    else if (strength > 0.15) level = "Strong ";
    else if (strength > 0.05) level = "Moderate ";
    else return `Neutral ${type}`;
    if (value > 0) return `${level}Buy ${type}`;
    return `${level}Sell ${type}`;
}

function getMicroPriceInterpretation(micro_price, bestBidPrice, bestAskPrice) {
    if (bestBidPrice >= bestAskPrice || bestAskPrice <= 0 || bestBidPrice <= 0) return "N/A (Invalid BBO)";
    const midPoint = (bestBidPrice + bestAskPrice) / 2;
    const spread = bestAskPrice - bestBidPrice;
    if (spread === 0) return "Neutral (Zero Spread)";
    const diff_from_mid = micro_price - midPoint;
    const moderate_lean_threshold = 0.10 * (spread / 2);
    const strong_lean_threshold = 0.30 * (spread / 2);
    if (diff_from_mid > strong_lean_threshold) return "Strong Bullish Lean (Near Ask)";
    if (diff_from_mid > moderate_lean_threshold) return "Moderate Bullish Lean";
    if (diff_from_mid < -strong_lean_threshold) return "Strong Bearish Lean (Near Bid)";
    if (diff_from_mid < -moderate_lean_threshold) return "Moderate Bearish Lean";
    return "Neutral Lean (Near Midpoint)";
}

function getSpreadInterpretation(spread, bestAskPrice) {
    if (bestAskPrice <= 0) return "N/A";
    const spread_percentage = (spread / bestAskPrice) * 100;
    if (spread_percentage < 0.005) return `Very Tight (${spread.toFixed(2)})`;
    if (spread_percentage < 0.01) return `Tight (${spread.toFixed(2)})`;
    if (spread_percentage < 0.02) return `Moderate (${spread.toFixed(2)})`;
    return `Wide (${spread.toFixed(2)})`;
}

function getQuantityInterpretation(quantity, symbol, context = "", bestAskPrice = 0) {
    const asset = symbol.replace(/USDT$/, '');
    let size = "";
    if (symbol.includes("BTC")) { // BTC-centric thresholds
        if (quantity > 2) size = "Very Large";
        else if (quantity > 0.5) size = "Large";
        else if (quantity > 0.1) size = "Moderate";
        else size = "Small";
    } else { // Generic fallback (less accurate, needs per-symbol tuning)
        if (bestAskPrice > 0) { // Requires price context for non-BTC generic
             if (quantity * bestAskPrice > 100000) size = "Large ($)"; // > $100k
             else if (quantity * bestAskPrice > 10000) size = "Moderate ($)"; // > $10k
             else size = "Small ($)";
        } else {
            size = "Unknown Size"; // Cannot determine without price for non-BTC
        }
    }
    return `${size} Qty ${context} (${quantity.toFixed(3)} ${asset})`;
}


// --- Bybit Stream Connection & Logic ---
function connectToBybit() {
    if (bybitWsClient && (bybitWsClient.readyState === WebSocket.OPEN || bybitWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    // console.log(`[Listener] PID: ${process.pid} --- Connecting to Bybit: ${BYBIT_STREAM_URL} for ${BYBIT_ORDERBOOK_TOPIC}`);
    bybitWsClient = new WebSocket(BYBIT_STREAM_URL);
    bybitSnapshotReceived = false;

    bybitWsClient.on('open', () => {
        console.log(`[Listener] PID: ${process.pid} --- Connected to Bybit stream.`);
        const subscriptionMessage = { op: "subscribe", args: [BYBIT_ORDERBOOK_TOPIC], req_id: `sub-${Date.now()}` };
        try {
            bybitWsClient.send(JSON.stringify(subscriptionMessage));
        } catch (e) {
             console.error(`[Listener] Error sending Bybit subscription: ${e.message}`);
        }
        if (bybitPingIntervalId) clearInterval(bybitPingIntervalId);
        bybitPingIntervalId = setInterval(() => {
            if (bybitWsClient && bybitWsClient.readyState === WebSocket.OPEN) {
                try { bybitWsClient.send(JSON.stringify({ op: "ping", req_id: `ping-${Date.now()}` })); } catch (e) { /* console.error(`[Listener] Error sending ping to Bybit: ${e.message}`); */ }
            }
        }, BYBIT_PING_INTERVAL_MS);
        localBids.clear(); localAsks.clear(); lastBybitUpdateId = null; lastSentBybitMicroPrice = null; bybitMetrics = null;
    });

    bybitWsClient.on('message', (data) => { handleBybitMessage(data.toString()); });

    bybitWsClient.on('error', (err) => {
        console.error(`[Listener] PID: ${process.pid} --- Bybit WebSocket error: ${err.message}`);
    });

    bybitWsClient.on('close', (code, reason) => {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] PID: ${process.pid} --- Bybit WebSocket closed. Code: ${code}, Reason: ${reasonStr}. Reconnecting...`);
        if (bybitPingIntervalId) { clearInterval(bybitPingIntervalId); bybitPingIntervalId = null; }
        bybitWsClient = null; bybitSnapshotReceived = false; localBids.clear(); localAsks.clear();
        lastBybitUpdateId = null; lastSentBybitMicroPrice = null; bybitMetrics = null;
        if (bybitMetricsSendTimer) { clearTimeout(bybitMetricsSendTimer); bybitMetricsSendTimer = null; }
        setTimeout(connectToBybit, RECONNECT_INTERVAL_MS);
    });
}

function handleBybitMessage(jsonDataString) {
    try {
        const msg = JSON.parse(jsonDataString);

        if (msg.op === "subscribe") {
            if (msg.success) {
                console.log(`[Listener] Successfully subscribed to Bybit topic: ${msg.args ? msg.args.join(',') : (msg.req_id || 'N/A')}`);
            } else {
                console.error(`[Listener] Failed to subscribe to Bybit topic: ${msg.ret_msg}. Req ID: ${msg.req_id}`);
                if (bybitWsClient) bybitWsClient.terminate();
            }
            return;
        }
        if (msg.op === "pong") return;

        if (msg.topic && msg.topic === BYBIT_ORDERBOOK_TOPIC && msg.data) {
            const data = msg.data;
            let bookUpdated = false;

            if (msg.type === "snapshot") {
                applyBybitSnapshot(data);
                lastBybitUpdateId = data.u;
                bybitSnapshotReceived = true;
                bookUpdated = true;
                console.log(`[Listener] Bybit snapshot processed. Update ID: ${data.u}, Symbol: ${data.s}`);
            } else if (msg.type === "delta") {
                if (!bybitSnapshotReceived) return; // Wait for snapshot
                if (lastBybitUpdateId !== null && data.u !== (lastBybitUpdateId + 1)) {
                    console.error(`[Listener] Bybit delta sequence mismatch! Last U: ${lastBybitUpdateId}, current U: ${data.u}. Terminating for fresh snapshot.`);
                    if (bybitWsClient) bybitWsClient.terminate();
                    bybitSnapshotReceived = false; localBids.clear(); localAsks.clear(); lastBybitUpdateId = null;
                    bybitMetrics = null; if (bybitMetricsSendTimer) { clearTimeout(bybitMetricsSendTimer); bybitMetricsSendTimer = null; }
                    lastSentBybitMicroPrice = null;
                    return;
                }
                applyBybitDelta(data);
                lastBybitUpdateId = data.u;
                bookUpdated = true;
            }

            if (bookUpdated && localBids.size > 0 && localAsks.size > 0) {
                bybitMetrics = calculateAndFormatBybitMetrics();
                if (!bybitMetricsSendTimer) {
                    bybitMetricsSendTimer = setTimeout(() => {
                        if (bybitMetrics && internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                            const currentMicroPrice = bybitMetrics.micro_price;
                            if (lastSentBybitMicroPrice === null || Math.abs(currentMicroPrice - lastSentBybitMicroPrice) >= BYBIT_PRICE_CHANGE_THRESHOLD_USD) {
                                try {
                                    internalWsClient.send(JSON.stringify(bybitMetrics));
                                    lastSentBybitMicroPrice = currentMicroPrice;
                                } catch (sendError) { console.error(`[Listener] Error sending Bybit metrics: ${sendError.message}`); }
                            }
                        }
                        bybitMetricsSendTimer = null;
                    }, BYBIT_METRIC_SEND_INTERVAL_MS);
                }
            }
        }
    } catch (error) {
        console.error(`[Listener] Error processing Bybit message: ${error.message}. Data: ${jsonDataString.substring(0,100)}`);
    }
}

function applyBybitSnapshot(snapshotData) {
    localBids.clear(); localAsks.clear();
    snapshotData.b.forEach(([p, q]) => { if (parseFloat(q) > 0) localBids.set(p, q); });
    snapshotData.a.forEach(([p, q]) => { if (parseFloat(q) > 0) localAsks.set(p, q); });
}

function applyBybitDelta(deltaData) {
    deltaData.b.forEach(([p, q]) => { parseFloat(q) === 0 ? localBids.delete(p) : localBids.set(p, q); });
    deltaData.a.forEach(([p, q]) => { parseFloat(q) === 0 ? localAsks.delete(p) : localAsks.set(p, q); });
}

function calculateAndFormatBybitMetrics() {
    if (localBids.size === 0 || localAsks.size === 0) return null;

    const bidsArray = Array.from(localBids.entries()).map(([p, q]) => ({ price: parseFloat(p), qty: parseFloat(q) })).sort((a, b) => b.price - a.price);
    const asksArray = Array.from(localAsks.entries()).map(([p, q]) => ({ price: parseFloat(p), qty: parseFloat(q) })).sort((a, b) => a.price - b.price);

    if (bidsArray.length === 0 || asksArray.length === 0) return null;
    
    const bestBid = bidsArray[0]; const bestAsk = asksArray[0];
    const bestBidPrice = bestBid.price; const bestBidQty = bestBid.qty;
    const bestAskPrice = bestAsk.price; const bestAskQty = bestAsk.qty;
    
    const pricePrecision = (bestAskPrice.toString().split('.')[1] || '').length;
    const spread_val = parseFloat((bestAskPrice - bestBidPrice).toFixed(Math.max(2, pricePrecision)));

    const obi_tob_val = (bestBidQty + bestAskQty === 0) ? 0 : (bestBidQty - bestAskQty) / (bestBidQty + bestAskQty);
    const micro_price_val = (bestBidQty + bestAskQty === 0) ? (bestBidPrice + bestAskPrice) / 2 : (bestBidPrice * bestAskQty + bestAskPrice * bestBidQty) / (bestBidQty + bestAskQty);

    const sumQtyN = (N, arr) => arr.slice(0, N).reduce((s, l) => s + l.qty, 0);
    const sumBidQty_5 = sumQtyN(5, bidsArray); const sumAskQty_5 = sumQtyN(5, asksArray);
    const cobi_5_val = (sumBidQty_5 + sumAskQty_5 === 0) ? 0 : (sumBidQty_5 - sumAskQty_5) / (sumBidQty_5 + sumAskQty_5);

    return {
        type: "bybit_metrics_interpreted", source: "bybit_orderbook", symbol: BYBIT_SYMBOL,
        timestamp_processing: Date.now(), timestamp_event: lastBybitUpdateId,
        best_bid_price: bestBidPrice, best_bid_qty: bestBidQty,
        best_bid_qty_interpretation: getQuantityInterpretation(bestBidQty, BYBIT_SYMBOL, "at Best Bid", bestAskPrice),
        best_ask_price: bestAskPrice, best_ask_qty: bestAskQty,
        best_ask_qty_interpretation: getQuantityInterpretation(bestAskQty, BYBIT_SYMBOL, "at Best Ask", bestAskPrice),
        spread: spread_val, spread_interpretation: getSpreadInterpretation(spread_val, bestAskPrice),
        obi_top_of_book: parseFloat(obi_tob_val.toFixed(4)),
        obi_top_of_book_interpretation: getImbalanceInterpretation(obi_tob_val, "Pressure"),
        micro_price: parseFloat(micro_price_val.toFixed(Math.max(2, pricePrecision))),
        micro_price_interpretation: getMicroPriceInterpretation(micro_price_val, bestBidPrice, bestAskPrice),
        cobi_5_levels: parseFloat(cobi_5_val.toFixed(4)),
        cobi_5_levels_interpretation: getImbalanceInterpretation(cobi_5_val, "Support/Resistance in 5 Levels"),
        depth_qty_top5_bids: parseFloat(sumBidQty_5.toFixed(3)),
        depth_qty_top5_asks: parseFloat(sumAskQty_5.toFixed(3)),
        num_bid_levels: bidsArray.length, num_ask_levels: asksArray.length
    };
}

// --- Start the connections ---
console.log(`[Listener] PID: ${process.pid} --- Binance Listener starting for ${binanceStreamUrl.split('/').pop()}`);
console.log(`[Listener] PID: ${process.pid} --- Bybit Listener starting for ${BYBIT_ORDERBOOK_TOPIC}`);
console.log(`[Listener] Internal Receiver URL: ${internalReceiverUrl}`);

connectToInternalReceiver();
connectToBinance();
connectToBybit();

console.log(`[Listener] PID: ${process.pid} --- Initial connection attempts initiated.`);
