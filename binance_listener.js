

// binance_listener.js

const WebSocket = require('ws');

// --- Global Error Handlers ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[App] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`);
    console.error(err.stack || err);
    console.error(`[App] Exception origin: ${origin}`);
    console.error(`[App] PID: ${process.pid} --- Exiting due to uncaught exception...`);
    setTimeout(() => {
        if (pushIntervalId) { try { clearInterval(pushIntervalId); } catch(e) { /* ignore */ } }
        if (internalWsClient && typeof internalWsClient.terminate === 'function') {
            try { internalWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (binanceWsClient && typeof binanceWsClient.terminate === 'function') {
            try { binanceWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (spotWsClient && typeof spotWsClient.terminate === 'function') {
            try { spotWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (futuresWsClient && typeof futuresWsClient.terminate === 'function') {
            try { futuresWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (binancePingIntervalId) { try { clearInterval(binancePingIntervalId); } catch(e) { /* ignore */ } }
        if (spotPingIntervalId) { try { clearInterval(spotPingIntervalId); } catch(e) { /* ignore */ } }
        if (futuresPingIntervalId) { try { clearInterval(futuresPingIntervalId); } catch(e) { /* ignore */ } }
        if (arbitrageCheckIntervalId) { try { clearInterval(arbitrageCheckIntervalId); } catch(e) { /* ignore */ } }
        process.exit(1);
    }, 1000).unref();
});

process.on('unhandledRejection', (reason, promise) => {
    console.error(`[App] PID: ${process.pid} --- FATAL: UNHANDLED PROMISE REJECTION`);
    console.error('[App] Unhandled Rejection at:', promise);
    console.error('[App] Reason:', reason instanceof Error ? reason.stack : reason);
    console.error(`[App] PID: ${process.pid} --- Exiting due to unhandled promise rejection...`);
    setTimeout(() => {
        if (pushIntervalId) { try { clearInterval(pushIntervalId); } catch(e) { /* ignore */ } }
        if (internalWsClient && typeof internalWsClient.terminate === 'function') {
            try { internalWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (binanceWsClient && typeof binanceWsClient.terminate === 'function') {
            try { binanceWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (spotWsClient && typeof spotWsClient.terminate === 'function') {
            try { spotWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (futuresWsClient && typeof futuresWsClient.terminate === 'function') {
            try { futuresWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (binancePingIntervalId) { try { clearInterval(binancePingIntervalId); } catch(e) { /* ignore */ } }
        if (spotPingIntervalId) { try { clearInterval(spotPingIntervalId); } catch(e) { /* ignore */ } }
        if (futuresPingIntervalId) { try { clearInterval(futuresPingIntervalId); } catch(e) { /* ignore */ } }
        if (arbitrageCheckIntervalId) { try { clearInterval(arbitrageCheckIntervalId); } catch(e) { /* ignore */ } }
        process.exit(1);
    }, 1000).unref();
});

// --- AggTrade Listener Configuration ---
const binanceStreamUrl = 'wss://stream.binance.com:9443/ws/btcusdt@aggTrade';
const internalReceiverUrl = 'ws://localhost:8082'; // Your internal client address
const RECONNECT_INTERVAL_MS = 5000;
const BINANCE_PING_INTERVAL_MS = 3 * 60 * 1000;
const PRICE_CHANGE_THRESHOLD = 1.2; // For aggTrade listener push to internal client
const PUSH_INTERVAL_MS = 25.0; // Fixed push interval for aggTrade data

// --- Arbitrage Configuration ---
const SPOT_BOOKTICKER_URL = 'wss://stream.binance.com:9443/ws/btcusdt@bookTicker';
const FUTURES_BOOKTICKER_URL = 'wss://fstream.binance.com/ws/btcusdt@bookTicker'; // For USDT-M Perpetual Futures
const ARB_RECONNECT_INTERVAL_MS = 5000;
const ARB_PING_INTERVAL_MS = 3 * 60 * 1000;
const DESIRED_PROFIT_THRESHOLD_USD = 10.0; // Desired profit *from the deviation*
// IMPORTANT: Review TOTAL_FEES_PER_UNIT_USD. This fixed USD value per BTC is likely inaccurate.
// Real fees are percentage-based and should be calculated dynamically using current prices.
// Example: (spot_price * spot_fee_rate) + (futures_price * futures_fee_rate)
const TOTAL_FEES_PER_UNIT_USD = 0.2;
const ARBITRAGE_CHECK_INTERVAL_MS = 10; // How often to check for arbitrage opportunities

// NEW: Natural basis derived from Premium Index (example value)
// This value represents how much higher the Spot price typically is over the Futures price.
// If Premium Index is -0.06% and Spot is $70,000, then 0.0006 * 70000 = $42.
// A negative Premium Index (on Binance for BTCUSDT perp) usually means Spot > Futures, so NATURAL_BASIS_SPOT_OVER_FUTURES is positive.
// Example: Spot is typically $42.40 higher than Futures.
const NATURAL_BASIS_SPOT_OVER_FUTURES = 35.0;

// --- AggTrade Listener State Variables ---
let binanceWsClient = null;
let internalWsClient = null;
let binancePingIntervalId = null;
let pushIntervalId = null;
let lastPriceMeetingThreshold = null;
let latestSignificantDataToPush = null;
let lastPushedDataByInterval = null;

// --- Arbitrage State Variables ---
let spotWsClient = null;
let futuresWsClient = null;
let spotPingIntervalId = null;
let futuresPingIntervalId = null;
let arbitrageCheckIntervalId = null;

let latestSpotData = { bestBid: null, bestAsk: null, timestamp: null };
let latestFuturesData = { bestBid: null, bestAsk: null, timestamp: null };
let lastSentArbitrageType = null; // Tracks the type of last sent signal


// --- Internal Receiver Connection (for aggTrade AND Arbitrage Signals) ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    console.log(`[Listener] PID: ${process.pid} --- Connecting to internal receiver at ${internalReceiverUrl}...`);
    internalWsClient = new WebSocket(internalReceiverUrl);

    internalWsClient.on('open', () => {
        console.log(`[Listener] PID: ${process.pid} --- Connected to internal receiver.`);
        lastPushedDataByInterval = null;
        latestSignificantDataToPush = null;
        startPushInterval();
    });

    internalWsClient.on('error', (err) => {
        console.error(`[Listener] PID: ${process.pid} --- Internal receiver WebSocket error:`, err.message);
    });

    internalWsClient.on('close', (code, reason) => {
        console.log(`[Listener] PID: ${process.pid} --- Disconnected from internal receiver. Code: ${code}, Reason: ${reason ? reason.toString() : 'N/A'}`);
        if (pushIntervalId) {
            clearInterval(pushIntervalId);
            pushIntervalId = null;
        }
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
}

/**
 * Manually extracts minimal data from an aggTrade message string.
 */
function manualExtractMinimalData(messageString) {
    try {
        const outputEventTypeStr = "trade";
        let eventTimeNum = null;
        let priceStr = null;

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

        const priceKey = '"p":"';
        let priceStartIndex = messageString.indexOf(priceKey, valueEndIndex);
        if (priceStartIndex === -1) return null;
        priceStartIndex += priceKey.length;
        let priceEndIndex = messageString.indexOf('"', priceStartIndex);
        if (priceEndIndex === -1) return null;
        priceStr = messageString.substring(priceStartIndex, priceEndIndex);
        if (priceStr.length === 0 || isNaN(parseFloat(priceStr))) return null;

        return { e: outputEventTypeStr, E: eventTimeNum, p: priceStr };
    } catch (error) {
        return null;
    }
}

// --- AggTrade Push Interval Logic ---
function startPushInterval() {
    if (pushIntervalId) {
        clearInterval(pushIntervalId);
        pushIntervalId = null;
    }

    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        pushIntervalId = setInterval(() => {
            if (!(internalWsClient && internalWsClient.readyState === WebSocket.OPEN)) {
                if (pushIntervalId) {
                    clearInterval(pushIntervalId);
                    pushIntervalId = null;
                }
                return;
            }
            if (!latestSignificantDataToPush) return;
            if (latestSignificantDataToPush === lastPushedDataByInterval) return;

            const dataToPush = latestSignificantDataToPush;
            let minimalJsonString;
            try {
                minimalJsonString = JSON.stringify(dataToPush);
            } catch (stringifyError) {
                console.error(`[Listener] PID: ${process.pid} --- CRITICAL: Error stringifying aggTrade data for interval push:`, stringifyError.message);
                return;
            }
            try {
                internalWsClient.send(minimalJsonString);
                lastPushedDataByInterval = dataToPush;
            } catch (sendError) {
                console.error(`[Listener] PID: ${process.pid} --- Error sending aggTrade data to internal receiver via interval:`, sendError.message);
            }
        }, PUSH_INTERVAL_MS);
    }
}

// --- Binance AggTrade Stream Connection ---
function connectToBinanceAggTrade() {
    if (binanceWsClient && (binanceWsClient.readyState === WebSocket.OPEN || binanceWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    console.log(`[Listener] PID: ${process.pid} --- Connecting to Binance AggTrade stream...`);
    binanceWsClient = new WebSocket(binanceStreamUrl);

    binanceWsClient.on('open', function open() {
        console.log(`[Listener] PID: ${process.pid} --- Connected to Binance AggTrade stream.`);
        lastPriceMeetingThreshold = null;
        latestSignificantDataToPush = null;

        if (binancePingIntervalId) clearInterval(binancePingIntervalId);
        binancePingIntervalId = setInterval(() => {
            if (binanceWsClient && binanceWsClient.readyState === WebSocket.OPEN) {
                try {
                    binanceWsClient.ping(() => {});
                } catch (pingError) {
                    console.error(`[Listener] PID: ${process.pid} --- Error sending ping to Binance AggTrade:`, pingError.message);
                }
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
                    return;
                }
                let isSignificantChange = false;
                if (lastPriceMeetingThreshold === null) {
                    isSignificantChange = true;
                } else {
                    const priceDifference = Math.abs(currentPrice - lastPriceMeetingThreshold);
                    if (priceDifference >= PRICE_CHANGE_THRESHOLD) {
                        isSignificantChange = true;
                    }
                }
                if (isSignificantChange) {
                    latestSignificantDataToPush = minimalData;
                    lastPriceMeetingThreshold = currentPrice;
                }
            }
        } catch (e) {
            console.error(`[Listener] PID: ${process.pid} --- CRITICAL ERROR in Binance AggTrade message handler:`, e.message, e.stack);
        }
    });

    binanceWsClient.on('pong', () => { });

    binanceWsClient.on('error', function error(err) {
        console.error(`[Listener] PID: ${process.pid} --- Binance AggTrade WebSocket error:`, err.message);
    });

    binanceWsClient.on('close', function close(code, reason) {
        console.log(`[Listener] PID: ${process.pid} --- Binance AggTrade WebSocket closed. Code: ${code}, Reason: ${reason ? reason.toString() : 'N/A'}`);
        if (binancePingIntervalId) { clearInterval(binancePingIntervalId); binancePingIntervalId = null; }
        binanceWsClient = null;
        setTimeout(connectToBinanceAggTrade, RECONNECT_INTERVAL_MS);
    });
}

// --- Arbitrage Logic Core (Called by Interval) --- // *** UPDATED with Natural Basis Logic ***
function performArbitrageCheckAndSignal() {
    if (!latestSpotData.bestBid || !latestSpotData.bestAsk || !latestSpotData.timestamp ||
        !latestFuturesData.bestBid || !latestFuturesData.bestAsk || !latestFuturesData.timestamp) {
        if (lastSentArbitrageType !== null) {
            console.log(`[Arbitrage] PID: ${process.pid} --- Opportunity (${lastSentArbitrageType}) cleared due to incomplete market data.`);
            lastSentArbitrageType = null;
        }
        return;
    }

    let currentOpportunityType = null;
    let arbSignalPayload = null;
    let identifiedNetProfit = 0; // This will be the profit *beyond* the natural basis
    let spotTradePrice = null;
    let futuresTradePrice = null;

    // --- Calculate potential profits based on DEVIATIONS from the NATURAL_BASIS_SPOT_OVER_FUTURES ---

    // Scenario 1: Sell Spot, Buy Futures (Spot is "too high" even for its natural premium)
    // We sell spot at Spot Best Bid, Buy futures at Futures Best Ask.
    // The current observed basis is (Spot Bid - Futures Ask).
    // We are interested in how much this current basis EXCEEDS the natural basis.
    const currentBasisSpotOverFutures = latestSpotData.bestBid - latestFuturesData.bestAsk;
    const deviationProfitSpotSellFuturesBuy = (currentBasisSpotOverFutures - NATURAL_BASIS_SPOT_OVER_FUTURES) - TOTAL_FEES_PER_UNIT_USD;

    // Scenario 2: Sell Futures, Buy Spot (Futures are "too high" relative to Spot, or Spot's premium has shrunk/reversed)
    // We sell futures at Futures Best Bid, Buy spot at Spot Best Ask.
    // The current observed basis (from futures' perspective) is (Futures Bid - Spot Ask).
    // We are interested in how much this current basis (which is usually negative if spot is naturally higher)
    // has moved *positively* relative to the natural state.
    // If NATURAL_BASIS_SPOT_OVER_FUTURES = $42 (Spot is $42 > Futures),
    // then the natural (Futures Bid - Spot Ask) is approx -$42.
    // If current (Futures Bid - Spot Ask) becomes -$10, the deviation is (-$10) - (-$42) = $32.
    // This is equivalent to: (Futures Bid - Spot Ask) + NATURAL_BASIS_SPOT_OVER_FUTURES
    const currentBasisFuturesOverSpot = latestFuturesData.bestBid - latestSpotData.bestAsk;
    const deviationProfitFuturesSellSpotBuy = (currentBasisFuturesOverSpot + NATURAL_BASIS_SPOT_OVER_FUTURES) - TOTAL_FEES_PER_UNIT_USD;


    // --- Determine if an arbitrage opportunity exists based on deviation profits ---

    // Check Scenario 1 (Sell Spot, Buy Futures)
    if (deviationProfitSpotSellFuturesBuy > DESIRED_PROFIT_THRESHOLD_USD) {
        currentOpportunityType = "scenario1_SellSpotBuyFutures_Deviation";
        identifiedNetProfit = deviationProfitSpotSellFuturesBuy;
        spotTradePrice = latestSpotData.bestBid;       // Selling spot
        futuresTradePrice = latestFuturesData.bestAsk; // Buying futures
        arbSignalPayload = {
            arb_signal: {
                sell_market: "spot",
                buy_market: "futures",
                net_profit_usd: parseFloat(identifiedNetProfit.toFixed(4)), // Profit from deviation
                spot_price_trade: spotTradePrice,
                futures_price_trade: futuresTradePrice
            }
        };
    }
    // Else, check Scenario 2 (Sell Futures, Buy Spot)
    else if (deviationProfitFuturesSellSpotBuy > DESIRED_PROFIT_THRESHOLD_USD) {
        currentOpportunityType = "scenario2_SellFuturesBuySpot_Deviation";
        identifiedNetProfit = deviationProfitFuturesSellSpotBuy;
        spotTradePrice = latestSpotData.bestAsk;        // Buying spot
        futuresTradePrice = latestFuturesData.bestBid;  // Selling futures
        arbSignalPayload = {
            arb_signal: {
                sell_market: "futures",
                buy_market: "spot",
                net_profit_usd: parseFloat(identifiedNetProfit.toFixed(4)), // Profit from deviation
                spot_price_trade: spotTradePrice,
                futures_price_trade: futuresTradePrice
            }
        };
    }

    // --- Signal handling (remains the same logic, but uses identifiedNetProfit) ---
    if (currentOpportunityType) {
        if (currentOpportunityType !== lastSentArbitrageType) {
            if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                try {
                    const messageString = JSON.stringify(arbSignalPayload);
                    internalWsClient.send(messageString);
                    // Log the identifiedNetProfit (the deviation profit)
                    console.log(`[Arbitrage] PID: ${process.pid} --- SENT Signal. Type: ${currentOpportunityType}, Sell: ${arbSignalPayload.arb_signal.sell_market} (SpotP: ${arbSignalPayload.arb_signal.spot_price_trade}, FuturesP: ${arbSignalPayload.arb_signal.futures_price_trade}), Buy: ${arbSignalPayload.arb_signal.buy_market}, Deviation Profit: $${identifiedNetProfit.toFixed(4)}`);
                    lastSentArbitrageType = currentOpportunityType;
                } catch (error) {
                    console.error(`[Arbitrage] PID: ${process.pid} --- Error stringifying or sending arbitrage opportunity:`, error.message, error.stack);
                }
            } else {
                console.warn(`[Arbitrage] PID: ${process.pid} --- Detected ${currentOpportunityType} opportunity but internal client NOT OPEN. Deviation Profit: $${identifiedNetProfit.toFixed(4)}. Signal not sent.`);
            }
        } else {
            // console.debug(`[Arbitrage] PID: ${process.pid} --- ${currentOpportunityType} opportunity persists. Deviation Profit $${identifiedNetProfit.toFixed(4)}. Signal already sent.`);
        }
    } else {
        if (lastSentArbitrageType !== null) {
            console.log(`[Arbitrage] PID: ${process.pid} --- Previously signaled arbitrage opportunity (${lastSentArbitrageType}) has now disappeared or fallen below threshold.`);
            lastSentArbitrageType = null;
        }
    }
}


// --- Spot BookTicker Connection (for Arbitrage) ---
function connectToSpotBookTicker() {
    if (spotWsClient && (spotWsClient.readyState === WebSocket.OPEN || spotWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    console.log(`[Arbitrage] PID: ${process.pid} --- Connecting to Spot BookTicker...`);
    spotWsClient = new WebSocket(SPOT_BOOKTICKER_URL);

    spotWsClient.on('open', function open() {
        console.log(`[Arbitrage] PID: ${process.pid} --- Connected to Spot BookTicker.`);
        if (spotPingIntervalId) clearInterval(spotPingIntervalId);
        spotPingIntervalId = setInterval(() => {
            if (spotWsClient && spotWsClient.readyState === WebSocket.OPEN) {
                try {
                    spotWsClient.ping(() => {});
                } catch (pingError) {
                    console.error(`[Arbitrage] PID: ${process.pid} --- Error sending ping to Spot BookTicker:`, pingError.message);
                }
            }
        }, ARB_PING_INTERVAL_MS);
    });

    spotWsClient.on('message', function incoming(data) {
        try {
            const messageString = data.toString();
            const tickerData = JSON.parse(messageString);

            if (tickerData && typeof tickerData.b !== 'undefined' && typeof tickerData.a !== 'undefined') {
                const newSpotBestBid = parseFloat(tickerData.b);
                const newSpotBestAsk = parseFloat(tickerData.a);

                if (isNaN(newSpotBestBid) || isNaN(newSpotBestAsk)) {
                    console.warn(`[Arbitrage] PID: ${process.pid} --- Invalid price in Spot BookTicker data: Bid=${tickerData.b}, Ask=${tickerData.a}`);
                    return;
                }
                latestSpotData.bestBid = newSpotBestBid;
                latestSpotData.bestAsk = newSpotBestAsk;
                latestSpotData.timestamp = tickerData.E || Date.now();
            }
        } catch (e) {
            console.error(`[Arbitrage] PID: ${process.pid} --- CRITICAL ERROR in Spot BookTicker message handler:`, e.message, e.stack);
        }
    });

    spotWsClient.on('pong', () => { });

    spotWsClient.on('error', function error(err) {
        console.error(`[Arbitrage] PID: ${process.pid} --- Spot BookTicker WebSocket error:`, err.message);
    });

    spotWsClient.on('close', function close(code, reason) {
        console.log(`[Arbitrage] PID: ${process.pid} --- Spot BookTicker WebSocket closed. Code: ${code}, Reason: ${reason ? reason.toString() : 'N/A'}`);
        if (spotPingIntervalId) { clearInterval(spotPingIntervalId); spotPingIntervalId = null; }
        spotWsClient = null;
        latestSpotData = { bestBid: null, bestAsk: null, timestamp: null };
        setTimeout(connectToSpotBookTicker, ARB_RECONNECT_INTERVAL_MS);
    });
}

// --- Futures BookTicker Connection (for Arbitrage) ---
function connectToFuturesBookTicker() {
    if (futuresWsClient && (futuresWsClient.readyState === WebSocket.OPEN || futuresWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    console.log(`[Arbitrage] PID: ${process.pid} --- Connecting to Futures BookTicker...`);
    futuresWsClient = new WebSocket(FUTURES_BOOKTICKER_URL);

    futuresWsClient.on('open', function open() {
        console.log(`[Arbitrage] PID: ${process.pid} --- Connected to Futures BookTicker.`);
        if (futuresPingIntervalId) clearInterval(futuresPingIntervalId);
        futuresPingIntervalId = setInterval(() => {
            if (futuresWsClient && futuresWsClient.readyState === WebSocket.OPEN) {
                try {
                    futuresWsClient.ping(() => {});
                } catch (pingError) {
                    console.error(`[Arbitrage] PID: ${process.pid} --- Error sending ping to Futures BookTicker:`, pingError.message);
                }
            }
        }, ARB_PING_INTERVAL_MS);
    });

    futuresWsClient.on('message', function incoming(data) {
        try {
            const messageString = data.toString();
            if (messageString.includes('"e":"pong"')) { // Futures stream sends pong as a message
                return;
            }
            const tickerData = JSON.parse(messageString);

            if (tickerData && typeof tickerData.b !== 'undefined' && typeof tickerData.a !== 'undefined') {
                const newFuturesBestBid = parseFloat(tickerData.b);
                const newFuturesBestAsk = parseFloat(tickerData.a);

                if (isNaN(newFuturesBestBid) || isNaN(newFuturesBestAsk)) {
                    console.warn(`[Arbitrage] PID: ${process.pid} --- Invalid price in Futures BookTicker data: Bid=${tickerData.b}, Ask=${tickerData.a}`);
                    return;
                }
                latestFuturesData.bestBid = newFuturesBestBid;
                latestFuturesData.bestAsk = newFuturesBestAsk;
                latestFuturesData.timestamp = tickerData.E || Date.now();
            }
        } catch (e) {
            console.error(`[Arbitrage] PID: ${process.pid} --- CRITICAL ERROR in Futures BookTicker message handler:`, e.message, e.stack);
        }
    });

    futuresWsClient.on('pong', () => { }); // Spot sends actual pong frames, futures sends pong as message

    futuresWsClient.on('error', function error(err) {
        console.error(`[Arbitrage] PID: ${process.pid} --- Futures BookTicker WebSocket error:`, err.message);
    });

    futuresWsClient.on('close', function close(code, reason) {
        console.log(`[Arbitrage] PID: ${process.pid} --- Futures BookTicker WebSocket closed. Code: ${code}, Reason: ${reason ? reason.toString() : 'N/A'}`);
        if (futuresPingIntervalId) { clearInterval(futuresPingIntervalId); futuresPingIntervalId = null; }
        futuresWsClient = null;
        latestFuturesData = { bestBid: null, bestAsk: null, timestamp: null };
        setTimeout(connectToFuturesBookTicker, ARB_RECONNECT_INTERVAL_MS);
    });
}

// --- Start the connections and intervals ---
console.log(`[App] PID: ${process.pid} --- Binance Listener starting...`);

connectToInternalReceiver();
connectToBinanceAggTrade();

connectToSpotBookTicker();
connectToFuturesBookTicker();

if (arbitrageCheckIntervalId) {
    clearInterval(arbitrageCheckIntervalId);
}
arbitrageCheckIntervalId = setInterval(performArbitrageCheckAndSignal, ARBITRAGE_CHECK_INTERVAL_MS);
console.log(`[Arbitrage] PID: ${process.pid} --- Arbitrage check interval started, checking every ${ARBITRAGE_CHECK_INTERVAL_MS}ms.`);
