// arbitrage_module.js

const WebSocket = require('ws');

// --- Global Error Handlers (Error logs retained) ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[Arbitrage-Module] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`);
    console.error(err.stack || err);
    console.error(`[Arbitrage-Module] Exception origin: ${origin}`);
    console.error(`[Arbitrage-Module] PID: ${process.pid} --- Exiting due to uncaught exception...`);
    setTimeout(() => {
        if (internalWsClient && typeof internalWsClient.terminate === 'function') { try { internalWsClient.terminate(); } catch (e) { /* ignore */ } }
        if (spotWsClient && typeof spotWsClient.terminate === 'function') { try { spotWsClient.terminate(); } catch (e) { /* ignore */ } }
        if (futuresWsClient && typeof futuresWsClient.terminate === 'function') { try { futuresWsClient.terminate(); } catch (e) { /* ignore */ } }
        if (markPriceWsClient && typeof markPriceWsClient.terminate === 'function') { try { markPriceWsClient.terminate(); } catch (e) { /* ignore */ } }
        if (spotPingIntervalId) { try { clearInterval(spotPingIntervalId); } catch(e) { /* ignore */ } }
        if (futuresPingIntervalId) { try { clearInterval(futuresPingIntervalId); } catch(e) { /* ignore */ } }
        if (markPricePingIntervalId) { try { clearInterval(markPricePingIntervalId); } catch(e) { /* ignore */ } }
        if (arbitrageCheckIntervalId) { try { clearInterval(arbitrageCheckIntervalId); } catch(e) { /* ignore */ } }
        process.exit(1);
    }, 1000).unref();
});

process.on('unhandledRejection', (reason, promise) => {
    console.error(`[Arbitrage-Module] PID: ${process.pid} --- FATAL: UNHANDLED PROMISE REJECTION`);
    console.error('[Arbitrage-Module] Unhandled Rejection at:', promise);
    console.error('[Arbitrage-Module] Reason:', reason instanceof Error ? reason.stack : reason);
    console.error(`[Arbitrage-Module] PID: ${process.pid} --- Exiting due to unhandled promise rejection...`);
    setTimeout(() => {
        if (internalWsClient && typeof internalWsClient.terminate === 'function') { try { internalWsClient.terminate(); } catch (e) { /* ignore */ } }
        if (spotWsClient && typeof spotWsClient.terminate === 'function') { try { spotWsClient.terminate(); } catch (e) { /* ignore */ } }
        if (futuresWsClient && typeof futuresWsClient.terminate === 'function') { try { futuresWsClient.terminate(); } catch (e) { /* ignore */ } }
        if (markPriceWsClient && typeof markPriceWsClient.terminate === 'function') { try { markPriceWsClient.terminate(); } catch (e) { /* ignore */ } }
        if (spotPingIntervalId) { try { clearInterval(spotPingIntervalId); } catch(e) { /* ignore */ } }
        if (futuresPingIntervalId) { try { clearInterval(futuresPingIntervalId); } catch(e) { /* ignore */ } }
        if (markPricePingIntervalId) { try { clearInterval(markPricePingIntervalId); } catch(e) { /* ignore */ } }
        if (arbitrageCheckIntervalId) { try { clearInterval(arbitrageCheckIntervalId); } catch(e) { /* ignore */ } }
        process.exit(1);
    }, 1000).unref();
});

// --- Arbitrage Configuration ---
const internalReceiverUrl = 'ws://localhost:8082';
const SPOT_BOOKTICKER_URL = 'wss://stream.binance.com:9443/ws/btcusdt@bookTicker';
const FUTURES_BOOKTICKER_URL = 'wss://fstream.binance.com/ws/btcusdt@bookTicker';
const MARK_PRICE_STREAM_URL = 'wss://fstream.binance.com/ws/btcusdt@markPrice@1s';
const ARB_RECONNECT_INTERVAL_MS = 5000;
const ARB_PING_INTERVAL_MS = 3 * 60 * 1000;
const DESIRED_PROFIT_THRESHOLD_USD = 5.0;
const TOTAL_FEES_PER_UNIT_USD = 0.2;
const ARBITRAGE_CHECK_INTERVAL_MS = 20;

// --- Arbitrage State Variables ---
let internalWsClient = null;
let spotWsClient = null;
let futuresWsClient = null;
let markPriceWsClient = null;
let spotPingIntervalId = null;
let futuresPingIntervalId = null;
let markPricePingIntervalId = null;
let arbitrageCheckIntervalId = null;

let latestSpotData = { bestBid: null, bestAsk: null, timestamp: null };
let latestFuturesData = { bestBid: null, bestAsk: null, timestamp: null };
let latestRawPremiumIndexPercentage = null;
let latestSpotIndexPrice = null;

let lastSentArbitrageType = null;

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    // console.log(`[Arbitrage-Module] PID: ${process.pid} --- Connecting to internal receiver at ${internalReceiverUrl}...`); // Reduced
    internalWsClient = new WebSocket(internalReceiverUrl);

    internalWsClient.on('open', () => {
        // Connection success is implicit by lack of errors.
    });

    internalWsClient.on('error', (err) => {
        console.error(`[Arbitrage-Module] PID: ${process.pid} --- Internal receiver WebSocket error:`, err.message);
    });

    internalWsClient.on('close', (code, reason) => {
        console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Disconnected from internal receiver. Code: ${code}, Reason: ${reason ? reason.toString() : 'N/A'}. Reconnecting...`);
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, ARB_RECONNECT_INTERVAL_MS);
    });
}

// --- Arbitrage Logic Core (Called by Interval) ---
function performArbitrageCheckAndSignal() {
    const clearSignalPayload = { as: null };

    if (!latestSpotData.bestBid || !latestSpotData.bestAsk ||
        !latestFuturesData.bestBid || !latestFuturesData.bestAsk ||
        latestRawPremiumIndexPercentage === null || latestSpotIndexPrice === null) {
        if (lastSentArbitrageType !== null) {
            console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Opportunity (${lastSentArbitrageType}) cleared due to incomplete market/basis data.`);
            if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                try {
                    internalWsClient.send(JSON.stringify(clearSignalPayload));
                    // Successfully sent clear signal (due to incomplete data) - no log for brevity
                } catch (error) {
                    console.error(`[Arbitrage-Module] PID: ${process.pid} --- Error sending clear signal (incomplete data):`, error.message, error.stack);
                }
            } else {
                 console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Incomplete market/basis data for ${lastSentArbitrageType}, but internal client NOT OPEN. Clear signal not sent.`);
            }
            lastSentArbitrageType = null;
        }
        return;
    }

    let currentOpportunityType = null;
    let arbSignalPayload = null;
    let identifiedNetProfit = 0; // Not strictly needed for reduced logging but kept for clarity
    let spotTradePrice = null; // Not strictly needed for reduced logging
    let futuresTradePrice = null; // Not strictly needed for reduced logging

    const naturalSpotPremiumPercentage_dynamic = -latestRawPremiumIndexPercentage;
    const currentApproxSpotPriceForBasis = latestSpotIndexPrice;
    const naturalBasisSpotOverFutures_USD = naturalSpotPremiumPercentage_dynamic * currentApproxSpotPriceForBasis;

    const currentSpotBid_minus_FuturesAsk = latestSpotData.bestBid - latestFuturesData.bestAsk;
    const deviationProfitSpotSellFuturesBuy = (currentSpotBid_minus_FuturesAsk - naturalBasisSpotOverFutures_USD) - TOTAL_FEES_PER_UNIT_USD;

    const currentFuturesBid_minus_SpotAsk = latestFuturesData.bestBid - latestSpotData.bestAsk;
    const deviationProfitFuturesSellSpotBuy = (currentFuturesBid_minus_SpotAsk + naturalBasisSpotOverFutures_USD) - TOTAL_FEES_PER_UNIT_USD;


    if (deviationProfitSpotSellFuturesBuy > DESIRED_PROFIT_THRESHOLD_USD) {
        currentOpportunityType = "scenario1_SellSpotBuyFutures_Deviation";
        identifiedNetProfit = deviationProfitSpotSellFuturesBuy;
        spotTradePrice = latestSpotData.bestBid;
        futuresTradePrice = latestFuturesData.bestAsk;
        arbSignalPayload = {
            as: {
                sell: "spot", buy: "futures", np: parseFloat(identifiedNetProfit.toFixed(4)),
                spt: spotTradePrice, fpt: futuresTradePrice,
                basis: parseFloat(naturalBasisSpotOverFutures_USD.toFixed(4))
            }
        };
    } else if (deviationProfitFuturesSellSpotBuy > DESIRED_PROFIT_THRESHOLD_USD) {
        currentOpportunityType = "scenario2_SellFuturesBuySpot_Deviation";
        identifiedNetProfit = deviationProfitFuturesSellSpotBuy;
        spotTradePrice = latestSpotData.bestAsk;
        futuresTradePrice = latestFuturesData.bestBid;
        arbSignalPayload = {
            as: {
                sell: "futures", buy: "spot", np: parseFloat(identifiedNetProfit.toFixed(4)),
                spt: spotTradePrice, fpt: futuresTradePrice,
                basis: parseFloat(naturalBasisSpotOverFutures_USD.toFixed(4))
            }
        };
    }

    if (currentOpportunityType) {
        if (currentOpportunityType !== lastSentArbitrageType) {
            if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                try {
                    internalWsClient.send(JSON.stringify(arbSignalPayload));
                    // Successfully SENT Signal - no log for brevity
                    // console.log(`[Arbitrage-Module] PID: ${process.pid} --- SENT Signal. Type: ${currentOpportunityType}, NetProfit: $${identifiedNetProfit.toFixed(4)}, BasisUSD: $${arbSignalPayload.as.basis.toFixed(4)}`); // Reduced
                    lastSentArbitrageType = currentOpportunityType;
                } catch (error) {
                    console.error(`[Arbitrage-Module] PID: ${process.pid} --- Error stringifying or sending arbitrage opportunity (${currentOpportunityType}):`, error.message, error.stack);
                }
            } else {
                console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Detected ${currentOpportunityType} opportunity but internal client NOT OPEN. NetProfit: $${identifiedNetProfit.toFixed(4)}. BasisUSD: $${naturalBasisSpotOverFutures_USD.toFixed(4)}. Signal not sent.`);
            }
        }
    } else {
        if (lastSentArbitrageType !== null) {
            if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                try {
                    internalWsClient.send(JSON.stringify(clearSignalPayload));
                    // Successfully SENT Clear Signal - no log for brevity
                    // console.log(`[Arbitrage-Module] PID: ${process.pid} --- SENT Clear Signal. Previously signaled opportunity (${lastSentArbitrageType}) has now disappeared. Current BasisUSD: $${naturalBasisSpotOverFutures_USD.toFixed(4)}`); // Reduced
                    lastSentArbitrageType = null;
                } catch (error) {
                    console.error(`[Arbitrage-Module] PID: ${process.pid} --- Error stringifying or sending clear signal (opportunity ${lastSentArbitrageType} disappeared):`, error.message, error.stack);
                }
            } else {
                console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Previously signaled opportunity (${lastSentArbitrageType}) disappeared, but internal client NOT OPEN. Clear signal not sent.`);
                lastSentArbitrageType = null;
            }
        }
    }
}

// --- Spot BookTicker Connection ---
function connectToSpotBookTicker() {
    if (spotWsClient && (spotWsClient.readyState === WebSocket.OPEN || spotWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    // console.log(`[Arbitrage-Module] PID: ${process.pid} --- Connecting to Spot BookTicker...`); // Reduced
    spotWsClient = new WebSocket(SPOT_BOOKTICKER_URL);

    spotWsClient.on('open', function open() {
        if (spotPingIntervalId) clearInterval(spotPingIntervalId);
        spotPingIntervalId = setInterval(() => {
            if (spotWsClient && spotWsClient.readyState === WebSocket.OPEN) {
                try {
                    spotWsClient.ping(() => {});
                } catch (pingError) {
                    console.error(`[Arbitrage-Module] PID: ${process.pid} --- Error sending ping to Spot BookTicker:`, pingError.message);
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
                    console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Invalid price in Spot BookTicker data: Bid=${tickerData.b}, Ask=${tickerData.a}`);
                    latestSpotData.bestBid = null; latestSpotData.bestAsk = null;
                    return;
                }
                latestSpotData.bestBid = newSpotBestBid;
                latestSpotData.bestAsk = newSpotBestAsk;
                latestSpotData.timestamp = tickerData.E || Date.now();
            }
        } catch (e) {
            console.error(`[Arbitrage-Module] PID: ${process.pid} --- CRITICAL ERROR in Spot BookTicker message handler:`, e.message, e.stack);
        }
    });

    spotWsClient.on('pong', () => { });

    spotWsClient.on('error', function error(err) {
        console.error(`[Arbitrage-Module] PID: ${process.pid} --- Spot BookTicker WebSocket error:`, err.message);
    });

    spotWsClient.on('close', function close(code, reason) {
        console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Spot BookTicker WebSocket closed. Code: ${code}, Reason: ${reason ? reason.toString() : 'N/A'}. Reconnecting...`);
        if (spotPingIntervalId) { clearInterval(spotPingIntervalId); spotPingIntervalId = null; }
        spotWsClient = null;
        latestSpotData = { bestBid: null, bestAsk: null, timestamp: null };
        setTimeout(connectToSpotBookTicker, ARB_RECONNECT_INTERVAL_MS);
    });
}

// --- Futures BookTicker Connection ---
function connectToFuturesBookTicker() {
    if (futuresWsClient && (futuresWsClient.readyState === WebSocket.OPEN || futuresWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    // console.log(`[Arbitrage-Module] PID: ${process.pid} --- Connecting to Futures BookTicker...`); // Reduced
    futuresWsClient = new WebSocket(FUTURES_BOOKTICKER_URL);

    futuresWsClient.on('open', function open() {
        if (futuresPingIntervalId) clearInterval(futuresPingIntervalId);
        futuresPingIntervalId = setInterval(() => {
            if (futuresWsClient && futuresWsClient.readyState === WebSocket.OPEN) {
                try {
                    futuresWsClient.ping(() => {});
                } catch (pingError) {
                    console.error(`[Arbitrage-Module] PID: ${process.pid} --- Error sending ping to Futures BookTicker:`, pingError.message);
                }
            }
        }, ARB_PING_INTERVAL_MS);
    });

    futuresWsClient.on('message', function incoming(data) {
        try {
            const messageString = data.toString();
            if (messageString.includes('"e":"pong"') || messageString.trim().toLowerCase() === 'pong') {
                return;
            }
            const tickerData = JSON.parse(messageString);

            if (tickerData && typeof tickerData.b !== 'undefined' && typeof tickerData.a !== 'undefined') {
                const newFuturesBestBid = parseFloat(tickerData.b);
                const newFuturesBestAsk = parseFloat(tickerData.a);

                if (isNaN(newFuturesBestBid) || isNaN(newFuturesBestAsk)) {
                    console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Invalid price in Futures BookTicker data: Bid=${tickerData.b}, Ask=${tickerData.a}`);
                    latestFuturesData.bestBid = null; latestFuturesData.bestAsk = null;
                    return;
                }
                latestFuturesData.bestBid = newFuturesBestBid;
                latestFuturesData.bestAsk = newFuturesBestAsk;
                latestFuturesData.timestamp = tickerData.E || Date.now();
            }
        } catch (e) {
            console.error(`[Arbitrage-Module] PID: ${process.pid} --- CRITICAL ERROR in Futures BookTicker message handler:`, e.message, e.stack);
        }
    });

    futuresWsClient.on('pong', () => { });

    futuresWsClient.on('error', function error(err) {
        console.error(`[Arbitrage-Module] PID: ${process.pid} --- Futures BookTicker WebSocket error:`, err.message);
    });

    futuresWsClient.on('close', function close(code, reason) {
        console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Futures BookTicker WebSocket closed. Code: ${code}, Reason: ${reason ? reason.toString() : 'N/A'}. Reconnecting...`);
        if (futuresPingIntervalId) { clearInterval(futuresPingIntervalId); futuresPingIntervalId = null; }
        futuresWsClient = null;
        latestFuturesData = { bestBid: null, bestAsk: null, timestamp: null };
        setTimeout(connectToFuturesBookTicker, ARB_RECONNECT_INTERVAL_MS);
    });
}

// --- Mark Price Stream Connection ---
function connectToMarkPriceStream() {
    if (markPriceWsClient && (markPriceWsClient.readyState === WebSocket.OPEN || markPriceWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    // console.log(`[Arbitrage-Module] PID: ${process.pid} --- Connecting to Mark Price Stream (${MARK_PRICE_STREAM_URL})...`); // Reduced
    markPriceWsClient = new WebSocket(MARK_PRICE_STREAM_URL);

    markPriceWsClient.on('open', function open() {
        if (markPricePingIntervalId) clearInterval(markPricePingIntervalId);
        markPricePingIntervalId = setInterval(() => {
            if (markPriceWsClient && markPriceWsClient.readyState === WebSocket.OPEN) {
                try {
                    markPriceWsClient.ping(() => {});
                } catch (pingError) {
                    console.error(`[Arbitrage-Module] PID: ${process.pid} --- Error sending ping to Mark Price Stream:`, pingError.message);
                }
            }
        }, ARB_PING_INTERVAL_MS);
    });

    markPriceWsClient.on('message', function incoming(data) {
        try {
            const messageString = data.toString();
            const tickerData = JSON.parse(messageString);

            if (tickerData && typeof tickerData.p !== 'undefined' && typeof tickerData.i !== 'undefined') {
                const markPrice = parseFloat(tickerData.p);
                const indexPrice = parseFloat(tickerData.i);

                if (isNaN(markPrice) || isNaN(indexPrice)) {
                    console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Invalid numeric value in Mark Price stream: MarkP=${tickerData.p}, IndexP=${tickerData.i}`);
                    latestRawPremiumIndexPercentage = null;
                    latestSpotIndexPrice = null;
                    return;
                }
                
                if (indexPrice === 0) {
                    console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Index Price is zero in Mark Price stream. Cannot calculate premium percentage. MarkP=${tickerData.p}, IndexP=${tickerData.i}`);
                    latestRawPremiumIndexPercentage = null;
                    latestSpotIndexPrice = indexPrice;
                    return;
                }

                latestSpotIndexPrice = indexPrice;
                latestRawPremiumIndexPercentage = (markPrice - indexPrice) / indexPrice;
            }
        } catch (e) {
            console.error(`[Arbitrage-Module] PID: ${process.pid} --- CRITICAL ERROR in Mark Price Stream message handler:`, e.message, e.stack);
            latestRawPremiumIndexPercentage = null;
            latestSpotIndexPrice = null;
        }
    });

    markPriceWsClient.on('pong', () => { });

    markPriceWsClient.on('error', function error(err) {
        console.error(`[Arbitrage-Module] PID: ${process.pid} --- Mark Price Stream WebSocket error:`, err.message);
    });

    markPriceWsClient.on('close', function close(code, reason) {
        console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Mark Price Stream WebSocket closed. Code: ${code}, Reason: ${reason ? reason.toString() : 'N/A'}. Reconnecting...`);
        if (markPricePingIntervalId) { clearInterval(markPricePingIntervalId); markPricePingIntervalId = null; }
        markPriceWsClient = null;
        latestRawPremiumIndexPercentage = null;
        latestSpotIndexPrice = null;
        setTimeout(connectToMarkPriceStream, ARB_RECONNECT_INTERVAL_MS);
    });
}


// --- Start the connections and intervals ---
// console.log(`[Arbitrage-Module] PID: ${process.pid} --- Arbitrage Module starting...`); // Reduced

connectToInternalReceiver();
connectToSpotBookTicker();
connectToFuturesBookTicker();
connectToMarkPriceStream();

if (arbitrageCheckIntervalId) {
    clearInterval(arbitrageCheckIntervalId);
}
arbitrageCheckIntervalId = setInterval(performArbitrageCheckAndSignal, ARBITRAGE_CHECK_INTERVAL_MS);
// console.log(`[Arbitrage-Module] PID: ${process.pid} --- Arbitrage check interval started, checking every ${ARBITRAGE_CHECK_INTERVAL_MS}ms.`); // Reduced
