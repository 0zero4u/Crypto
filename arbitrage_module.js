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

const DESIRED_PROFIT_THRESHOLD_USD = 5.0;    // Entry threshold for a new signal
const CLEARANCE_PROFIT_FLOOR_USD = 1.5;     // Floor to keep an existing signal active
const TOTAL_FEES_PER_UNIT_USD = 0.2;
const ARBITRAGE_CHECK_INTERVAL_MS = 50;

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

let lastSentArbitrageType = null; // Stores the type of the last *sent* signal ("scenario1_...", "scenario2_...", or null if clear signal sent)

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    internalWsClient = new WebSocket(internalReceiverUrl);

    internalWsClient.on('open', () => { /* Connection success is implicit */ });

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
        // Market data or basis data is incomplete
        if (lastSentArbitrageType !== null) { // If an opportunity was previously active
            console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Opportunity (${lastSentArbitrageType}) cleared due to incomplete market/basis data.`);
            if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                try {
                    internalWsClient.send(JSON.stringify(clearSignalPayload));
                } catch (error) {
                    console.error(`[Arbitrage-Module] PID: ${process.pid} --- Error sending clear signal (incomplete data):`, error.message, error.stack);
                }
            } else {
                 console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Incomplete market/basis data for ${lastSentArbitrageType}, but internal client NOT OPEN. Clear signal not sent.`);
            }
            lastSentArbitrageType = null; // Mark that a clear state has been signaled (or attempted)
        }
        return; // Exit early as no valid calculation can be made
    }

    // Market data and basis data are complete, proceed with arbitrage calculation
    let newOpportunityType = null; // For a *new* opportunity based on ENTRY threshold
    let newArbSignalPayload = null;
    let identifiedNetProfitForNewSignal = 0; // Profit for a new signal

    // Calculate dynamic basis
    const naturalSpotPremiumPercentage_dynamic = -latestRawPremiumIndexPercentage;
    const currentApproxSpotPriceForBasis = latestSpotIndexPrice;
    const naturalBasisSpotOverFutures_USD = naturalSpotPremiumPercentage_dynamic * currentApproxSpotPriceForBasis;

    // Calculate current potential profits for BOTH scenarios
    // Scenario 1: Sell Spot, Buy Futures (Exploiting Spot being "too high" relative to its natural premium)
    const currentSpotBid_minus_FuturesAsk_S1 = latestSpotData.bestBid - latestFuturesData.bestAsk;
    const deviationProfitSpotSellFuturesBuy = (currentSpotBid_minus_FuturesAsk_S1 - naturalBasisSpotOverFutures_USD) - TOTAL_FEES_PER_UNIT_USD;

    // Scenario 2: Sell Futures, Buy Spot (Exploiting Futures being "too high" or Spot's premium shrinking/reversing)
    const currentFuturesBid_minus_SpotAsk_S2 = latestFuturesData.bestBid - latestSpotData.bestAsk;
    const deviationProfitFuturesSellSpotBuy = (currentFuturesBid_minus_SpotAsk_S2 + naturalBasisSpotOverFutures_USD) - TOTAL_FEES_PER_UNIT_USD;

    // Check for a NEW opportunity exceeding ENTRY threshold (DESIRED_PROFIT_THRESHOLD_USD)
    if (deviationProfitSpotSellFuturesBuy > DESIRED_PROFIT_THRESHOLD_USD) {
        newOpportunityType = "scenario1_SellSpotBuyFutures_Deviation";
        identifiedNetProfitForNewSignal = deviationProfitSpotSellFuturesBuy;
        newArbSignalPayload = {
            as: {
                sell: "spot", buy: "futures", np: parseFloat(identifiedNetProfitForNewSignal.toFixed(4)),
                spt: latestSpotData.bestBid, fpt: latestFuturesData.bestAsk,
                basis: parseFloat(naturalBasisSpotOverFutures_USD.toFixed(4))
            }
        };
    } else if (deviationProfitFuturesSellSpotBuy > DESIRED_PROFIT_THRESHOLD_USD) {
        newOpportunityType = "scenario2_SellFuturesBuySpot_Deviation";
        identifiedNetProfitForNewSignal = deviationProfitFuturesSellSpotBuy;
        newArbSignalPayload = {
            as: {
                sell: "futures", buy: "spot", np: parseFloat(identifiedNetProfitForNewSignal.toFixed(4)),
                spt: latestSpotData.bestAsk, fpt: latestFuturesData.bestBid,
                basis: parseFloat(naturalBasisSpotOverFutures_USD.toFixed(4))
            }
        };
    }

    // Now, decide whether to send an opportunity signal or a clear signal
    if (newOpportunityType) {
        // A new opportunity (or a switch to a different type) meeting ENTRY threshold IS identified
        if (newOpportunityType !== lastSentArbitrageType) {
            // It's a new type or a different type of opportunity than last sent
            if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                try {
                    internalWsClient.send(JSON.stringify(newArbSignalPayload));
                    // Reduced log: console.log(`[Arbitrage-Module] PID: ${process.pid} --- SENT New Signal. Type: ${newOpportunityType}, NetProfit: $${identifiedNetProfitForNewSignal.toFixed(4)}, BasisUSD: $${newArbSignalPayload.as.basis.toFixed(4)}`);
                    lastSentArbitrageType = newOpportunityType;
                } catch (error) {
                    console.error(`[Arbitrage-Module] PID: ${process.pid} --- Error stringifying or sending new arbitrage opportunity (${newOpportunityType}):`, error.message, error.stack);
                }
            } else {
                console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Detected ${newOpportunityType} opportunity but internal client NOT OPEN. NetProfit: $${identifiedNetProfitForNewSignal.toFixed(4)}. BasisUSD: $${naturalBasisSpotOverFutures_USD.toFixed(4)}. Signal not sent.`);
            }
        }
        // If newOpportunityType === lastSentArbitrageType, do nothing (opportunity persists with same type and above ENTRY, no flood)
    } else {
        // NO *new* opportunity meeting ENTRY threshold currently identified.
        // Check if a PREVIOUSLY active opportunity (lastSentArbitrageType) should now be cleared because it dropped below CLEARANCE_PROFIT_FLOOR_USD.
        if (lastSentArbitrageType !== null) {
            let profitOfLastActiveType = -Infinity; // Default to ensure it clears if logic error/unknown type

            if (lastSentArbitrageType === "scenario1_SellSpotBuyFutures_Deviation") {
                profitOfLastActiveType = deviationProfitSpotSellFuturesBuy;
            } else if (lastSentArbitrageType === "scenario2_SellFuturesBuySpot_Deviation") {
                profitOfLastActiveType = deviationProfitFuturesSellSpotBuy;
            }

            if (profitOfLastActiveType < CLEARANCE_PROFIT_FLOOR_USD) {
                // The previously active opportunity has now fallen below the CLEARANCE floor. Send a clear signal.
                if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                    try {
                        internalWsClient.send(JSON.stringify(clearSignalPayload));
                        // Reduced log: console.warn(`[Arbitrage-Module] PID: ${process.pid} --- SENT Clear Signal. ${lastSentArbitrageType} profit (${profitOfLastActiveType.toFixed(4)}) fell below clearance floor (${CLEARANCE_PROFIT_FLOOR_USD}). BasisUSD: ${naturalBasisSpotOverFutures_USD.toFixed(4)}`);
                        lastSentArbitrageType = null; // Mark that a clear state has been signaled
                    } catch (error) {
                        console.error(`[Arbitrage-Module] PID: ${process.pid} --- Error stringifying or sending clear signal (below clearance):`, error.message, error.stack);
                    }
                } else {
                    console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Previously signaled opportunity (${lastSentArbitrageType}) profit (${profitOfLastActiveType.toFixed(4)}) dropped below clearance floor, but internal client NOT OPEN. Clear signal not sent.`);
                    lastSentArbitrageType = null; // Still update state to reflect opportunity is gone by this stricter criteria
                }
            }
            // If profitOfLastActiveType is NOT < CLEARANCE_PROFIT_FLOOR_USD, then the opportunity, while not meeting
            // the ENTRY threshold for a *new* signal, is still "active" (above the clearance floor). So, do nothing.
        }
        // If lastSentArbitrageType was already null, do nothing (still no opportunity, no change in signaled state)
    }
}

// --- Spot BookTicker Connection ---
function connectToSpotBookTicker() {
    if (spotWsClient && (spotWsClient.readyState === WebSocket.OPEN || spotWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    spotWsClient = new WebSocket(SPOT_BOOKTICKER_URL);

    spotWsClient.on('open', function open() {
        if (spotPingIntervalId) clearInterval(spotPingIntervalId);
        spotPingIntervalId = setInterval(() => {
            if (spotWsClient && spotWsClient.readyState === WebSocket.OPEN) {
                try { spotWsClient.ping(() => {}); } catch (pingError) {
                    console.error(`[Arbitrage-Module] PID: ${process.pid} --- Error sending ping to Spot BookTicker:`, pingError.message);
                }
            }
        }, ARB_PING_INTERVAL_MS);
    });

    spotWsClient.on('message', function incoming(data) {
        try {
            const tickerData = JSON.parse(data.toString());
            if (tickerData && typeof tickerData.b !== 'undefined' && typeof tickerData.a !== 'undefined') {
                const newSpotBestBid = parseFloat(tickerData.b);
                const newSpotBestAsk = parseFloat(tickerData.a);
                if (isNaN(newSpotBestBid) || isNaN(newSpotBestAsk)) {
                    console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Invalid price in Spot BookTicker data: Bid=${tickerData.b}, Ask=${tickerData.a}`);
                    latestSpotData.bestBid = null; latestSpotData.bestAsk = null; return;
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
    futuresWsClient = new WebSocket(FUTURES_BOOKTICKER_URL);

    futuresWsClient.on('open', function open() {
        if (futuresPingIntervalId) clearInterval(futuresPingIntervalId);
        futuresPingIntervalId = setInterval(() => {
            if (futuresWsClient && futuresWsClient.readyState === WebSocket.OPEN) {
                try { futuresWsClient.ping(() => {}); } catch (pingError) {
                    console.error(`[Arbitrage-Module] PID: ${process.pid} --- Error sending ping to Futures BookTicker:`, pingError.message);
                }
            }
        }, ARB_PING_INTERVAL_MS);
    });

    futuresWsClient.on('message', function incoming(data) {
        try {
            const messageString = data.toString();
            if (messageString.includes('"e":"pong"') || messageString.trim().toLowerCase() === 'pong') return;
            const tickerData = JSON.parse(messageString);
            if (tickerData && typeof tickerData.b !== 'undefined' && typeof tickerData.a !== 'undefined') {
                const newFuturesBestBid = parseFloat(tickerData.b);
                const newFuturesBestAsk = parseFloat(tickerData.a);
                if (isNaN(newFuturesBestBid) || isNaN(newFuturesBestAsk)) {
                    console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Invalid price in Futures BookTicker data: Bid=${tickerData.b}, Ask=${tickerData.a}`);
                    latestFuturesData.bestBid = null; latestFuturesData.bestAsk = null; return;
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
    markPriceWsClient = new WebSocket(MARK_PRICE_STREAM_URL);

    markPriceWsClient.on('open', function open() {
        if (markPricePingIntervalId) clearInterval(markPricePingIntervalId);
        markPricePingIntervalId = setInterval(() => {
            if (markPriceWsClient && markPriceWsClient.readyState === WebSocket.OPEN) {
                try { markPriceWsClient.ping(() => {}); } catch (pingError) {
                    console.error(`[Arbitrage-Module] PID: ${process.pid} --- Error sending ping to Mark Price Stream:`, pingError.message);
                }
            }
        }, ARB_PING_INTERVAL_MS);
    });

    markPriceWsClient.on('message', function incoming(data) {
        try {
            const tickerData = JSON.parse(data.toString());
            if (tickerData && typeof tickerData.p !== 'undefined' && typeof tickerData.i !== 'undefined') {
                const markPrice = parseFloat(tickerData.p);
                const indexPrice = parseFloat(tickerData.i);
                if (isNaN(markPrice) || isNaN(indexPrice)) {
                    console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Invalid numeric value in Mark Price stream: MarkP=${tickerData.p}, IndexP=${tickerData.i}`);
                    latestRawPremiumIndexPercentage = null; latestSpotIndexPrice = null; return;
                }
                if (indexPrice === 0) {
                    console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Index Price is zero in Mark Price stream. MarkP=${tickerData.p}, IndexP=${tickerData.i}`);
                    latestRawPremiumIndexPercentage = null; latestSpotIndexPrice = indexPrice; return;
                }
                latestSpotIndexPrice = indexPrice;
                latestRawPremiumIndexPercentage = (markPrice - indexPrice) / indexPrice;
            }
        } catch (e) {
            console.error(`[Arbitrage-Module] PID: ${process.pid} --- CRITICAL ERROR in Mark Price Stream message handler:`, e.message, e.stack);
            latestRawPremiumIndexPercentage = null; latestSpotIndexPrice = null;
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
        latestRawPremiumIndexPercentage = null; latestSpotIndexPrice = null;
        setTimeout(connectToMarkPriceStream, ARB_RECONNECT_INTERVAL_MS);
    });
}

// --- Start the connections and intervals ---
connectToInternalReceiver();
connectToSpotBookTicker();
connectToFuturesBookTicker();
connectToMarkPriceStream();

if (arbitrageCheckIntervalId) {
    clearInterval(arbitrageCheckIntervalId);
}
arbitrageCheckIntervalId = setInterval(performArbitrageCheckAndSignal, ARBITRAGE_CHECK_INTERVAL_MS);
// Reduced log: console.log(`[Arbitrage-Module] PID: ${process.pid} --- Arbitrage check interval started, checking every ${ARBITRAGE_CHECK_INTERVAL_MS}ms.`);
