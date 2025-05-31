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
const CLEARANCE_PROFIT_FLOOR_USD = 1.5;
const TOTAL_FEES_PER_UNIT_USD = 0.2;
const ARBITRAGE_CHECK_INTERVAL_MS = 100;

// --- Book Skew Configuration ---
const FAVORABLE_SKEW_RATIO_SELL_SIDE_HEAVY = 1.5; // For selling: AskQty / BidQty >= this
const FAVORABLE_SKEW_RATIO_BUY_SIDE_HEAVY = 1.13;  // For buying: BidQty / AskQty >= this
const MIN_TOTAL_TOP_LEVEL_QTY_FOR_SKEW_CHECK = 0.1;
const MODERATE_SKEW_PENALTY_USD = 4.0; // Penalty for "Meh" (moderately unfavorable) skew
const CRITICAL_SKEW_PENALTY_USD = 9999.0; // Effectively a veto for "Woah" (critically unfavorable) skew

// --- Arbitrage State Variables ---
let internalWsClient = null;
let spotWsClient = null;
let futuresWsClient = null;
let markPriceWsClient = null;
let spotPingIntervalId = null;
let futuresPingIntervalId = null;
let markPricePingIntervalId = null;
let arbitrageCheckIntervalId = null;

let latestSpotData = { bestBid: null, bestAsk: null, bestBidQty: null, bestAskQty: null, timestamp: null };
let latestFuturesData = { bestBid: null, bestAsk: null, bestBidQty: null, bestAskQty: null, timestamp: null };
let latestRawPremiumIndexPercentage = null;
let latestSpotIndexPrice = null;

let lastSentArbitrageType = null;
// For more robustly tracking last sent profit to avoid resending for minor NP increases
let lastSentSignalDetails = { type: null, np: 0 };


// --- Skew Status Levels ---
const SKEW_STATUS = {
    FAVORABLE: "FAVORABLE",
    MODERATELY_UNFAVORABLE: "MODERATELY_UNFAVORABLE", // Skew in correct direction but not meeting favorable threshold
    CRITICALLY_UNFAVORABLE: "CRITICALLY_UNFAVORABLE", // Skew in opposite direction or extremely poor
    LOW_LIQUIDITY: "LOW_LIQUIDITY",
    MISSING_DATA: "MISSING_DATA",
    INVALID_ACTION: "INVALID_ACTION",
    NOT_CHECKED: "NOT_CHECKED" // Default initial status
};

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    internalWsClient = new WebSocket(internalReceiverUrl);
    internalWsClient.on('open', () => { console.log(`[Arbitrage-Module] PID: ${process.pid} --- Connected to internal receiver.`); });
    internalWsClient.on('error', (err) => { console.error(`[Arbitrage-Module] PID: ${process.pid} --- Internal receiver WebSocket error:`, err.message); });
    internalWsClient.on('close', (code, reason) => {
        console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Disconnected from internal receiver. Code: ${code}, Reason: ${reason ? reason.toString() : 'N/A'}. Reconnecting...`);
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, ARB_RECONNECT_INTERVAL_MS);
    });
}

// --- Helper function for Book Skew Check ---
function getBookSkewAnalysis(bestBidQty, bestAskQty, action, marketName = "Market") {
    if (bestBidQty === null || bestAskQty === null) {
        // console.log(`[BookSkew-${marketName}] Check for ${action}: Quantity data missing.`);
        return { status: SKEW_STATUS.MISSING_DATA, ratio: NaN, reason: "Quantity data missing" };
    }

    const totalQty = bestBidQty + bestAskQty;
    if (totalQty < MIN_TOTAL_TOP_LEVEL_QTY_FOR_SKEW_CHECK) {
        // console.log(`[BookSkew-${marketName}] Check for ${action}: Low liquidity (${totalQty.toFixed(4)}).`);
        return { status: SKEW_STATUS.LOW_LIQUIDITY, ratio: NaN, reason: `Low liquidity: ${totalQty.toFixed(4)}` };
    }

    let ratio;
    let currentStatus;

    if (action === "SELL") { // We want AskQty / BidQty to be high
        if (bestBidQty <= 0.00000001) { // Effectively zero bid
            ratio = Infinity;
            currentStatus = bestAskQty > 0.00000001 ? SKEW_STATUS.FAVORABLE : SKEW_STATUS.CRITICALLY_UNFAVORABLE;
        } else {
            ratio = bestAskQty / bestBidQty;
            if (ratio >= FAVORABLE_SKEW_RATIO_SELL_SIDE_HEAVY) {
                currentStatus = SKEW_STATUS.FAVORABLE;
            } else if (ratio >= 1.0) { // Asks >= Bids, but not strongly favorable
                currentStatus = SKEW_STATUS.MODERATELY_UNFAVORABLE;
            } else { // ratio < 1.0 (Bids > Asks) - critically wrong for selling
                currentStatus = SKEW_STATUS.CRITICALLY_UNFAVORABLE;
            }
        }
    } else if (action === "BUY") { // We want BidQty / AskQty to be high
        if (bestAskQty <= 0.00000001) { // Effectively zero ask
            ratio = Infinity; //  Bid/Ask is effectively infinite
            currentStatus = bestBidQty > 0.00000001 ? SKEW_STATUS.FAVORABLE : SKEW_STATUS.CRITICALLY_UNFAVORABLE;
        } else {
            ratio = bestBidQty / bestAskQty;
            if (ratio >= FAVORABLE_SKEW_RATIO_BUY_SIDE_HEAVY) {
                currentStatus = SKEW_STATUS.FAVORABLE;
            } else if (ratio >= 1.0) { // Bids >= Asks, but not strongly favorable
                currentStatus = SKEW_STATUS.MODERATELY_UNFAVORABLE;
            } else { // ratio < 1.0 (Asks > Bids) - critically wrong for buying
                currentStatus = SKEW_STATUS.CRITICALLY_UNFAVORABLE;
            }
        }
    } else {
        console.error(`[BookSkew-${marketName}] Invalid action: ${action}`);
        return { status: SKEW_STATUS.INVALID_ACTION, ratio: NaN, reason: "Invalid action" };
    }
    return { status: currentStatus, ratio: isFinite(ratio) ? ratio : (action === "SELL" && bestBidQty <= 0.00000001 ? 9999 : (action === "BUY" && bestAskQty <= 0.00000001 ? 9999 : 0)), reason: `Ratio (${action === "SELL" ? "AQ/BQ" : "BQ/AQ"}): ${isFinite(ratio) ? ratio.toFixed(2) : "Inf"}` };
}


// --- Arbitrage Logic Core (Called by Interval) ---
function performArbitrageCheckAndSignal() {
    const clearSignalPayload = { as: null };

    if (!latestSpotData.bestBid || !latestSpotData.bestAsk || latestSpotData.bestBidQty === null || latestSpotData.bestAskQty === null ||
        !latestFuturesData.bestBid || !latestFuturesData.bestAsk || latestFuturesData.bestBidQty === null || latestFuturesData.bestAskQty === null ||
        latestRawPremiumIndexPercentage === null || latestSpotIndexPrice === null) {
        if (lastSentArbitrageType !== null) {
            console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Opportunity (${lastSentArbitrageType}) cleared: Incomplete market/basis/qty data.`);
            if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                try { internalWsClient.send(JSON.stringify(clearSignalPayload)); } catch (error) { console.error(`[Arbitrage-Module] Error sending clear signal (incomplete data):`, error.message); }
            } else { console.warn(`[Arbitrage-Module] Incomplete data, internal client NOT OPEN. Clear signal not sent.`); }
            lastSentArbitrageType = null;
            lastSentSignalDetails = { type: null, np: 0 };
        }
        return;
    }

    let newOpportunityType = null;
    let newArbSignalPayload = null;
    let identifiedNetProfitForNewSignal = 0;

    const naturalBasisSpotOverFutures_USD = (-latestRawPremiumIndexPercentage) * latestSpotIndexPrice;

    let deviationProfitSpotSellFuturesBuy = (latestSpotData.bestBid - latestFuturesData.bestAsk - naturalBasisSpotOverFutures_USD) - TOTAL_FEES_PER_UNIT_USD;
    let deviationProfitFuturesSellSpotBuy = (latestFuturesData.bestBid - latestSpotData.bestAsk + naturalBasisSpotOverFutures_USD) - TOTAL_FEES_PER_UNIT_USD;

    let s1OverallSkewStatus = SKEW_STATUS.NOT_CHECKED;
    let s1SpotSkewDetails = { status: SKEW_STATUS.NOT_CHECKED, ratio: NaN };
    let s1FuturesSkewDetails = { status: SKEW_STATUS.NOT_CHECKED, ratio: NaN };

    let s2OverallSkewStatus = SKEW_STATUS.NOT_CHECKED;
    let s2SpotSkewDetails = { status: SKEW_STATUS.NOT_CHECKED, ratio: NaN };
    let s2FuturesSkewDetails = { status: SKEW_STATUS.NOT_CHECKED, ratio: NaN };


    // Scenario 1: Sell Spot, Buy Futures
    // Check if profit is high enough to even bother with skew check (optimisation)
    const s1MinProfitForCheck = DESIRED_PROFIT_THRESHOLD_USD - Math.max(MODERATE_SKEW_PENALTY_USD, CRITICAL_SKEW_PENALTY_USD) - 0.01;
    if (deviationProfitSpotSellFuturesBuy > s1MinProfitForCheck) {
        s1SpotSkewDetails = getBookSkewAnalysis(latestSpotData.bestBidQty, latestSpotData.bestAskQty, "SELL", "SpotS1");
        s1FuturesSkewDetails = getBookSkewAnalysis(latestFuturesData.bestBidQty, latestFuturesData.bestAskQty, "BUY", "FuturesS1");

        if ([s1SpotSkewDetails.status, s1FuturesSkewDetails.status].includes(SKEW_STATUS.CRITICALLY_UNFAVORABLE) ||
            [s1SpotSkewDetails.status, s1FuturesSkewDetails.status].includes(SKEW_STATUS.LOW_LIQUIDITY) ||
            [s1SpotSkewDetails.status, s1FuturesSkewDetails.status].includes(SKEW_STATUS.MISSING_DATA) ) {
            s1OverallSkewStatus = SKEW_STATUS.CRITICALLY_UNFAVORABLE;
            deviationProfitSpotSellFuturesBuy -= CRITICAL_SKEW_PENALTY_USD;
        } else if ([s1SpotSkewDetails.status, s1FuturesSkewDetails.status].includes(SKEW_STATUS.MODERATELY_UNFAVORABLE)) {
            s1OverallSkewStatus = SKEW_STATUS.MODERATELY_UNFAVORABLE;
            deviationProfitSpotSellFuturesBuy -= MODERATE_SKEW_PENALTY_USD;
        } else if (s1SpotSkewDetails.status === SKEW_STATUS.FAVORABLE && s1FuturesSkewDetails.status === SKEW_STATUS.FAVORABLE) {
            s1OverallSkewStatus = SKEW_STATUS.FAVORABLE;
            // No penalty
        } else {
            s1OverallSkewStatus = SKEW_STATUS.CRITICALLY_UNFAVORABLE; // Default catch-all if logic missed a case
            deviationProfitSpotSellFuturesBuy -= CRITICAL_SKEW_PENALTY_USD;
        }
    }


    // Scenario 2: Sell Futures, Buy Spot
    const s2MinProfitForCheck = DESIRED_PROFIT_THRESHOLD_USD - Math.max(MODERATE_SKEW_PENALTY_USD, CRITICAL_SKEW_PENALTY_USD) - 0.01;
    if (deviationProfitFuturesSellSpotBuy > s2MinProfitForCheck) {
        s2SpotSkewDetails = getBookSkewAnalysis(latestSpotData.bestBidQty, latestSpotData.bestAskQty, "BUY", "SpotS2");
        s2FuturesSkewDetails = getBookSkewAnalysis(latestFuturesData.bestBidQty, latestFuturesData.bestAskQty, "SELL", "FuturesS2");

        if ([s2SpotSkewDetails.status, s2FuturesSkewDetails.status].includes(SKEW_STATUS.CRITICALLY_UNFAVORABLE) ||
            [s2SpotSkewDetails.status, s2FuturesSkewDetails.status].includes(SKEW_STATUS.LOW_LIQUIDITY) ||
            [s2SpotSkewDetails.status, s2FuturesSkewDetails.status].includes(SKEW_STATUS.MISSING_DATA)) {
            s2OverallSkewStatus = SKEW_STATUS.CRITICALLY_UNFAVORABLE;
            deviationProfitFuturesSellSpotBuy -= CRITICAL_SKEW_PENALTY_USD;
        } else if ([s2SpotSkewDetails.status, s2FuturesSkewDetails.status].includes(SKEW_STATUS.MODERATELY_UNFAVORABLE)) {
            s2OverallSkewStatus = SKEW_STATUS.MODERATELY_UNFAVORABLE;
            deviationProfitFuturesSellSpotBuy -= MODERATE_SKEW_PENALTY_USD;
        } else if (s2SpotSkewDetails.status === SKEW_STATUS.FAVORABLE && s2FuturesSkewDetails.status === SKEW_STATUS.FAVORABLE) {
            s2OverallSkewStatus = SKEW_STATUS.FAVORABLE;
        } else {
            s2OverallSkewStatus = SKEW_STATUS.CRITICALLY_UNFAVORABLE;
            deviationProfitFuturesSellSpotBuy -= CRITICAL_SKEW_PENALTY_USD;
        }
    }


    if (deviationProfitSpotSellFuturesBuy > DESIRED_PROFIT_THRESHOLD_USD) {
        newOpportunityType = "scenario1_SellSpotBuyFutures_Deviation";
        identifiedNetProfitForNewSignal = deviationProfitSpotSellFuturesBuy;
        newArbSignalPayload = {
            as: {
                type: newOpportunityType, sell: "spot", buy: "futures", np: parseFloat(identifiedNetProfitForNewSignal.toFixed(4)),
                spt: latestSpotData.bestBid, fpt: latestFuturesData.bestAsk,
                basis: parseFloat(naturalBasisSpotOverFutures_USD.toFixed(4)),
                sBQ: latestSpotData.bestBidQty, sAQ: latestSpotData.bestAskQty, sSR: parseFloat(s1SpotSkewDetails.ratio.toFixed(2)),
                fBQ: latestFuturesData.bestBidQty, fAQ: latestFuturesData.bestAskQty, fSR: parseFloat(s1FuturesSkewDetails.ratio.toFixed(2)),
                skewStatus: s1OverallSkewStatus
            }
        };
    } else if (deviationProfitFuturesSellSpotBuy > DESIRED_PROFIT_THRESHOLD_USD) {
        newOpportunityType = "scenario2_SellFuturesBuySpot_Deviation";
        identifiedNetProfitForNewSignal = deviationProfitFuturesSellSpotBuy;
        newArbSignalPayload = {
            as: {
                type: newOpportunityType, sell: "futures", buy: "spot", np: parseFloat(identifiedNetProfitForNewSignal.toFixed(4)),
                spt: latestSpotData.bestAsk, fpt: latestFuturesData.bestBid,
                basis: parseFloat(naturalBasisSpotOverFutures_USD.toFixed(4)),
                sBQ: latestSpotData.bestBidQty, sAQ: latestSpotData.bestAskQty, sSR: parseFloat(s2SpotSkewDetails.ratio.toFixed(2)),
                fBQ: latestFuturesData.bestBidQty, fAQ: latestFuturesData.bestAskQty, fSR: parseFloat(s2FuturesSkewDetails.ratio.toFixed(2)),
                skewStatus: s2OverallSkewStatus
            }
        };
    }

    // Signal Sending Logic
    const RESEND_PROFIT_INCREASE_THRESHOLD = 0.5; // Only resend same type if NP increased by this much
    if (newOpportunityType) {
        if (newOpportunityType !== lastSentSignalDetails.type ||
            (newOpportunityType === lastSentSignalDetails.type && identifiedNetProfitForNewSignal > (lastSentSignalDetails.np + RESEND_PROFIT_INCREASE_THRESHOLD))) {
            if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                try {
                    internalWsClient.send(JSON.stringify(newArbSignalPayload));
                    console.log(`[Arbitrage-Module] SENT New Signal: ${newArbSignalPayload.as.type}, NP: $${newArbSignalPayload.as.np.toFixed(4)}, Basis: $${newArbSignalPayload.as.basis.toFixed(4)}, Skew: ${newArbSignalPayload.as.skewStatus}, SpotR: ${newArbSignalPayload.as.sSR}, FutR: ${newArbSignalPayload.as.fSR}`);
                    lastSentArbitrageType = newOpportunityType; // Keep for clearance logic compatibility
                    lastSentSignalDetails = { type: newOpportunityType, np: identifiedNetProfitForNewSignal };
                } catch (error) {
                    console.error(`[Arbitrage-Module] Error sending new opportunity (${newOpportunityType}):`, error.message);
                }
            } else {
                console.warn(`[Arbitrage-Module] Detected ${newOpportunityType} (NP: $${identifiedNetProfitForNewSignal.toFixed(4)}) but internal client NOT OPEN.`);
            }
        }
    } else { // No new opportunity meeting entry threshold
        if (lastSentArbitrageType !== null) { // If an opportunity was previously active
            let profitOfLastActiveTypeOriginal = -Infinity;
            const originalS1Profit = ((latestSpotData.bestBid - latestFuturesData.bestAsk) - naturalBasisSpotOverFutures_USD) - TOTAL_FEES_PER_UNIT_USD;
            const originalS2Profit = ((latestFuturesData.bestBid - latestSpotData.bestAsk) + naturalBasisSpotOverFutures_USD) - TOTAL_FEES_PER_UNIT_USD;

            if (lastSentArbitrageType === "scenario1_SellSpotBuyFutures_Deviation") {
                profitOfLastActiveTypeOriginal = originalS1Profit;
            } else if (lastSentArbitrageType === "scenario2_SellFuturesBuySpot_Deviation") {
                profitOfLastActiveTypeOriginal = originalS2Profit;
            }

            if (profitOfLastActiveTypeOriginal < CLEARANCE_PROFIT_FLOOR_USD) {
                if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                    try {
                        internalWsClient.send(JSON.stringify(clearSignalPayload));
                        console.warn(`[Arbitrage-Module] SENT Clear Signal. ${lastSentArbitrageType} original profit (${profitOfLastActiveTypeOriginal.toFixed(4)}) fell below clearance floor (${CLEARANCE_PROFIT_FLOOR_USD}).`);
                        lastSentArbitrageType = null;
                        lastSentSignalDetails = { type: null, np: 0 };
                    } catch (error) {
                        console.error(`[Arbitrage-Module] Error sending clear signal (below clearance):`, error.message);
                    }
                } else {
                    console.warn(`[Arbitrage-Module] ${lastSentArbitrageType} profit (${profitOfLastActiveTypeOriginal.toFixed(4)}) dropped below clearance, internal client NOT OPEN.`);
                    lastSentArbitrageType = null;
                    lastSentSignalDetails = { type: null, np: 0 };
                }
            }
        }
    }
}


// --- Spot BookTicker Connection ---
function connectToSpotBookTicker() {
    if (spotWsClient && (spotWsClient.readyState === WebSocket.OPEN || spotWsClient.readyState === WebSocket.CONNECTING)) { return; }
    spotWsClient = new WebSocket(SPOT_BOOKTICKER_URL);
    console.log(`[Arbitrage-Module] PID: ${process.pid} --- Connecting to Spot BookTicker...`);
    spotWsClient.on('open', () => {
        console.log(`[Arbitrage-Module] PID: ${process.pid} --- Connected to Spot BookTicker.`);
        if (spotPingIntervalId) clearInterval(spotPingIntervalId);
        spotPingIntervalId = setInterval(() => { if (spotWsClient && spotWsClient.readyState === WebSocket.OPEN) try { spotWsClient.ping(); } catch (e) { console.error(`Spot Ping Error: ${e.message}`);}}, ARB_PING_INTERVAL_MS);
    });
    spotWsClient.on('message', (data) => {
        try {
            const t = JSON.parse(data.toString());
            if (t && t.b !== undefined && t.a !== undefined && t.B !== undefined && t.A !== undefined) {
                const bP = parseFloat(t.b), aP = parseFloat(t.a), bQ = parseFloat(t.B), aQ = parseFloat(t.A);
                if (isNaN(bP) || isNaN(aP) || isNaN(bQ) || isNaN(aQ)) {
                    latestSpotData = { bestBid: null, bestAsk: null, bestBidQty: null, bestAskQty: null, timestamp: null }; return;
                }
                latestSpotData = { bestBid: bP, bestAsk: aP, bestBidQty: bQ, bestAskQty: aQ, timestamp: t.E || Date.now() };
            }
        } catch (e) { console.error(`Spot ticker error: ${e.message}`, e.stack); latestSpotData = { bestBid: null, bestAsk: null, bestBidQty: null, bestAskQty: null, timestamp: null }; }
    });
    spotWsClient.on('pong', () => {});
    spotWsClient.on('error', (err) => { console.error(`Spot WebSocket error: ${err.message}`); });
    spotWsClient.on('close', (code, reason) => {
        console.warn(`Spot WebSocket closed. Code: ${code}, Reason: ${reason ? reason.toString() : 'N/A'}. Reconnecting...`);
        if (spotPingIntervalId) clearInterval(spotPingIntervalId); spotPingIntervalId = null;
        spotWsClient = null; latestSpotData = { bestBid: null, bestAsk: null, bestBidQty: null, bestAskQty: null, timestamp: null };
        setTimeout(connectToSpotBookTicker, ARB_RECONNECT_INTERVAL_MS);
    });
}

// --- Futures BookTicker Connection ---
function connectToFuturesBookTicker() {
    if (futuresWsClient && (futuresWsClient.readyState === WebSocket.OPEN || futuresWsClient.readyState === WebSocket.CONNECTING)) { return; }
    futuresWsClient = new WebSocket(FUTURES_BOOKTICKER_URL);
    console.log(`[Arbitrage-Module] PID: ${process.pid} --- Connecting to Futures BookTicker...`);
    futuresWsClient.on('open', () => {
        console.log(`[Arbitrage-Module] PID: ${process.pid} --- Connected to Futures BookTicker.`);
        if (futuresPingIntervalId) clearInterval(futuresPingIntervalId);
        futuresPingIntervalId = setInterval(() => { if (futuresWsClient && futuresWsClient.readyState === WebSocket.OPEN) try { futuresWsClient.ping(); } catch (e) { console.error(`Futures Ping Error: ${e.message}`); } }, ARB_PING_INTERVAL_MS);
    });
    futuresWsClient.on('message', (data) => {
        try {
            const mS = data.toString(); if (mS.includes('"e":"pong"')) return;
            const t = JSON.parse(mS);
            if (t && t.b !== undefined && t.a !== undefined && t.B !== undefined && t.A !== undefined) {
                const bP = parseFloat(t.b), aP = parseFloat(t.a), bQ = parseFloat(t.B), aQ = parseFloat(t.A);
                if (isNaN(bP) || isNaN(aP) || isNaN(bQ) || isNaN(aQ)) {
                    latestFuturesData = { bestBid: null, bestAsk: null, bestBidQty: null, bestAskQty: null, timestamp: null }; return;
                }
                latestFuturesData = { bestBid: bP, bestAsk: aP, bestBidQty: bQ, bestAskQty: aQ, timestamp: t.E || Date.now() };
            }
        } catch (e) { console.error(`Futures ticker error: ${e.message}`, e.stack); latestFuturesData = { bestBid: null, bestAsk: null, bestBidQty: null, bestAskQty: null, timestamp: null }; }
    });
    futuresWsClient.on('pong', () => {});
    futuresWsClient.on('error', (err) => { console.error(`Futures WebSocket error: ${err.message}`); });
    futuresWsClient.on('close', (code, reason) => {
        console.warn(`Futures WebSocket closed. Code: ${code}, Reason: ${reason ? reason.toString() : 'N/A'}. Reconnecting...`);
        if (futuresPingIntervalId) clearInterval(futuresPingIntervalId); futuresPingIntervalId = null;
        futuresWsClient = null; latestFuturesData = { bestBid: null, bestAsk: null, bestBidQty: null, bestAskQty: null, timestamp: null };
        setTimeout(connectToFuturesBookTicker, ARB_RECONNECT_INTERVAL_MS);
    });
}

// --- Mark Price Stream Connection ---
function connectToMarkPriceStream() {
    if (markPriceWsClient && (markPriceWsClient.readyState === WebSocket.OPEN || markPriceWsClient.readyState === WebSocket.CONNECTING)) { return; }
    markPriceWsClient = new WebSocket(MARK_PRICE_STREAM_URL);
    console.log(`[Arbitrage-Module] PID: ${process.pid} --- Connecting to Mark Price Stream...`);
    markPriceWsClient.on('open', () => {
        console.log(`[Arbitrage-Module] PID: ${process.pid} --- Connected to Mark Price Stream.`);
        if (markPricePingIntervalId) clearInterval(markPricePingIntervalId);
        markPricePingIntervalId = setInterval(() => { if (markPriceWsClient && markPriceWsClient.readyState === WebSocket.OPEN) try { markPriceWsClient.ping(); } catch (e) { console.error(`Mark Price Ping Error: ${e.message}`); } }, ARB_PING_INTERVAL_MS);
    });
    markPriceWsClient.on('message', (data) => {
        try {
            const t = JSON.parse(data.toString());
            if (t && t.p !== undefined && t.i !== undefined) {
                const mP = parseFloat(t.p), iP = parseFloat(t.i);
                if (isNaN(mP) || isNaN(iP)) { latestRawPremiumIndexPercentage = null; latestSpotIndexPrice = null; return; }
                if (iP === 0) { console.warn("Index Price is zero in Mark Price stream."); latestRawPremiumIndexPercentage = null; latestSpotIndexPrice = iP; return; }
                latestSpotIndexPrice = iP; latestRawPremiumIndexPercentage = (mP - iP) / iP;
            }
        } catch (e) { console.error(`Mark Price stream error: ${e.message}`, e.stack); latestRawPremiumIndexPercentage = null; latestSpotIndexPrice = null; }
    });
    markPriceWsClient.on('pong', () => {});
    markPriceWsClient.on('error', (err) => { console.error(`Mark Price WebSocket error: ${err.message}`); });
    markPriceWsClient.on('close', (code, reason) => {
        console.warn(`Mark Price WebSocket closed. Code: ${code}, Reason: ${reason ? reason.toString() : 'N/A'}. Reconnecting...`);
        if (markPricePingIntervalId) clearInterval(markPricePingIntervalId); markPricePingIntervalId = null;
        markPriceWsClient = null; latestRawPremiumIndexPercentage = null; latestSpotIndexPrice = null;
        setTimeout(connectToMarkPriceStream, ARB_RECONNECT_INTERVAL_MS);
    });
}

// --- Start the connections and intervals ---
console.log(`[Arbitrage-Module] PID: ${process.pid} --- Initializing Arbitrage Module...`);
connectToInternalReceiver();
connectToSpotBookTicker();
connectToFuturesBookTicker();
connectToMarkPriceStream();

if (arbitrageCheckIntervalId) { clearInterval(arbitrageCheckIntervalId); }
arbitrageCheckIntervalId = setInterval(performArbitrageCheckAndSignal, ARBITRAGE_CHECK_INTERVAL_MS);
console.log(`[Arbitrage-Module] PID: ${process.pid} --- Arbitrage check interval started, checking every ${ARBITRAGE_CHECK_INTERVAL_MS}ms.`);
