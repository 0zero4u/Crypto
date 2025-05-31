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
const CLEARANCE_PROFIT_FLOOR_USD = 1.5;     // Floor to keep an existing signal active (MUST BE < DESIRED_PROFIT_THRESHOLD_USD)
const TOTAL_FEES_PER_UNIT_USD = 0.2;
const ARBITRAGE_CHECK_INTERVAL_MS = 100; // Increased from 25ms

// --- Book Skew Configuration ---
const FAVORABLE_SKEW_RATIO_SELL_SIDE_HEAVY = 1.5; // For selling asset X, want X_AskQty / X_BidQty >= this
const FAVORABLE_SKEW_RATIO_BUY_SIDE_HEAVY = 1.13;  // For buying asset X, want X_BidQty / X_AskQty >= this
const MIN_TOTAL_TOP_LEVEL_QTY_FOR_SKEW_CHECK = 0.1; // Min (BidQ+AskQ) for skew check to be valid (e.g. 0.1 BTC)
const PENALTY_FOR_UNFAVORABLE_SKEW_USD = 6.0; // Subtract this from profit if combined skew is not ideal.
                                             // Set to a high value (e.g., 9999) for strict veto, 0 to disable penalty (rely on boolean check).

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

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    internalWsClient = new WebSocket(internalReceiverUrl);

    internalWsClient.on('open', () => { console.log(`[Arbitrage-Module] PID: ${process.pid} --- Connected to internal receiver.`); });

    internalWsClient.on('error', (err) => {
        console.error(`[Arbitrage-Module] PID: ${process.pid} --- Internal receiver WebSocket error:`, err.message);
    });

    internalWsClient.on('close', (code, reason) => {
        console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Disconnected from internal receiver. Code: ${code}, Reason: ${reason ? reason.toString() : 'N/A'}. Reconnecting...`);
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, ARB_RECONNECT_INTERVAL_MS);
    });
}

// --- Helper function for Book Skew Check ---
function getBookSkewFavorability(bestBidQty, bestAskQty, action, marketName = "Market") {
    if (bestBidQty === null || bestAskQty === null) { // Guard against null quantities
        console.log(`[BookSkew-${marketName}] Skipped skew check for ${action}: Quantity data missing.`);
        return { favorable: false, reason: "missing_qty_data", ratio: NaN };
    }
    const totalQty = bestBidQty + bestAskQty;
    if (totalQty < MIN_TOTAL_TOP_LEVEL_QTY_FOR_SKEW_CHECK) {
        // console.log(`[BookSkew-${marketName}] Skipped skew check for ${action}: Not enough liquidity (${totalQty.toFixed(4)}). Total Qty: ${totalQty}, Min Req: ${MIN_TOTAL_TOP_LEVEL_QTY_FOR_SKEW_CHECK}. Considered UNFAVORABLE.`);
        return { favorable: false, reason: "low_liquidity", ratio: NaN };
    }

    let ratio;
    let isFavorable;

    if (action === "SELL") {
        if (bestBidQty <= 0.00000001) { // Effectively zero or negligible bid quantity
            isFavorable = bestAskQty > 0.00000001; // Favorable if any asks exist when no bids
            ratio = Infinity;
        } else {
            ratio = bestAskQty / bestBidQty;
            isFavorable = ratio >= FAVORABLE_SKEW_RATIO_SELL_SIDE_HEAVY;
        }
        // if (!isFavorable) console.log(`[BookSkew-${marketName}] Skew NOT favorable for SELLING. Ask/Bid Ratio: ${bestBidQty <= 0.00000001 ? "Inf" : ratio.toFixed(2)} (Want >= ${FAVORABLE_SKEW_RATIO_SELL_SIDE_HEAVY}) BQ:${bestBidQty} AQ:${bestAskQty}`);
    } else if (action === "BUY") {
        if (bestAskQty <= 0.00000001) { // Effectively zero or negligible ask quantity
            isFavorable = bestBidQty > 0.00000001; // Favorable if any bids exist when no asks
            ratio = Infinity; // Effectively Bid/Ask ratio
        } else {
            ratio = bestBidQty / bestAskQty; // For buying, we look at BidQty / AskQty
            isFavorable = ratio >= FAVORABLE_SKEW_RATIO_BUY_SIDE_HEAVY;
        }
        // if (!isFavorable) console.log(`[BookSkew-${marketName}] Skew NOT favorable for BUYING. Bid/Ask Ratio: ${bestAskQty <= 0.00000001 ? "Inf" : ratio.toFixed(2)} (Want >= ${FAVORABLE_SKEW_RATIO_BUY_SIDE_HEAVY}) BQ:${bestBidQty} AQ:${bestAskQty}`);
    } else {
        console.error(`[BookSkew-${marketName}] Invalid action: ${action}`);
        return { favorable: false, reason: "invalid_action", ratio: NaN };
    }
    return { favorable: isFavorable, reason: isFavorable ? "ok" : "unfavorable_ratio", ratio: ratio };
}

// --- Arbitrage Logic Core (Called by Interval) ---
function performArbitrageCheckAndSignal() {
    const clearSignalPayload = { as: null };

    if (!latestSpotData.bestBid || !latestSpotData.bestAsk || latestSpotData.bestBidQty === null || latestSpotData.bestAskQty === null ||
        !latestFuturesData.bestBid || !latestFuturesData.bestAsk || latestFuturesData.bestBidQty === null || latestFuturesData.bestAskQty === null ||
        latestRawPremiumIndexPercentage === null || latestSpotIndexPrice === null) {
        if (lastSentArbitrageType !== null) {
            console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Opportunity (${lastSentArbitrageType}) cleared due to incomplete market/basis/qty data.`);
            if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                try { internalWsClient.send(JSON.stringify(clearSignalPayload)); } catch (error) { console.error(`[Arbitrage-Module] PID: ${process.pid} --- Error sending clear signal (incomplete data):`, error.message, error.stack); }
            } else { console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Incomplete data for ${lastSentArbitrageType}, but internal client NOT OPEN. Clear signal not sent.`); }
            lastSentArbitrageType = null;
        }
        return;
    }

    let newOpportunityType = null;
    let newArbSignalPayload = null;
    let identifiedNetProfitForNewSignal = 0;

    const naturalSpotPremiumPercentage_dynamic = -latestRawPremiumIndexPercentage;
    const currentApproxSpotPriceForBasis = latestSpotIndexPrice;
    const naturalBasisSpotOverFutures_USD = naturalSpotPremiumPercentage_dynamic * currentApproxSpotPriceForBasis;

    const currentSpotBid_minus_FuturesAsk_S1 = latestSpotData.bestBid - latestFuturesData.bestAsk;
    let deviationProfitSpotSellFuturesBuy = (currentSpotBid_minus_FuturesAsk_S1 - naturalBasisSpotOverFutures_USD) - TOTAL_FEES_PER_UNIT_USD;

    const currentFuturesBid_minus_SpotAsk_S2 = latestFuturesData.bestBid - latestSpotData.bestAsk;
    let deviationProfitFuturesSellSpotBuy = (currentFuturesBid_minus_SpotAsk_S2 + naturalBasisSpotOverFutures_USD) - TOTAL_FEES_PER_UNIT_USD;

    let s1BookPressureOk = false;
    let s2BookPressureOk = false;
    let s1SpotSkewDetails = { favorable: false, reason: "not_checked", ratio: NaN };
    let s1FuturesSkewDetails = { favorable: false, reason: "not_checked", ratio: NaN };
    let s2SpotSkewDetails = { favorable: false, reason: "not_checked", ratio: NaN };
    let s2FuturesSkewDetails = { favorable: false, reason: "not_checked", ratio: NaN };


    if (deviationProfitSpotSellFuturesBuy > (DESIRED_PROFIT_THRESHOLD_USD - PENALTY_FOR_UNFAVORABLE_SKEW_USD - 0.01) ) { // Check if profit is high enough to warrant skew check
        s1SpotSkewDetails = getBookSkewFavorability(latestSpotData.bestBidQty, latestSpotData.bestAskQty, "SELL", "SpotS1");
        s1FuturesSkewDetails = getBookSkewFavorability(latestFuturesData.bestBidQty, latestFuturesData.bestAskQty, "BUY", "FuturesS1");
        s1BookPressureOk = s1SpotSkewDetails.favorable && s1FuturesSkewDetails.favorable;

        if (!s1BookPressureOk && PENALTY_FOR_UNFAVORABLE_SKEW_USD > 0) {
            // console.log(`[Arbitrage-Module] S1: Applying skew penalty. Original NP: ${deviationProfitSpotSellFuturesBuy.toFixed(4)}`);
            deviationProfitSpotSellFuturesBuy -= PENALTY_FOR_UNFAVORABLE_SKEW_USD;
        }
    }

    if (deviationProfitFuturesSellSpotBuy > (DESIRED_PROFIT_THRESHOLD_USD - PENALTY_FOR_UNFAVORABLE_SKEW_USD - 0.01)) {
        s2SpotSkewDetails = getBookSkewFavorability(latestSpotData.bestBidQty, latestSpotData.bestAskQty, "BUY", "SpotS2");
        s2FuturesSkewDetails = getBookSkewFavorability(latestFuturesData.bestBidQty, latestFuturesData.bestAskQty, "SELL", "FuturesS2");
        s2BookPressureOk = s2SpotSkewDetails.favorable && s2FuturesSkewDetails.favorable;

        if (!s2BookPressureOk && PENALTY_FOR_UNFAVORABLE_SKEW_USD > 0) {
            // console.log(`[Arbitrage-Module] S2: Applying skew penalty. Original NP: ${deviationProfitFuturesSellSpotBuy.toFixed(4)}`);
            deviationProfitFuturesSellSpotBuy -= PENALTY_FOR_UNFAVORABLE_SKEW_USD;
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
                skewOk: s1BookPressureOk
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
                skewOk: s2BookPressureOk
            }
        };
    }

    if (newOpportunityType) {
        if (newOpportunityType !== lastSentArbitrageType || identifiedNetProfitForNewSignal > ( (lastSentArbitrageType && newArbSignalPayload.as.np > (JSON.parse(JSON.stringify(internalWsClient._sender._bufferedMessages.find(m=>JSON.parse(m).as && JSON.parse(m).as.type === lastSentArbitrageType)?.as?.np || 0)) ) ) ? (JSON.parse(JSON.stringify(internalWsClient._sender._bufferedMessages.find(m=>JSON.parse(m).as && JSON.parse(m).as.type === lastSentArbitrageType)?.as?.np || 0)) + 0.5) : DESIRED_PROFIT_THRESHOLD_USD ) ) { // Send if new type, or same type but significantly more profitable
            if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                try {
                    internalWsClient.send(JSON.stringify(newArbSignalPayload));
                    console.log(`[Arbitrage-Module] PID: ${process.pid} --- SENT New Signal: ${newArbSignalPayload.as.type}, NP: $${newArbSignalPayload.as.np.toFixed(4)}, Basis: $${newArbSignalPayload.as.basis.toFixed(4)}, SkewInitiallyOK: ${newArbSignalPayload.as.skewOk}, SpotRatio: ${newArbSignalPayload.as.sSR}, FutRatio: ${newArbSignalPayload.as.fSR}`);
                    lastSentArbitrageType = newOpportunityType;
                } catch (error) {
                    console.error(`[Arbitrage-Module] PID: ${process.pid} --- Error sending new opportunity (${newOpportunityType}):`, error.message, error.stack);
                }
            } else {
                console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Detected ${newOpportunityType} (NP: $${identifiedNetProfitForNewSignal.toFixed(4)}) but internal client NOT OPEN. Signal not sent.`);
            }
        }
    } else {
        if (lastSentArbitrageType !== null) {
            let profitOfLastActiveTypeOriginal = -Infinity;
            const originalS1Profit = ( (latestSpotData.bestBid - latestFuturesData.bestAsk) - naturalBasisSpotOverFutures_USD) - TOTAL_FEES_PER_UNIT_USD;
            const originalS2Profit = ( (latestFuturesData.bestBid - latestSpotData.bestAsk) + naturalBasisSpotOverFutures_USD) - TOTAL_FEES_PER_UNIT_USD;

            if (lastSentArbitrageType === "scenario1_SellSpotBuyFutures_Deviation") {
                profitOfLastActiveTypeOriginal = originalS1Profit;
            } else if (lastSentArbitrageType === "scenario2_SellFuturesBuySpot_Deviation") {
                profitOfLastActiveTypeOriginal = originalS2Profit;
            }

            if (profitOfLastActiveTypeOriginal < CLEARANCE_PROFIT_FLOOR_USD) {
                if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                    try {
                        internalWsClient.send(JSON.stringify(clearSignalPayload));
                        console.warn(`[Arbitrage-Module] PID: ${process.pid} --- SENT Clear Signal. ${lastSentArbitrageType} original profit (${profitOfLastActiveTypeOriginal.toFixed(4)}) fell below clearance floor (${CLEARANCE_PROFIT_FLOOR_USD}).`);
                        lastSentArbitrageType = null;
                    } catch (error) {
                        console.error(`[Arbitrage-Module] PID: ${process.pid} --- Error sending clear signal (below clearance):`, error.message, error.stack);
                    }
                } else {
                    console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Previously signaled ${lastSentArbitrageType} (profit ${profitOfLastActiveTypeOriginal.toFixed(4)}) dropped below clearance, but internal client NOT OPEN. Clear signal not sent.`);
                    lastSentArbitrageType = null;
                }
            }
        }
    }
}

// --- Spot BookTicker Connection ---
function connectToSpotBookTicker() {
    if (spotWsClient && (spotWsClient.readyState === WebSocket.OPEN || spotWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    spotWsClient = new WebSocket(SPOT_BOOKTICKER_URL);
    console.log(`[Arbitrage-Module] PID: ${process.pid} --- Connecting to Spot BookTicker...`);

    spotWsClient.on('open', function open() {
        console.log(`[Arbitrage-Module] PID: ${process.pid} --- Connected to Spot BookTicker.`);
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
            if (tickerData && typeof tickerData.b !== 'undefined' && typeof tickerData.a !== 'undefined' &&
                typeof tickerData.B !== 'undefined' && typeof tickerData.A !== 'undefined') {
                const newSpotBestBid = parseFloat(tickerData.b);
                const newSpotBestAsk = parseFloat(tickerData.a);
                const newSpotBestBidQty = parseFloat(tickerData.B);
                const newSpotBestAskQty = parseFloat(tickerData.A);

                if (isNaN(newSpotBestBid) || isNaN(newSpotBestAsk) || isNaN(newSpotBestBidQty) || isNaN(newSpotBestAskQty)) {
                    // console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Invalid price/qty in Spot BookTicker: Bid=${tickerData.b}, Ask=${tickerData.a}, BQty=${tickerData.B}, AQty=${tickerData.A}`);
                    latestSpotData = { bestBid: null, bestAsk: null, bestBidQty: null, bestAskQty: null, timestamp: null }; return;
                }
                latestSpotData.bestBid = newSpotBestBid;
                latestSpotData.bestAsk = newSpotBestAsk;
                latestSpotData.bestBidQty = newSpotBestBidQty;
                latestSpotData.bestAskQty = newSpotBestAskQty;
                latestSpotData.timestamp = tickerData.E || Date.now();
            }
        } catch (e) {
            console.error(`[Arbitrage-Module] PID: ${process.pid} --- CRITICAL ERROR in Spot BookTicker message handler:`, e.message, e.stack);
            latestSpotData = { bestBid: null, bestAsk: null, bestBidQty: null, bestAskQty: null, timestamp: null };
        }
    });

    spotWsClient.on('pong', () => { /* console.log('[Arbitrage-Module] Spot pong received'); */ });
    spotWsClient.on('error', function error(err) {
        console.error(`[Arbitrage-Module] PID: ${process.pid} --- Spot BookTicker WebSocket error:`, err.message);
    });
    spotWsClient.on('close', function close(code, reason) {
        console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Spot BookTicker WebSocket closed. Code: ${code}, Reason: ${reason ? reason.toString() : 'N/A'}. Reconnecting...`);
        if (spotPingIntervalId) { clearInterval(spotPingIntervalId); spotPingIntervalId = null; }
        spotWsClient = null;
        latestSpotData = { bestBid: null, bestAsk: null, bestBidQty: null, bestAskQty: null, timestamp: null };
        setTimeout(connectToSpotBookTicker, ARB_RECONNECT_INTERVAL_MS);
    });
}

// --- Futures BookTicker Connection ---
function connectToFuturesBookTicker() {
    if (futuresWsClient && (futuresWsClient.readyState === WebSocket.OPEN || futuresWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    futuresWsClient = new WebSocket(FUTURES_BOOKTICKER_URL);
    console.log(`[Arbitrage-Module] PID: ${process.pid} --- Connecting to Futures BookTicker...`);

    futuresWsClient.on('open', function open() {
        console.log(`[Arbitrage-Module] PID: ${process.pid} --- Connected to Futures BookTicker.`);
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
             if (tickerData && typeof tickerData.b !== 'undefined' && typeof tickerData.a !== 'undefined' &&
                typeof tickerData.B !== 'undefined' && typeof tickerData.A !== 'undefined') {
                const newFuturesBestBid = parseFloat(tickerData.b);
                const newFuturesBestAsk = parseFloat(tickerData.a);
                const newFuturesBestBidQty = parseFloat(tickerData.B);
                const newFuturesBestAskQty = parseFloat(tickerData.A);

                if (isNaN(newFuturesBestBid) || isNaN(newFuturesBestAsk) || isNaN(newFuturesBestBidQty) || isNaN(newFuturesBestAskQty)) {
                    // console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Invalid price/qty in Futures BookTicker: Bid=${tickerData.b}, Ask=${tickerData.a}, BQty=${tickerData.B}, AQty=${tickerData.A}`);
                    latestFuturesData = { bestBid: null, bestAsk: null, bestBidQty: null, bestAskQty: null, timestamp: null }; return;
                }
                latestFuturesData.bestBid = newFuturesBestBid;
                latestFuturesData.bestAsk = newFuturesBestAsk;
                latestFuturesData.bestBidQty = newFuturesBestBidQty;
                latestFuturesData.bestAskQty = newFuturesBestAskQty;
                latestFuturesData.timestamp = tickerData.E || Date.now();
            }
        } catch (e) {
            console.error(`[Arbitrage-Module] PID: ${process.pid} --- CRITICAL ERROR in Futures BookTicker message handler:`, e.message, e.stack);
            latestFuturesData = { bestBid: null, bestAsk: null, bestBidQty: null, bestAskQty: null, timestamp: null };
        }
    });

    futuresWsClient.on('pong', () => { /* console.log('[Arbitrage-Module] Futures pong received'); */ });
    futuresWsClient.on('error', function error(err) {
        console.error(`[Arbitrage-Module] PID: ${process.pid} --- Futures BookTicker WebSocket error:`, err.message);
    });
    futuresWsClient.on('close', function close(code, reason) {
        console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Futures BookTicker WebSocket closed. Code: ${code}, Reason: ${reason ? reason.toString() : 'N/A'}. Reconnecting...`);
        if (futuresPingIntervalId) { clearInterval(futuresPingIntervalId); futuresPingIntervalId = null; }
        futuresWsClient = null;
        latestFuturesData = { bestBid: null, bestAsk: null, bestBidQty: null, bestAskQty: null, timestamp: null };
        setTimeout(connectToFuturesBookTicker, ARB_RECONNECT_INTERVAL_MS);
    });
}

// --- Mark Price Stream Connection ---
function connectToMarkPriceStream() {
    if (markPriceWsClient && (markPriceWsClient.readyState === WebSocket.OPEN || markPriceWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    markPriceWsClient = new WebSocket(MARK_PRICE_STREAM_URL);
    console.log(`[Arbitrage-Module] PID: ${process.pid} --- Connecting to Mark Price Stream...`);

    markPriceWsClient.on('open', function open() {
        console.log(`[Arbitrage-Module] PID: ${process.pid} --- Connected to Mark Price Stream.`);
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
                    // console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Invalid numeric value in Mark Price stream: MarkP=${tickerData.p}, IndexP=${tickerData.i}`);
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

    markPriceWsClient.on('pong', () => { /* console.log('[Arbitrage-Module] Mark Price pong received'); */ });
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
console.log(`[Arbitrage-Module] PID: ${process.pid} --- Initializing Arbitrage Module...`);
connectToInternalReceiver();
connectToSpotBookTicker();
connectToFuturesBookTicker();
connectToMarkPriceStream();

if (arbitrageCheckIntervalId) {
    clearInterval(arbitrageCheckIntervalId);
}
arbitrageCheckIntervalId = setInterval(performArbitrageCheckAndSignal, ARBITRAGE_CHECK_INTERVAL_MS);
console.log(`[Arbitrage-Module] PID: ${process.pid} --- Arbitrage check interval started, checking every ${ARBITRAGE_CHECK_INTERVAL_MS}ms.`);
