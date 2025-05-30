// arbitrage_module.js 

const WebSocket = require('ws');

// --- Global Error Handlers (Error logs retained) ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[Arbitrage-Module] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`);
    console.error(err.stack || err);
    console.error(`[Arbitrage-Module] Exception origin: ${origin}`);
    console.error(`[Arbitrage-Module] PID: ${process.pid} --- Exiting due to uncaught exception...`);
    setTimeout(() => {
        if (internalWsClient && typeof internalWsClient.terminate === 'function') {
            try { internalWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (spotWsClient && typeof spotWsClient.terminate === 'function') {
            try { spotWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (futuresWsClient && typeof futuresWsClient.terminate === 'function') {
            try { futuresWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (spotPingIntervalId) { try { clearInterval(spotPingIntervalId); } catch(e) { /* ignore */ } }
        if (futuresPingIntervalId) { try { clearInterval(futuresPingIntervalId); } catch(e) { /* ignore */ } }
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
        if (internalWsClient && typeof internalWsClient.terminate === 'function') {
            try { internalWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (spotWsClient && typeof spotWsClient.terminate === 'function') {
            try { spotWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (futuresWsClient && typeof futuresWsClient.terminate === 'function') {
            try { futuresWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (spotPingIntervalId) { try { clearInterval(spotPingIntervalId); } catch(e) { /* ignore */ } }
        if (futuresPingIntervalId) { try { clearInterval(futuresPingIntervalId); } catch(e) { /* ignore */ } }
        if (arbitrageCheckIntervalId) { try { clearInterval(arbitrageCheckIntervalId); } catch(e) { /* ignore */ } }
        process.exit(1);
    }, 1000).unref();
});

// --- Arbitrage Configuration ---
const internalReceiverUrl = 'ws://localhost:8082';
const SPOT_BOOKTICKER_URL = 'wss://stream.binance.com:9443/ws/btcusdt@bookTicker';
const FUTURES_BOOKTICKER_URL = 'wss://fstream.binance.com/ws/btcusdt@bookTicker';
const ARB_RECONNECT_INTERVAL_MS = 5000;
const ARB_PING_INTERVAL_MS = 3 * 60 * 1000;
const DESIRED_PROFIT_THRESHOLD_USD = 5.0;
const TOTAL_FEES_PER_UNIT_USD = 0.2;
const ARBITRAGE_CHECK_INTERVAL_MS = 10;
const NATURAL_BASIS_SPOT_OVER_FUTURES = 50.0;

// --- Arbitrage State Variables ---
let internalWsClient = null;
let spotWsClient = null;
let futuresWsClient = null;
let spotPingIntervalId = null;
let futuresPingIntervalId = null;
let arbitrageCheckIntervalId = null;

let latestSpotData = { bestBid: null, bestAsk: null, timestamp: null };
let latestFuturesData = { bestBid: null, bestAsk: null, timestamp: null };
let lastSentArbitrageType = null;

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    // Log connection attempt, useful for diagnosing repeated failures
    console.log(`[Arbitrage-Module] PID: ${process.pid} --- Connecting to internal receiver at ${internalReceiverUrl}...`);
    internalWsClient = new WebSocket(internalReceiverUrl);

    internalWsClient.on('open', () => {
        // "Connected to..." log removed for brevity. Successful connection is implied by lack of errors.
    });

    internalWsClient.on('error', (err) => {
        console.error(`[Arbitrage-Module] PID: ${process.pid} --- Internal receiver WebSocket error:`, err.message);
    });

    internalWsClient.on('close', (code, reason) => {
        // Log disconnection, important for diagnostics
        console.log(`[Arbitrage-Module] PID: ${process.pid} --- Disconnected from internal receiver. Code: ${code}, Reason: ${reason ? reason.toString() : 'N/A'}`);
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, ARB_RECONNECT_INTERVAL_MS);
    });
}

// --- Arbitrage Logic Core (Called by Interval) ---
function performArbitrageCheckAndSignal() {
    if (!latestSpotData.bestBid || !latestSpotData.bestAsk ||
        !latestFuturesData.bestBid || !latestFuturesData.bestAsk) {
        if (lastSentArbitrageType !== null) {
            // This log is important as it indicates a change in state or data quality
            console.log(`[Arbitrage-Module] PID: ${process.pid} --- Opportunity (${lastSentArbitrageType}) cleared due to incomplete market data (prices).`);
            lastSentArbitrageType = null;
        }
        return;
    }

    let currentOpportunityType = null;
    let arbSignalPayload = null;
    let identifiedNetProfit = 0;
    let spotTradePrice = null;
    let futuresTradePrice = null;

    const currentBasisSpotOverFutures = latestSpotData.bestBid - latestFuturesData.bestAsk;
    const deviationProfitSpotSellFuturesBuy = (currentBasisSpotOverFutures - NATURAL_BASIS_SPOT_OVER_FUTURES) - TOTAL_FEES_PER_UNIT_USD;

    const currentBasisFuturesOverSpot = latestFuturesData.bestBid - latestSpotData.bestAsk;
    const deviationProfitFuturesSellSpotBuy = (currentBasisFuturesOverSpot + NATURAL_BASIS_SPOT_OVER_FUTURES) - TOTAL_FEES_PER_UNIT_USD;

    if (deviationProfitSpotSellFuturesBuy > DESIRED_PROFIT_THRESHOLD_USD) {
        currentOpportunityType = "scenario1_SellSpotBuyFutures_Deviation";
        identifiedNetProfit = deviationProfitSpotSellFuturesBuy;
        spotTradePrice = latestSpotData.bestBid;
        futuresTradePrice = latestFuturesData.bestAsk;
        arbSignalPayload = {
            as: { sell: "spot", buy: "futures", np: parseFloat(identifiedNetProfit.toFixed(4)), spt: spotTradePrice, fpt: futuresTradePrice }
        };
    } else if (deviationProfitFuturesSellSpotBuy > DESIRED_PROFIT_THRESHOLD_USD) {
        currentOpportunityType = "scenario2_SellFuturesBuySpot_Deviation";
        identifiedNetProfit = deviationProfitFuturesSellSpotBuy;
        spotTradePrice = latestSpotData.bestAsk;
        futuresTradePrice = latestFuturesData.bestBid;
        arbSignalPayload = {
            as: { sell: "futures", buy: "spot", np: parseFloat(identifiedNetProfit.toFixed(4)), spt: spotTradePrice, fpt: futuresTradePrice }
        };
    }

    if (currentOpportunityType) {
        if (currentOpportunityType !== lastSentArbitrageType) {
            if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                try {
                    const messageString = JSON.stringify(arbSignalPayload);
                    internalWsClient.send(messageString);
                    // This log is CRITICAL - indicates a signal was sent.
                    console.log(`[Arbitrage-Module] PID: ${process.pid} --- SENT Signal. Type: ${currentOpportunityType}, Sell: ${arbSignalPayload.as.sell} (SpotP: ${arbSignalPayload.as.spt}, FuturesP: ${arbSignalPayload.as.fpt}), Buy: ${arbSignalPayload.as.buy}, Deviation Profit: $${identifiedNetProfit.toFixed(4)}`);
                    lastSentArbitrageType = currentOpportunityType;
                } catch (error) {
                    console.error(`[Arbitrage-Module] PID: ${process.pid} --- Error stringifying or sending arbitrage opportunity:`, error.message, error.stack);
                }
            } else {
                // This warning is important.
                console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Detected ${currentOpportunityType} opportunity but internal client NOT OPEN. Deviation Profit: $${identifiedNetProfit.toFixed(4)}. Signal not sent.`);
            }
        }
        // The previously commented-out debug log for "opportunity persists" is naturally removed.
    } else {
        if (lastSentArbitrageType !== null) {
            // This log is important as it indicates a change in state.
            console.log(`[Arbitrage-Module] PID: ${process.pid} --- Previously signaled arbitrage opportunity (${lastSentArbitrageType}) has now disappeared or fallen below threshold.`);
            lastSentArbitrageType = null;
        }
    }
}

// --- Spot BookTicker Connection ---
function connectToSpotBookTicker() {
    if (spotWsClient && (spotWsClient.readyState === WebSocket.OPEN || spotWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    console.log(`[Arbitrage-Module] PID: ${process.pid} --- Connecting to Spot BookTicker...`);
    spotWsClient = new WebSocket(SPOT_BOOKTICKER_URL);

    spotWsClient.on('open', function open() {
        // "Connected to..." log removed for brevity.
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

    spotWsClient.on('pong', () => { }); // No logging for pongs by default

    spotWsClient.on('error', function error(err) {
        console.error(`[Arbitrage-Module] PID: ${process.pid} --- Spot BookTicker WebSocket error:`, err.message);
    });

    spotWsClient.on('close', function close(code, reason) {
        console.log(`[Arbitrage-Module] PID: ${process.pid} --- Spot BookTicker WebSocket closed. Code: ${code}, Reason: ${reason ? reason.toString() : 'N/A'}`);
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
    console.log(`[Arbitrage-Module] PID: ${process.pid} --- Connecting to Futures BookTicker...`);
    futuresWsClient = new WebSocket(FUTURES_BOOKTICKER_URL);

    futuresWsClient.on('open', function open() {
        // "Connected to..." log removed for brevity.
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
                return; // No logging for pong messages
            }
            const tickerData = JSON.parse(messageString);

            if (tickerData && typeof tickerData.b !== 'undefined' && typeof tickerData.a !== 'undefined') {
                const newFuturesBestBid = parseFloat(tickerData.b);
                const newFuturesBestAsk = parseFloat(tickerData.a);

                if (isNaN(newFuturesBestBid) || isNaN(newFuturesBestAsk)) {
                    console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Invalid price in Futures BookTicker data: Bid=${tickerData.b}, Ask=${tickerData.a}`);
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

    futuresWsClient.on('pong', () => { }); // No logging for pongs by default

    futuresWsClient.on('error', function error(err) {
        console.error(`[Arbitrage-Module] PID: ${process.pid} --- Futures BookTicker WebSocket error:`, err.message);
    });

    futuresWsClient.on('close', function close(code, reason) {
        console.log(`[Arbitrage-Module] PID: ${process.pid} --- Futures BookTicker WebSocket closed. Code: ${code}, Reason: ${reason ? reason.toString() : 'N/A'}`);
        if (futuresPingIntervalId) { clearInterval(futuresPingIntervalId); futuresPingIntervalId = null; }
        futuresWsClient = null;
        latestFuturesData = { bestBid: null, bestAsk: null, timestamp: null };
        setTimeout(connectToFuturesBookTicker, ARB_RECONNECT_INTERVAL_MS);
    });
}

// --- Start the connections and intervals (Startup logs retained) ---
console.log(`[Arbitrage-Module] PID: ${process.pid} --- Arbitrage Module starting...`);

connectToInternalReceiver();
connectToSpotBookTicker();
connectToFuturesBookTicker();

if (arbitrageCheckIntervalId) {
    clearInterval(arbitrageCheckIntervalId);
}
arbitrageCheckIntervalId = setInterval(performArbitrageCheckAndSignal, ARBITRAGE_CHECK_INTERVAL_MS);
console.log(`[Arbitrage-Module] PID: ${process.pid} --- Arbitrage check interval started, checking every ${ARBITRAGE_CHECK_INTERVAL_MS}ms.`);
