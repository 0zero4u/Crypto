// arbitrage_module.js

const WebSocket = require('ws');

// --- Global Error Handlers ---
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
const internalReceiverUrl = 'ws://localhost:8082'; // Your internal client address
const SPOT_BOOKTICKER_URL = 'wss://stream.binance.com:9443/ws/btcusdt@bookTicker';
const FUTURES_BOOKTICKER_URL = 'wss://fstream.binance.com/ws/btcusdt@bookTicker'; // For USDT-M Perpetual Futures
const ARB_RECONNECT_INTERVAL_MS = 5000;
const ARB_PING_INTERVAL_MS = 3 * 60 * 1000;
const DESIRED_PROFIT_THRESHOLD_USD = 5.0; // Desired profit *from the deviation*
const TOTAL_FEES_PER_UNIT_USD = 0.2; // IMPORTANT: Review this fixed value.
const ARBITRAGE_CHECK_INTERVAL_MS = 10; // How often to check for arbitrage opportunities
const NATURAL_BASIS_SPOT_OVER_FUTURES = 50.0; // Example value

// --- Arbitrage State Variables ---
let internalWsClient = null;
let spotWsClient = null;
let futuresWsClient = null;
let spotPingIntervalId = null;
let futuresPingIntervalId = null;
let arbitrageCheckIntervalId = null;

let latestSpotData = { bestBid: null, bestAsk: null, timestamp: null };
let latestFuturesData = { bestBid: null, bestAsk: null, timestamp: null };
let lastSentArbitrageType = null; // Tracks the type of last sent signal

// --- Internal Receiver Connection (for Arbitrage Signals) ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    console.log(`[Arbitrage-Module] PID: ${process.pid} --- Connecting to internal receiver at ${internalReceiverUrl}...`);
    internalWsClient = new WebSocket(internalReceiverUrl);

    internalWsClient.on('open', () => {
        console.log(`[Arbitrage-Module] PID: ${process.pid} --- Connected to internal receiver.`);
        // No specific action needed on open for arbitrage signals beyond logging
    });

    internalWsClient.on('error', (err) => {
        console.error(`[Arbitrage-Module] PID: ${process.pid} --- Internal receiver WebSocket error:`, err.message);
    });

    internalWsClient.on('close', (code, reason) => {
        console.log(`[Arbitrage-Module] PID: ${process.pid} --- Disconnected from internal receiver. Code: ${code}, Reason: ${reason ? reason.toString() : 'N/A'}`);
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, ARB_RECONNECT_INTERVAL_MS); // Use ARB_RECONNECT for consistency
    });
}


// --- Arbitrage Logic Core (Called by Interval) ---
function performArbitrageCheckAndSignal() {
    if (!latestSpotData.bestBid || !latestSpotData.bestAsk || !latestSpotData.timestamp ||
        !latestFuturesData.bestBid || !latestFuturesData.bestAsk || !latestFuturesData.timestamp) {
        if (lastSentArbitrageType !== null) {
            console.log(`[Arbitrage-Module] PID: ${process.pid} --- Opportunity (${lastSentArbitrageType}) cleared due to incomplete market data.`);
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
            arb_signal: {
                type: "arbitrage_opportunity", // Add a type field
                sell_market: "spot",
                buy_market: "futures",
                net_profit_usd: parseFloat(identifiedNetProfit.toFixed(4)),
                spot_price_trade: spotTradePrice,
                futures_price_trade: futuresTradePrice,
                timestamp: Date.now() // Add a timestamp for the signal
            }
        };
    } else if (deviationProfitFuturesSellSpotBuy > DESIRED_PROFIT_THRESHOLD_USD) {
        currentOpportunityType = "scenario2_SellFuturesBuySpot_Deviation";
        identifiedNetProfit = deviationProfitFuturesSellSpotBuy;
        spotTradePrice = latestSpotData.bestAsk;
        futuresTradePrice = latestFuturesData.bestBid;
        arbSignalPayload = {
            arb_signal: {
                type: "arbitrage_opportunity", // Add a type field
                sell_market: "futures",
                buy_market: "spot",
                net_profit_usd: parseFloat(identifiedNetProfit.toFixed(4)),
                spot_price_trade: spotTradePrice,
                futures_price_trade: futuresTradePrice,
                timestamp: Date.now() // Add a timestamp for the signal
            }
        };
    }

    if (currentOpportunityType) {
        if (currentOpportunityType !== lastSentArbitrageType) {
            if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                try {
                    const messageString = JSON.stringify(arbSignalPayload);
                    internalWsClient.send(messageString);
                    console.log(`[Arbitrage-Module] PID: ${process.pid} --- SENT Signal. Type: ${currentOpportunityType}, Sell: ${arbSignalPayload.arb_signal.sell_market} (SpotP: ${arbSignalPayload.arb_signal.spot_price_trade}, FuturesP: ${arbSignalPayload.arb_signal.futures_price_trade}), Buy: ${arbSignalPayload.arb_signal.buy_market}, Deviation Profit: $${identifiedNetProfit.toFixed(4)}`);
                    lastSentArbitrageType = currentOpportunityType;
                } catch (error) {
                    console.error(`[Arbitrage-Module] PID: ${process.pid} --- Error stringifying or sending arbitrage opportunity:`, error.message, error.stack);
                }
            } else {
                console.warn(`[Arbitrage-Module] PID: ${process.pid} --- Detected ${currentOpportunityType} opportunity but internal client NOT OPEN. Deviation Profit: $${identifiedNetProfit.toFixed(4)}. Signal not sent.`);
            }
        }
    } else {
        if (lastSentArbitrageType !== null) {
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
        console.log(`[Arbitrage-Module] PID: ${process.pid} --- Connected to Spot BookTicker.`);
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

    spotWsClient.on('pong', () => { });

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
        console.log(`[Arbitrage-Module] PID: ${process.pid} --- Connected to Futures BookTicker.`);
        if (futuresPingIntervalId) clearInterval(futuresPingIntervalId);
        futuresPingIntervalId = setInterval(() => {
            if (futuresWsClient && futuresWsClient.readyState === WebSocket.OPEN) {
                try {
                    // Futures stream typically sends pong as a message, but ping frames should still be accepted.
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
            // Binance futures sends pongs as messages: {"e":"pong","T":1620835200000}
            // Or sometimes just `pong` for certain types of keepalives.
            // It's safer to parse and check for a known pong structure or ignore simple "pong" messages.
            if (messageString.includes('"e":"pong"') || messageString.trim().toLowerCase() === 'pong') {
                // console.debug(`[Arbitrage-Module] PID: ${process.pid} --- Pong message received from Futures BookTicker.`);
                return;
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

    // Spot sends actual pong frames, futures often sends pong as a message.
    // Still, listen for actual pong frames in case behavior changes or for other types of connections.
    futuresWsClient.on('pong', () => {
        // console.debug(`[Arbitrage-Module] PID: ${process.pid} --- Pong frame received from Futures BookTicker.`);
    });

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

// --- Start the connections and intervals ---
console.log(`[Arbitrage-Module] PID: ${process.pid} --- Arbitrage Module starting...`);

connectToInternalReceiver(); // For sending out arbitrage signals
connectToSpotBookTicker();
connectToFuturesBookTicker();

if (arbitrageCheckIntervalId) {
    clearInterval(arbitrageCheckIntervalId);
}
arbitrageCheckIntervalId = setInterval(performArbitrageCheckAndSignal, ARBITRAGE_CHECK_INTERVAL_MS);
console.log(`[Arbitrage-Module] PID: ${process.pid} --- Arbitrage check interval started, checking every ${ARBITRAGE_CHECK_INTERVAL_MS}ms.`);
