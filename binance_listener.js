// binance_listener.js

//Don't add any additional logs , it's not allowed 
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
        process.exit(1);
    }, 1000).unref();
});

// --- AggTrade Listener Configuration ---
const binanceStreamUrl = 'wss://stream.binance.com:9443/ws/btcusdt@aggTrade';
const internalReceiverUrl = 'ws://localhost:8082';
const RECONNECT_INTERVAL_MS = 5000;
const BINANCE_PING_INTERVAL_MS = 3 * 60 * 1000;
const PRICE_CHANGE_THRESHOLD = 1.2; // For aggTrade listener push to internal client
const PUSH_INTERVAL_MS = 25; // Fixed push interval for aggTrade data

// --- Arbitrage Configuration ---
const SPOT_BOOKTICKER_URL = 'wss://stream.binance.com:9443/ws/btcusdt@bookTicker';
const FUTURES_BOOKTICKER_URL = 'wss://fstream.binance.com/ws/btcusdt@bookTicker'; // For USDT-M Perpetual Futures
const ARB_RECONNECT_INTERVAL_MS = 5000;
const ARB_PING_INTERVAL_MS = 3 * 60 * 1000;
const TIMESTAMP_TOLERANCE_MS = 300; // Max allowed time difference between spot and futures data for arb check
const DESIRED_PROFIT_THRESHOLD_USD = 25.0; 
const TOTAL_FEES_PER_UNIT_USD = 0.2; 

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

let spotBestBid = null;
let spotBestAsk = null;
let spotTimestamp = null; 

let futuresBestBid = null;
let futuresBestAsk = null;
let futuresTimestamp = null; 


// --- Internal Receiver Connection (for aggTrade AND Arbitrage Signals) ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    internalWsClient = new WebSocket(internalReceiverUrl);

    internalWsClient.on('open', () => {
        lastPushedDataByInterval = null;
        latestSignificantDataToPush = null;
        startPushInterval(); 
    });

    internalWsClient.on('error', (err) => {
        console.error(`[Listener] PID: ${process.pid} --- Internal receiver WebSocket error:`, err.message);
    });

    internalWsClient.on('close', (code, reason) => {
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
    binanceWsClient = new WebSocket(binanceStreamUrl);

    binanceWsClient.on('open', function open() {
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
                    console.error(`[Listener] PID: ${process.pid} --- Invalid price in transformed aggTrade data:`, minimalData.p);
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

    binanceWsClient.on('pong', () => {});

    binanceWsClient.on('error', function error(err) {
        console.error(`[Listener] PID: ${process.pid} --- Binance AggTrade WebSocket error:`, err.message);
    });

    binanceWsClient.on('close', function close(code, reason) {
        if (binancePingIntervalId) { clearInterval(binancePingIntervalId); binancePingIntervalId = null; }
        binanceWsClient = null;
        setTimeout(connectToBinanceAggTrade, RECONNECT_INTERVAL_MS);
    });
}

// --- Arbitrage Logic Core ---
function checkArbitrageOpportunity() {
    if (spotBestBid === null || spotBestAsk === null || spotTimestamp === null ||
        futuresBestBid === null || futuresBestAsk === null || futuresTimestamp === null) {
        return; 
    }

    const timeDiff = Math.abs(spotTimestamp - futuresTimestamp);
    if (timeDiff > TIMESTAMP_TOLERANCE_MS) {
        return; 
    }

    let arbSignalPayload = null;

    // Scenario 1: Sell Futures, Buy Spot
    const profitScenario1 = futuresBestBid - spotBestAsk - TOTAL_FEES_PER_UNIT_USD;
    if (profitScenario1 > DESIRED_PROFIT_THRESHOLD_USD) {
        arbSignalPayload = {
            arb_signal: {
                sell_on: "futures",
                sell_price: futuresBestBid,
                buy_on: "spot",
                buy_price: spotBestAsk,
                net_profit_usd: parseFloat(profitScenario1.toFixed(4)),
                ts_spot_ms: spotTimestamp,
                ts_futures_ms: futuresTimestamp
            }
        };
    }

    // Scenario 2: Buy Futures, Sell Spot
    if (!arbSignalPayload) {
        const profitScenario2 = spotBestBid - futuresBestAsk - TOTAL_FEES_PER_UNIT_USD;
        if (profitScenario2 > DESIRED_PROFIT_THRESHOLD_USD) {
            arbSignalPayload = {
                arb_signal: {
                    sell_on: "spot",
                    sell_price: spotBestBid,
                    buy_on: "futures",
                    buy_price: futuresBestAsk,
                    net_profit_usd: parseFloat(profitScenario2.toFixed(4)),
                    ts_spot_ms: spotTimestamp,
                    ts_futures_ms: futuresTimestamp
                }
            };
        }
    }

    if (arbSignalPayload && internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        try {
            const messageString = JSON.stringify(arbSignalPayload);
            internalWsClient.send(messageString);
        } catch (error) {
            console.error(`[Arbitrage] PID: ${process.pid} --- Error stringifying or sending arbitrage opportunity:`, error.message, error.stack);
        }
    } else if (arbSignalPayload) {
        console.error(`[Arbitrage] PID: ${process.pid} --- Detected arbitrage opportunity but internal client is not open. Opportunity not sent. Profit: $${arbSignalPayload.arb_signal.net_profit_usd}`);
    }
}


// --- Spot BookTicker Connection (for Arbitrage) ---
function connectToSpotBookTicker() {
    if (spotWsClient && (spotWsClient.readyState === WebSocket.OPEN || spotWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    spotWsClient = new WebSocket(SPOT_BOOKTICKER_URL);

    spotWsClient.on('open', function open() {
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
                    console.error(`[Arbitrage] PID: ${process.pid} --- Invalid price in Spot BookTicker data: Bid=${tickerData.b}, Ask=${tickerData.a}`);
                    return;
                }
                
                spotBestBid = newSpotBestBid;
                spotBestAsk = newSpotBestAsk;
                spotTimestamp = tickerData.E || Date.now(); 

                checkArbitrageOpportunity();
            }
        } catch (e) {
            console.error(`[Arbitrage] PID: ${process.pid} --- CRITICAL ERROR in Spot BookTicker message handler:`, e.message, e.stack);
        }
    });

    spotWsClient.on('pong', () => {});

    spotWsClient.on('error', function error(err) {
        console.error(`[Arbitrage] PID: ${process.pid} --- Spot BookTicker WebSocket error:`, err.message);
    });

    spotWsClient.on('close', function close(code, reason) {
        if (spotPingIntervalId) { clearInterval(spotPingIntervalId); spotPingIntervalId = null; }
        spotWsClient = null;
        spotBestBid = null; 
        spotBestAsk = null; 
        spotTimestamp = null;
        setTimeout(connectToSpotBookTicker, ARB_RECONNECT_INTERVAL_MS);
    });
}

// --- Futures BookTicker Connection (for Arbitrage) ---
function connectToFuturesBookTicker() {
    if (futuresWsClient && (futuresWsClient.readyState === WebSocket.OPEN || futuresWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    futuresWsClient = new WebSocket(FUTURES_BOOKTICKER_URL);

    futuresWsClient.on('open', function open() {
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
            if (messageString.includes('"e":"pong"')) { 
                return; 
            }
            const tickerData = JSON.parse(messageString);

            if (tickerData && typeof tickerData.b !== 'undefined' && typeof tickerData.a !== 'undefined') {
                const newFuturesBestBid = parseFloat(tickerData.b);
                const newFuturesBestAsk = parseFloat(tickerData.a);

                if (isNaN(newFuturesBestBid) || isNaN(newFuturesBestAsk)) {
                    console.error(`[Arbitrage] PID: ${process.pid} --- Invalid price in Futures BookTicker data: Bid=${tickerData.b}, Ask=${tickerData.a}`);
                    return;
                }

                futuresBestBid = newFuturesBestBid;
                futuresBestAsk = newFuturesBestAsk;
                futuresTimestamp = tickerData.E || Date.now(); 

                checkArbitrageOpportunity();
            }
        } catch (e) {
            console.error(`[Arbitrage] PID: ${process.pid} --- CRITICAL ERROR in Futures BookTicker message handler:`, e.message, e.stack);
        }
    });

    futuresWsClient.on('pong', () => {}); 

    futuresWsClient.on('error', function error(err) {
        console.error(`[Arbitrage] PID: ${process.pid} --- Futures BookTicker WebSocket error:`, err.message);
    });

    futuresWsClient.on('close', function close(code, reason) {
        if (futuresPingIntervalId) { clearInterval(futuresPingIntervalId); futuresPingIntervalId = null; }
        futuresWsClient = null;
        futuresBestBid = null; 
        futuresBestAsk = null; 
        futuresTimestamp = null;
        setTimeout(connectToFuturesBookTicker, ARB_RECONNECT_INTERVAL_MS);
    });
}

// --- Start the connections ---
connectToInternalReceiver(); 
connectToBinanceAggTrade();

connectToSpotBookTicker();
connectToFuturesBookTicker();
