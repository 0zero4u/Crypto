// binance_listener.js

const WebSocket = require('ws');

// --- Global Error Handlers ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`);
    console.error(err.stack || err);
    console.error(`[Listener] Exception origin: ${origin}`);
    console.error(`[Listener] PID: ${process.pid} --- Exiting due to uncaught exception...`);
    setTimeout(() => {
        if (internalWsClient && typeof internalWsClient.terminate === 'function') { try { internalWsClient.terminate(); } catch (e) { /* ignore */ } }
        if (binanceWsClient && typeof binanceWsClient.terminate === 'function') { try { binanceWsClient.terminate(); } catch (e) { /* ignore */ } }
        if (bybitWsClient && typeof bybitWsClient.terminate === 'function') { try { bybitWsClient.terminate(); } catch (e) { /* ignore */ } }
        process.exit(1);
    }, 1000).unref();
});

process.on('unhandledRejection', (reason, promise) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNHANDLED PROMISE REJECTION`);
    console.error('[Listener] Unhandled Rejection at:', promise);
    console.error('[Listener] Reason:', reason instanceof Error ? reason.stack : reason);
    console.error(`[Listener] PID: ${process.pid} --- Exiting due to unhandled promise rejection...`);
    setTimeout(() => {
        if (internalWsClient && typeof internalWsClient.terminate === 'function') { try { internalWsClient.terminate(); } catch (e) { /* ignore */ } }
        if (binanceWsClient && typeof binanceWsClient.terminate === 'function') { try { binanceWsClient.terminate(); } catch (e) { /* ignore */ } }
        if (bybitWsClient && typeof bybitWsClient.terminate === 'function') { try { bybitWsClient.terminate(); } catch (e) { /* ignore */ } }
        process.exit(1);
    }, 1000).unref();
});

// --- Configuration ---
const INTERNAL_RECEIVER_URL = 'ws://localhost:8082';
const RECONNECT_INTERVAL_MS = 5000;

const PRICE_CHANGE_THRESHOLD_BINANCE = 1.0; 
const PRICE_CHANGE_THRESHOLD_BYBIT = 4.0;   

const BINANCE_STREAM_URL = 'wss://stream.binance.com:9443/ws/btcusdt@aggTrade';
const BINANCE_PING_INTERVAL_MS = 3 * 60 * 1000;

const BYBIT_SPOT_STREAM_URL = 'wss://stream.bybit.com/v5/public/spot';
const BYBIT_SUBSCRIPTION_PAYLOAD = { "op": "subscribe", "args": ["orderbook.1.BTCUSDT"] };
const BYBIT_PING_PAYLOAD = { "op": "ping" };
const BYBIT_PING_INTERVAL_MS = 20 * 1000;

const MIN_REVERSAL_PRICE_DIFFERENCE = 0.1; // IMPORTANT: Adjust this based on the asset & desired sensitivity! For BTC, $2 might be small.

// --- State Variables ---
let internalWsClient = null;

let binanceWsClient = null;
let binancePingIntervalId = null;
let lastSentBinancePrice = null;

let bybitWsClient = null;
let bybitPingIntervalId = null;
let lastSentBybitPrice = null;

// --- Ticker Direction State ---
let lastBinanceAggTradePrice_Direction = null;
let latestBybitBestBid = null;
let prevBybitBidAtLastBinanceIncrease = null;
let prevBybitBidAtLastBinanceDecrease = null;
let lastSentSignalType = null; 
let priceAtLastBuySignal = null;    // NEW
let priceAtLastSellSignal = null;   // NEW


// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    console.log(`[Listener] PID: ${process.pid} --- Connecting to internal receiver: ${INTERNAL_RECEIVER_URL}`);
    internalWsClient = new WebSocket(INTERNAL_RECEIVER_URL);

    internalWsClient.on('open', () => {
        console.log(`[Listener] PID: ${process.pid} --- Connected to internal receiver.`);
        lastSentBinancePrice = null;
        lastSentBybitPrice = null;
        lastSentSignalType = null; 
        priceAtLastBuySignal = null;    // Reset on new/reconnect
        priceAtLastSellSignal = null;   // Reset on new/reconnect
    });

    internalWsClient.on('error', (err) => {
        console.error(`[Listener] PID: ${process.pid} --- Internal receiver WebSocket error:`, err.message);
    });

    internalWsClient.on('close', (code, reason) => {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] PID: ${process.pid} --- Internal receiver closed. Code: ${code}, Reason: ${reasonStr}. Reconnecting in ${RECONNECT_INTERVAL_MS / 1000}s...`);
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
}

// --- Manual Data Extraction (Binance) ---
function manualExtractBinanceAggTradeData(messageString) {
    try {
        let eventTimeNum = null;
        let priceStr = null;
        const aggTradeEventTypeKey = '"e":"aggTrade"';
        let currentIndex = messageString.indexOf(aggTradeEventTypeKey);
        if (currentIndex === -1) return null;
        const eventTimeKey = '"E":';
        currentIndex = messageString.indexOf(eventTimeKey); 
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
        let priceStartIndex = messageString.indexOf(priceKey); 
        if (priceStartIndex === -1) return null;
        priceStartIndex += priceKey.length;
        let priceEndIndex = messageString.indexOf('"', priceStartIndex);
        if (priceEndIndex === -1) return null;
        priceStr = messageString.substring(priceStartIndex, priceEndIndex);
        if (priceStr.length === 0 || isNaN(parseFloat(priceStr))) return null;
        return { e: "aggTrade", E: eventTimeNum, p: priceStr };
    } catch (error) {
        return null;
    }
}

// --- Binance Stream Connection ---
function connectToBinance() {
    if (binanceWsClient && (binanceWsClient.readyState === WebSocket.OPEN || binanceWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    console.log(`[Listener] PID: ${process.pid} --- Connecting to Binance: ${BINANCE_STREAM_URL}`);
    binanceWsClient = new WebSocket(BINANCE_STREAM_URL);

    binanceWsClient.on('open', function open() {
        console.log(`[Listener] PID: ${process.pid} --- Connected to Binance stream (btcusdt@aggTrade).`);
        lastSentBinancePrice = null; 
        if (binancePingIntervalId) clearInterval(binancePingIntervalId);
        binancePingIntervalId = setInterval(() => {
            if (binanceWsClient && binanceWsClient.readyState === WebSocket.OPEN) {
                try { binanceWsClient.ping(() => {}); } 
                catch (pingError) { console.error(`[Listener] PID: ${process.pid} --- Binance: Error sending ping:`, pingError.message); }
            }
        }, BINANCE_PING_INTERVAL_MS);
    });

    binanceWsClient.on('message', function incoming(data) {
        try {
            const messageString = data.toString();
            const extractedBinanceData = manualExtractBinanceAggTradeData(messageString); 

            if (extractedBinanceData) {
                const currentPrice = parseFloat(extractedBinanceData.p);
                if (isNaN(currentPrice)) {
                    console.warn(`[Listener] PID: ${process.pid} --- Binance: Invalid price in aggTrade data:`, extractedBinanceData.p);
                    return;
                }

                // --- Ticker Direction Logic ---
                if (lastBinanceAggTradePrice_Direction === null) {
                    lastBinanceAggTradePrice_Direction = currentPrice;
                    if (latestBybitBestBid !== null) {
                        prevBybitBidAtLastBinanceIncrease = latestBybitBestBid;
                        prevBybitBidAtLastBinanceDecrease = latestBybitBestBid;
                    }
                } else {
                    let immediateTickDirection = "NEUTRAL";
                    if (currentPrice > lastBinanceAggTradePrice_Direction) immediateTickDirection = "INCREASE";
                    else if (currentPrice < lastBinanceAggTradePrice_Direction) immediateTickDirection = "DECREASE";

                    if (immediateTickDirection === "INCREASE") {
                        if (latestBybitBestBid !== null && prevBybitBidAtLastBinanceIncrease !== null) {
                            if (latestBybitBestBid > prevBybitBidAtLastBinanceIncrease) { // Bybit confirms
                                const currentSignalType = "BUY";
                                let allowSignal = true;

                                if (lastSentSignalType === currentSignalType) {
                                    allowSignal = false; 
                                } else if (lastSentSignalType === "SELL") {
                                    if (priceAtLastSellSignal === null || (currentPrice - priceAtLastSellSignal) <= MIN_REVERSAL_PRICE_DIFFERENCE) {
                                        allowSignal = false; 
                                    }
                                }

                                if (allowSignal) {
                                    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                                        const signalMsg = { o: "binance", p: extractedBinanceData.p, signal: currentSignalType };
                                        console.log(`[SIG] --> Sending TRUE TICK [${currentSignalType}]. Binance: ${lastBinanceAggTradePrice_Direction.toFixed(2)}->${currentPrice.toFixed(2)}, Bybit Bid: ${prevBybitBidAtLastBinanceIncrease.toFixed(2)}->${latestBybitBestBid.toFixed(2)}`);
                                        try {
                                            internalWsClient.send(JSON.stringify(signalMsg));
                                            lastSentSignalType = currentSignalType;
                                            priceAtLastBuySignal = currentPrice; 
                                            priceAtLastSellSignal = null;       
                                        } catch (e) {
                                            console.error(`[Listener] PID: ${process.pid} --- Error sending TRUE TICK [${currentSignalType}]:`, e.message);
                                        }
                                    }
                                }
                            }
                        }
                        if (latestBybitBestBid !== null) prevBybitBidAtLastBinanceIncrease = latestBybitBestBid;
                    } else if (immediateTickDirection === "DECREASE") {
                        if (latestBybitBestBid !== null && prevBybitBidAtLastBinanceDecrease !== null) {
                            if (latestBybitBestBid < prevBybitBidAtLastBinanceDecrease) { // Bybit confirms
                                const currentSignalType = "SELL";
                                let allowSignal = true;

                                if (lastSentSignalType === currentSignalType) {
                                    allowSignal = false; 
                                } else if (lastSentSignalType === "BUY") {
                                    if (priceAtLastBuySignal === null || (priceAtLastBuySignal - currentPrice) <= MIN_REVERSAL_PRICE_DIFFERENCE) {
                                        allowSignal = false;
                                    }
                                }
                                
                                if (allowSignal) {
                                    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                                        const signalMsg = { o: "binance", p: extractedBinanceData.p, signal: currentSignalType };
                                        console.log(`[SIG] --> Sending TRUE TICK [${currentSignalType}]. Binance: ${lastBinanceAggTradePrice_Direction.toFixed(2)}->${currentPrice.toFixed(2)}, Bybit Bid: ${prevBybitBidAtLastBinanceDecrease.toFixed(2)}->${latestBybitBestBid.toFixed(2)}`);
                                        try {
                                            internalWsClient.send(JSON.stringify(signalMsg));
                                            lastSentSignalType = currentSignalType;
                                            priceAtLastSellSignal = currentPrice; 
                                            priceAtLastBuySignal = null;        
                                        } catch (e) {
                                            console.error(`[Listener] PID: ${process.pid} --- Error sending TRUE TICK [${currentSignalType}]:`, e.message);
                                        }
                                    }
                                }
                            }
                        }
                        if (latestBybitBestBid !== null) prevBybitBidAtLastBinanceDecrease = latestBybitBestBid;
                    }
                    lastBinanceAggTradePrice_Direction = currentPrice;
                }

                // --- General Price Update Sending ---
                let shouldSendData = false;
                if (lastSentBinancePrice === null) shouldSendData = true;
                else {
                    const priceDifference = Math.abs(currentPrice - lastSentBinancePrice);
                    if (priceDifference >= PRICE_CHANGE_THRESHOLD_BINANCE) shouldSendData = true;
                }

                if (shouldSendData) {
                    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                        try {
                            const generalUpdatePayload = { o: "binance", p: extractedBinanceData.p };
                            internalWsClient.send(JSON.stringify(generalUpdatePayload)); 
                            lastSentBinancePrice = currentPrice;
                        } catch (sendError) {
                            console.error(`[Listener] PID: ${process.pid} --- Binance: Error sending general price data:`, sendError.message);
                        }
                    }
                }
            } else { 
                if (messageString && !messageString.includes('"e":"pong"')) { 
                    let isPotentiallyJson = false;
                    try { JSON.parse(messageString); isPotentiallyJson = true; } catch (e) { /* not json */ }
                    if (isPotentiallyJson && !messageString.includes('"e":"aggTrade"')) {
                         console.warn(`[Listener] PID: ${process.pid} --- Binance: Received unexpected JSON (not aggTrade). Snippet:`, messageString.substring(0, 150));
                    } else if (!isPotentiallyJson) {
                         console.warn(`[Listener] PID: ${process.pid} --- Binance: Received non-JSON message. Snippet:`, messageString.substring(0, 150));
                    }
                }
            }
        } catch (e) {
            console.error(`[Listener] PID: ${process.pid} --- Binance: CRITICAL ERROR in message handler:`, e.message, e.stack);
        }
    });
    
    binanceWsClient.on('error', function error(err) {
        console.error(`[Listener] PID: ${process.pid} --- Binance WebSocket error:`, err.message);
    });

    binanceWsClient.on('close', function close(code, reason) {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] PID: ${process.pid} --- Binance WebSocket closed. Code: ${code}, Reason: ${reasonStr}. Reconnecting in ${RECONNECT_INTERVAL_MS / 1000}s...`);
        if (binancePingIntervalId) { clearInterval(binancePingIntervalId); binancePingIntervalId = null; }
        binanceWsClient = null;
        setTimeout(connectToBinance, RECONNECT_INTERVAL_MS);
    });
}

// --- Bybit Stream Connection ---
function connectToBybit() {
    if (bybitWsClient && (bybitWsClient.readyState === WebSocket.OPEN || bybitWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    console.log(`[Listener] PID: ${process.pid} --- Connecting to Bybit: ${BYBIT_SPOT_STREAM_URL}`);
    bybitWsClient = new WebSocket(BYBIT_SPOT_STREAM_URL);

    bybitWsClient.on('open', function open() {
        console.log(`[Listener] PID: ${process.pid} --- Connected to Bybit stream.`);
        lastSentBybitPrice = null;
        try {
            bybitWsClient.send(JSON.stringify(BYBIT_SUBSCRIPTION_PAYLOAD));
        } catch (e) {
            console.error(`[Listener] PID: ${process.pid} --- Bybit: Error sending subscription message:`, e.message);
            if (bybitWsClient) bybitWsClient.terminate(); 
            return;
        }
        if (bybitPingIntervalId) clearInterval(bybitPingIntervalId);
        bybitPingIntervalId = setInterval(() => {
            if (bybitWsClient && bybitWsClient.readyState === WebSocket.OPEN) {
                try { bybitWsClient.send(JSON.stringify(BYBIT_PING_PAYLOAD)); } 
                catch (pingError) { console.error(`[Listener] PID: ${process.pid} --- Bybit: Error sending ping:`, pingError.message); }
            }
        }, BYBIT_PING_INTERVAL_MS);
    });

    bybitWsClient.on('message', function incoming(data) {
        try {
            const messageString = data.toString();
            const parsedMessage = JSON.parse(messageString);

            if (parsedMessage.op === 'ping' || (parsedMessage.ret_msg === 'pong' && parsedMessage.op === 'ping')) {
                return;
            }
            if (parsedMessage.op === 'subscribe') { 
                if (parsedMessage.success) {
                    console.log(`[Listener] PID: ${process.pid} --- Bybit: Subscription success for ${parsedMessage.args ? parsedMessage.args.join(', ') : 'N/A'}`);
                } else {
                    console.error(`[Listener] PID: ${process.pid} --- Bybit: Subscription failed: ${messageString}`);
                }
                return; 
            }

            if (parsedMessage.topic && parsedMessage.topic === BYBIT_SUBSCRIPTION_PAYLOAD.args[0] && parsedMessage.data) {
                const bybitOrderbookData = parsedMessage.data; 
                let currentBestBidNum = null;
                if (bybitOrderbookData.b && bybitOrderbookData.b.length > 0 && bybitOrderbookData.b[0] && typeof bybitOrderbookData.b[0][0] === 'string') {
                    currentBestBidNum = parseFloat(bybitOrderbookData.b[0][0]);
                    if (isNaN(currentBestBidNum)) currentBestBidNum = null;
                }
                if (currentBestBidNum !== null) {
                    latestBybitBestBid = currentBestBidNum;
                    if (prevBybitBidAtLastBinanceIncrease === null) prevBybitBidAtLastBinanceIncrease = latestBybitBestBid;
                    if (prevBybitBidAtLastBinanceDecrease === null) prevBybitBidAtLastBinanceDecrease = latestBybitBestBid;
                }
                let priceStringForSending = null;
                if (bybitOrderbookData.a && bybitOrderbookData.a.length > 0 && bybitOrderbookData.a[0] && typeof bybitOrderbookData.a[0][0] === 'string') {
                    priceStringForSending = bybitOrderbookData.a[0][0]; 
                } else if (bybitOrderbookData.b && bybitOrderbookData.b.length > 0 && bybitOrderbookData.b[0] && typeof bybitOrderbookData.b[0][0] === 'string') {
                    priceStringForSending = bybitOrderbookData.b[0][0]; 
                }
                if (!priceStringForSending) return; 
                const currentPriceForSending = parseFloat(priceStringForSending);
                if (isNaN(currentPriceForSending)) {
                    console.warn(`[Listener] PID: ${process.pid} --- Bybit: Invalid price for general sending from orderbook:`, priceStringForSending);
                    return;
                }
                const generalBybitPayload = { o: "bybit", p: priceStringForSending };
                let shouldSendData = false;
                if (lastSentBybitPrice === null) shouldSendData = true;
                else {
                    const priceDifference = Math.abs(currentPriceForSending - lastSentBybitPrice);
                    if (priceDifference >= PRICE_CHANGE_THRESHOLD_BYBIT) shouldSendData = true; 
                }
                if (shouldSendData) {
                    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                        try {
                            internalWsClient.send(JSON.stringify(generalBybitPayload));
                            lastSentBybitPrice = currentPriceForSending;
                        } catch (sendError) {
                            console.error(`[Listener] PID: ${process.pid} --- Bybit: Error sending general price data:`, sendError.message);
                        }
                    }
                }
            } else if (parsedMessage.topic && parsedMessage.topic.startsWith("kline")) {
                // Ignore
            } else if (parsedMessage.event !== "heartbeat" && parsedMessage.type !== "snapshot" && !(parsedMessage.ret_msg && parsedMessage.ret_msg.includes("pong"))) { 
                 console.warn(`[Listener] PID: ${process.pid} --- Bybit: Received unhandled message type. Topic: ${parsedMessage.topic}, Op: ${parsedMessage.op}, Snippet:`, messageString.substring(0, 250));
            }
        } catch (e) {
             if (e instanceof SyntaxError && data.toString().includes("pong")) {
                // ignore
            } else {
                console.error(`[Listener] PID: ${process.pid} --- Bybit: CRITICAL ERROR in message handler:`, e.message, e.stack);
                console.error(`[Listener] PID: ${process.pid} --- Bybit: Failing message string: ${data.toString().substring(0,500)}`);
            }
        }
    });

    bybitWsClient.on('error', function error(err) {
        console.error(`[Listener] PID: ${process.pid} --- Bybit WebSocket error:`, err.message);
    });

    bybitWsClient.on('close', function close(code, reason) {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] PID: ${process.pid} --- Bybit WebSocket closed. Code: ${code}, Reason: ${reasonStr}. Reconnecting in ${RECONNECT_INTERVAL_MS / 1000}s...`);
        if (bybitPingIntervalId) { clearInterval(bybitPingIntervalId); bybitPingIntervalId = null; }
        bybitWsClient = null;
        latestBybitBestBid = null;
        prevBybitBidAtLastBinanceIncrease = null;
        prevBybitBidAtLastBinanceDecrease = null;
        setTimeout(connectToBybit, RECONNECT_INTERVAL_MS);
    });
}

// --- Start the connections ---
console.log(`[Listener] PID: ${process.pid} --- Listener starting...`);
console.log(`[Listener] Binance AggTrade: ${BINANCE_STREAM_URL}, Bybit Orderbook: ${BYBIT_SPOT_STREAM_URL}`);
console.log(`[Listener] General Price Update Thresholds: Binance=${PRICE_CHANGE_THRESHOLD_BINANCE}, Bybit=${PRICE_CHANGE_THRESHOLD_BYBIT}`);
console.log(`[Listener] Ticker Direction signal logic: Min Reversal Diff=${MIN_REVERSAL_PRICE_DIFFERENCE}. Consecutive identical signals suppressed.`);
console.log(`[Listener] General price updates client payload: {o, p}.`);

connectToInternalReceiver();
connectToBinance();
connectToBybit();
