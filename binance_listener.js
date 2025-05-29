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
const BYBIT_SUBSCRIPTION_PAYLOAD = { "op": "subscribe", "args": ["orderbook.1.BTCUSDT"] }; // Using orderbook.1 for best bid/ask
const BYBIT_PING_PAYLOAD = { "op": "ping" };
const BYBIT_PING_INTERVAL_MS = 20 * 1000;

const MIN_REVERSAL_PRICE_DIFFERENCE = 2.0; // Applied to Bybit prices now

// --- State Variables ---
let internalWsClient = null;

let binanceWsClient = null;
let binancePingIntervalId = null;
let lastSentBinancePrice = null; // For general Binance updates

let bybitWsClient = null;
let bybitPingIntervalId = null;
let lastSentBybitPrice = null; // For general Bybit updates

// --- Ticker Direction State (Bybit initiates, Binance confirms) ---
let lastBybitPrice_Direction = null;      // Last Bybit price used for directional signal logic
let latestBinanceAggTradePrice = null;   // Most recent Binance price for confirmation
let prevBinancePriceAtLastBybitIncrease = null;
let prevBinancePriceAtLastBybitDecrease = null;

let lastSentSignalType = null; 
let priceAtLastBuySignal = null;    // Stores Bybit's price at last BUY
let priceAtLastSellSignal = null;   // Stores Bybit's price at last SELL

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
        priceAtLastBuySignal = null;    
        priceAtLastSellSignal = null;   
        // Reset Bybit-Binance signal states
        lastBybitPrice_Direction = null;
        // latestBinanceAggTradePrice is dynamic, no need to reset here
        prevBinancePriceAtLastBybitIncrease = null;
        prevBinancePriceAtLastBybitDecrease = null;
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
        // Only need price for confirmation now, but keep E for potential future use
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

// --- Binance Stream Connection (Now primarily for confirmation & general updates) ---
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
                const currentBinancePriceNum = parseFloat(extractedBinanceData.p);
                if (isNaN(currentBinancePriceNum)) {
                    console.warn(`[Listener] PID: ${process.pid} --- Binance: Invalid price in aggTrade data:`, extractedBinanceData.p);
                    return;
                }
                latestBinanceAggTradePrice = currentBinancePriceNum; // Update for Bybit's confirmation logic

                // Initialize baselines if Bybit already has a price direction
                if (lastBybitPrice_Direction !== null) {
                    if (prevBinancePriceAtLastBybitIncrease === null) prevBinancePriceAtLastBybitIncrease = latestBinanceAggTradePrice;
                    if (prevBinancePriceAtLastBybitDecrease === null) prevBinancePriceAtLastBybitDecrease = latestBinanceAggTradePrice;
                }


                // --- General Binance Price Update Sending ---
                let shouldSendData = false;
                if (lastSentBinancePrice === null) shouldSendData = true;
                else {
                    const priceDifference = Math.abs(currentBinancePriceNum - lastSentBinancePrice);
                    if (priceDifference >= PRICE_CHANGE_THRESHOLD_BINANCE) shouldSendData = true;
                }

                if (shouldSendData) {
                    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                        try {
                            const generalUpdatePayload = { o: "binance", p: extractedBinanceData.p };
                            internalWsClient.send(JSON.stringify(generalUpdatePayload)); 
                            lastSentBinancePrice = currentBinancePriceNum;
                        } catch (sendError) {
                            console.error(`[Listener] PID: ${process.pid} --- Binance: Error sending general price data:`, sendError.message);
                        }
                    }
                }
            } else { 
                if (messageString && !messageString.includes('"e":"pong"')) { 
                    // ... (existing warning logs for unexpected Binance messages)
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
        latestBinanceAggTradePrice = null; // Binance disconnected, no confirmation price
        prevBinancePriceAtLastBybitIncrease = null;
        prevBinancePriceAtLastBybitDecrease = null;
        console.log(`[SIG] Binance disconnected. Resetting Binance confirmation baselines.`);
        setTimeout(connectToBinance, RECONNECT_INTERVAL_MS);
    });
}

// --- Bybit Stream Connection (Now initiates signals) ---
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

            if (parsedMessage.op === 'ping' || (parsedMessage.ret_msg === 'pong' && parsedMessage.op === 'ping')) return;
            if (parsedMessage.op === 'subscribe') { 
                if (parsedMessage.success) { console.log(`[Listener] PID: ${process.pid} --- Bybit: Subscription success for ${parsedMessage.args ? parsedMessage.args.join(', ') : 'N/A'}`); } 
                else { console.error(`[Listener] PID: ${process.pid} --- Bybit: Subscription failed: ${messageString}`); }
                return; 
            }

            if (parsedMessage.topic && parsedMessage.topic === BYBIT_SUBSCRIPTION_PAYLOAD.args[0] && parsedMessage.data) {
                const bybitOrderbookData = parsedMessage.data; 
                
                // Determine current Bybit price for signal logic (e.g., best bid)
                let currentBybitPriceStringForSignal = null;
                let currentBybitPriceNumForSignal = null;

                if (bybitOrderbookData.b && bybitOrderbookData.b.length > 0 && bybitOrderbookData.b[0] && typeof bybitOrderbookData.b[0][0] === 'string') {
                    currentBybitPriceStringForSignal = bybitOrderbookData.b[0][0];
                    currentBybitPriceNumForSignal = parseFloat(currentBybitPriceStringForSignal);
                    if (isNaN(currentBybitPriceNumForSignal)) currentBybitPriceNumForSignal = null;
                }

                if (currentBybitPriceNumForSignal === null) {
                     // console.warn(`[Listener] PID: ${process.pid} --- Bybit: No valid best bid for signal logic.`);
                    return; // Cannot proceed with signal logic without a price
                }

                // --- Ticker Direction Logic (Bybit initiates) ---
                if (lastBybitPrice_Direction === null) {
                    lastBybitPrice_Direction = currentBybitPriceNumForSignal;
                    if (latestBinanceAggTradePrice !== null) {
                        prevBinancePriceAtLastBybitIncrease = latestBinanceAggTradePrice;
                        prevBinancePriceAtLastBybitDecrease = latestBinanceAggTradePrice;
                    }
                } else {
                    let immediateBybitDirection = "NEUTRAL";
                    if (currentBybitPriceNumForSignal > lastBybitPrice_Direction) immediateBybitDirection = "INCREASE";
                    else if (currentBybitPriceNumForSignal < lastBybitPrice_Direction) immediateBybitDirection = "DECREASE";

                    if (immediateBybitDirection === "INCREASE") {
                        if (latestBinanceAggTradePrice !== null && prevBinancePriceAtLastBybitIncrease !== null) {
                            if (latestBinanceAggTradePrice > prevBinancePriceAtLastBybitIncrease) { // Binance confirms
                                const currentSignalType = "BUY";
                                let allowSignal = true;

                                if (lastSentSignalType === currentSignalType) {
                                    allowSignal = false; 
                                } else if (lastSentSignalType === "SELL") {
                                    if (priceAtLastSellSignal === null || (currentBybitPriceNumForSignal - priceAtLastSellSignal) <= MIN_REVERSAL_PRICE_DIFFERENCE) {
                                        allowSignal = false; 
                                    }
                                }

                                if (allowSignal) {
                                    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                                        const signalMsg = { o: "bybit", p: currentBybitPriceStringForSignal, signal: currentSignalType }; // Signal from Bybit
                                        console.log(`[SIG] --> Sending TRUE TICK [${currentSignalType}]. Bybit: ${lastBybitPrice_Direction.toFixed(2)}->${currentBybitPriceNumForSignal.toFixed(2)}, Binance Confirm: ${prevBinancePriceAtLastBybitIncrease.toFixed(2)}->${latestBinanceAggTradePrice.toFixed(2)}`);
                                        try {
                                            internalWsClient.send(JSON.stringify(signalMsg));
                                            lastSentSignalType = currentSignalType;
                                            priceAtLastBuySignal = currentBybitPriceNumForSignal; 
                                            priceAtLastSellSignal = null;       
                                        } catch (e) {
                                            console.error(`[Listener] PID: ${process.pid} --- Error sending TRUE TICK [${currentSignalType}] from Bybit:`, e.message);
                                        }
                                    }
                                }
                            }
                        }
                        if (latestBinanceAggTradePrice !== null) prevBinancePriceAtLastBybitIncrease = latestBinanceAggTradePrice;
                    } else if (immediateBybitDirection === "DECREASE") {
                        if (latestBinanceAggTradePrice !== null && prevBinancePriceAtLastBybitDecrease !== null) {
                            if (latestBinanceAggTradePrice < prevBinancePriceAtLastBybitDecrease) { // Binance confirms
                                const currentSignalType = "SELL";
                                let allowSignal = true;

                                if (lastSentSignalType === currentSignalType) {
                                    allowSignal = false; 
                                } else if (lastSentSignalType === "BUY") {
                                    if (priceAtLastBuySignal === null || (priceAtLastBuySignal - currentBybitPriceNumForSignal) <= MIN_REVERSAL_PRICE_DIFFERENCE) {
                                        allowSignal = false;
                                    }
                                }
                                
                                if (allowSignal) {
                                    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                                        const signalMsg = { o: "bybit", p: currentBybitPriceStringForSignal, signal: currentSignalType }; // Signal from Bybit
                                        console.log(`[SIG] --> Sending TRUE TICK [${currentSignalType}]. Bybit: ${lastBybitPrice_Direction.toFixed(2)}->${currentBybitPriceNumForSignal.toFixed(2)}, Binance Confirm: ${prevBinancePriceAtLastBybitDecrease.toFixed(2)}->${latestBinanceAggTradePrice.toFixed(2)}`);
                                        try {
                                            internalWsClient.send(JSON.stringify(signalMsg));
                                            lastSentSignalType = currentSignalType;
                                            priceAtLastSellSignal = currentBybitPriceNumForSignal; 
                                            priceAtLastBuySignal = null;        
                                        } catch (e) {
                                            console.error(`[Listener] PID: ${process.pid} --- Error sending TRUE TICK [${currentSignalType}] from Bybit:`, e.message);
                                        }
                                    }
                                }
                            }
                        }
                        if (latestBinanceAggTradePrice !== null) prevBinancePriceAtLastBybitDecrease = latestBinanceAggTradePrice;
                    }
                    lastBybitPrice_Direction = currentBybitPriceNumForSignal;
                }

                // --- General Bybit Price Update Sending (use best ask or best bid for this) ---
                let priceStringForGeneralSending = null;
                if (bybitOrderbookData.a && bybitOrderbookData.a.length > 0 && bybitOrderbookData.a[0] && typeof bybitOrderbookData.a[0][0] === 'string') {
                    priceStringForGeneralSending = bybitOrderbookData.a[0][0]; 
                } else if (bybitOrderbookData.b && bybitOrderbookData.b.length > 0 && bybitOrderbookData.b[0] && typeof bybitOrderbookData.b[0][0] === 'string') {
                    priceStringForGeneralSending = bybitOrderbookData.b[0][0]; // Fallback to best bid if no ask
                }

                if (priceStringForGeneralSending) {
                    const currentPriceForGeneralSending = parseFloat(priceStringForGeneralSending);
                    if (!isNaN(currentPriceForGeneralSending)) {
                        let shouldSendData = false;
                        if (lastSentBybitPrice === null) shouldSendData = true;
                        else {
                            const priceDifference = Math.abs(currentPriceForGeneralSending - lastSentBybitPrice);
                            if (priceDifference >= PRICE_CHANGE_THRESHOLD_BYBIT) shouldSendData = true; 
                        }
                        if (shouldSendData) {
                            if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                                try {
                                    const generalBybitPayload = { o: "bybit", p: priceStringForGeneralSending };
                                    internalWsClient.send(JSON.stringify(generalBybitPayload));
                                    lastSentBybitPrice = currentPriceForGeneralSending;
                                } catch (sendError) {
                                    console.error(`[Listener] PID: ${process.pid} --- Bybit: Error sending general price data:`, sendError.message);
                                }
                            }
                        }
                    } else {
                        console.warn(`[Listener] PID: ${process.pid} --- Bybit: Invalid price for general sending from orderbook:`, priceStringForGeneralSending);
                    }
                }
            } else if (parsedMessage.topic && parsedMessage.topic.startsWith("kline")) {
                // Ignore kline
            } else if (parsedMessage.event !== "heartbeat" && parsedMessage.type !== "snapshot" && !(parsedMessage.ret_msg && parsedMessage.ret_msg.includes("pong"))) { 
                 console.warn(`[Listener] PID: ${process.pid} --- Bybit: Received unhandled message type. Topic: ${parsedMessage.topic}, Op: ${parsedMessage.op}, Snippet:`, messageString.substring(0, 250));
            }
        } catch (e) {
             if (e instanceof SyntaxError && data.toString().includes("pong")) { /* ignore */ } 
             else {
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
        lastBybitPrice_Direction = null; // Bybit disconnected
        // prevBinancePriceAt... will be re-established when Binance sends new data or Bybit reconnects and gets first price
        console.log(`[SIG] Bybit disconnected. Resetting Bybit signal initiation baselines.`);
        setTimeout(connectToBybit, RECONNECT_INTERVAL_MS);
    });
}

// --- Start the connections ---
console.log(`[Listener] PID: ${process.pid} --- Listener starting...`);
console.log(`[Listener] Signal Source: Bybit Orderbook (${BYBIT_SPOT_STREAM_URL}), Confirmation: Binance AggTrade (${BINANCE_STREAM_URL})`);
console.log(`[Listener] General Price Update Thresholds: Binance=${PRICE_CHANGE_THRESHOLD_BINANCE}, Bybit=${PRICE_CHANGE_THRESHOLD_BYBIT}`);
console.log(`[Listener] Ticker Direction signal logic: Min Reversal Diff (Bybit Price)=${MIN_REVERSAL_PRICE_DIFFERENCE}. Consecutive identical signals suppressed.`);
console.log(`[Listener] Signal payload: {o: "bybit", p: BYBIT_PRICE, signal?}. General updates: {o: "binance/bybit", p}.`);

connectToInternalReceiver();
connectToBinance(); // Connect Binance first so latestBinanceAggTradePrice might be available early
connectToBybit();
