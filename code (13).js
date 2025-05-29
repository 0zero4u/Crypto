// binance_listener.js

const WebSocket = require('ws');

// ... (Global Error Handlers - remain the same) ...
process.on('uncaughtException', (err, origin) => { /* ... */ });
process.on('unhandledRejection', (reason, promise) => { /* ... */ });


// --- Configuration ---
const INTERNAL_RECEIVER_URL = 'ws://localhost:8082';
const RECONNECT_INTERVAL_MS = 5000;

const PRICE_CHANGE_THRESHOLD_BINANCE = 1.0; 
const PRICE_CHANGE_THRESHOLD_BYBIT = 4.0;   
const SIGNAL_REVERSAL_THRESHOLD = 2.0; // NEW: Min price change for opposite signal

const BINANCE_STREAM_URL = 'wss://stream.binance.com:9443/ws/btcusdt@aggTrade';
const BINANCE_PING_INTERVAL_MS = 3 * 60 * 1000;

const BYBIT_SPOT_STREAM_URL = 'wss://stream.bybit.com/v5/public/spot';
const BYBIT_SUBSCRIPTION_PAYLOAD = { "op": "subscribe", "args": ["orderbook.1.BTCUSDT"] };
const BYBIT_PING_PAYLOAD = { "op": "ping" };
const BYBIT_PING_INTERVAL_MS = 20 * 1000;

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
        priceAtLastBuySignal = null; // Reset on new/reconnect
        priceAtLastSellSignal = null;// Reset on new/reconnect
    });

    // ... (internalWsClient.on('error'...) and .on('close'...) remain the same) ...
    internalWsClient.on('error', (err) => { /* ... */ });
    internalWsClient.on('close', (code, reason) => { /* ... */ setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS); });
}

// ... (manualExtractBinanceAggTradeData - remains the same) ...
function manualExtractBinanceAggTradeData(messageString) { /* ... */ }

// --- Binance Stream Connection ---
function connectToBinance() {
    // ... (connection setup - remains the same) ...
    if (binanceWsClient && (binanceWsClient.readyState === WebSocket.OPEN || binanceWsClient.readyState === WebSocket.CONNECTING)) { return; }
    console.log(`[Listener] PID: ${process.pid} --- Connecting to Binance: ${BINANCE_STREAM_URL}`);
    binanceWsClient = new WebSocket(BINANCE_STREAM_URL);
    binanceWsClient.on('open', function open() { /* ... */ });


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
                                let canSendSignal = true;

                                if (lastSentSignalType === currentSignalType) { // Rule: Not a consecutive identical signal
                                    canSendSignal = false;
                                } else if (lastSentSignalType === "SELL" && priceAtLastSellSignal !== null) {
                                    // Rule: If last was SELL, new BUY must be above sell price + threshold
                                    if (currentPrice <= priceAtLastSellSignal + SIGNAL_REVERSAL_THRESHOLD) {
                                        canSendSignal = false;
                                        // Optional: Log why it's suppressed
                                        // console.log(`[SIG] Suppressed BUY: Price ${currentPrice} not > ${priceAtLastSellSignal} + ${SIGNAL_REVERSAL_THRESHOLD}`);
                                    }
                                }

                                if (canSendSignal) {
                                    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                                        const signalMsg = { o: "binance", p: extractedBinanceData.p, signal: currentSignalType };
                                        console.log(`[SIG] --> Sending TRUE TICK [${currentSignalType}]. Binance: ${lastBinanceAggTradePrice_Direction.toFixed(2)}->${currentPrice.toFixed(2)}, Bybit Bid: ${prevBybitBidAtLastBinanceIncrease.toFixed(2)}->${latestBybitBestBid.toFixed(2)}`);
                                        try {
                                            internalWsClient.send(JSON.stringify(signalMsg));
                                            lastSentSignalType = currentSignalType;
                                            priceAtLastBuySignal = currentPrice; // Store price of this BUY
                                            // Reset opposite signal price to allow immediate flip if conditions met next
                                            priceAtLastSellSignal = null; 
                                            if (latestBybitBestBid !== null) { // Original baseline adjustment from previous iteration
                                                prevBybitBidAtLastBinanceDecrease = latestBybitBestBid;
                                            }
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
                                let canSendSignal = true;

                                if (lastSentSignalType === currentSignalType) { // Rule: Not a consecutive identical signal
                                    canSendSignal = false;
                                } else if (lastSentSignalType === "BUY" && priceAtLastBuySignal !== null) {
                                    // Rule: If last was BUY, new SELL must be below buy price - threshold
                                    if (currentPrice >= priceAtLastBuySignal - SIGNAL_REVERSAL_THRESHOLD) {
                                        canSendSignal = false;
                                        // Optional: Log why it's suppressed
                                        // console.log(`[SIG] Suppressed SELL: Price ${currentPrice} not < ${priceAtLastBuySignal} - ${SIGNAL_REVERSAL_THRESHOLD}`);
                                    }
                                }
                                
                                if (canSendSignal) {
                                    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                                        const signalMsg = { o: "binance", p: extractedBinanceData.p, signal: currentSignalType };
                                        console.log(`[SIG] --> Sending TRUE TICK [${currentSignalType}]. Binance: ${lastBinanceAggTradePrice_Direction.toFixed(2)}->${currentPrice.toFixed(2)}, Bybit Bid: ${prevBybitBidAtLastBinanceDecrease.toFixed(2)}->${latestBybitBestBid.toFixed(2)}`);
                                        try {
                                            internalWsClient.send(JSON.stringify(signalMsg));
                                            lastSentSignalType = currentSignalType;
                                            priceAtLastSellSignal = currentPrice; // Store price of this SELL
                                            // Reset opposite signal price to allow immediate flip if conditions met next
                                            priceAtLastBuySignal = null;
                                            if (latestBybitBestBid !== null) { // Original baseline adjustment
                                                prevBybitBidAtLastBinanceIncrease = latestBybitBestBid;
                                            }
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

                // ... (General Price Update Sending - remains the same) ...
                 let shouldSendData = false;
                if (lastSentBinancePrice === null) shouldSendData = true;
                else { /* ... */ if (Math.abs(currentPrice - lastSentBinancePrice) >= PRICE_CHANGE_THRESHOLD_BINANCE) shouldSendData = true; }
                if (shouldSendData) { /* ... send general update ... */ }

            } else { 
                // ... (handle non-aggTrade messages - remains the same) ...
            }
        } catch (e) {
            console.error(`[Listener] PID: ${process.pid} --- Binance: CRITICAL ERROR in message handler:`, e.message, e.stack);
        }
    });
    
    // ... (binanceWsClient.on('error'...) and .on('close'...) remain the same) ...
    binanceWsClient.on('error', function error(err) { /* ... */ });
    binanceWsClient.on('close', function close(code, reason) { /* ... */ setTimeout(connectToBinance, RECONNECT_INTERVAL_MS); });
}

// ... (Bybit connection and message handling - largely remains the same, as it's for confirmation and general updates) ...
function connectToBybit() { /* ... */ }


// --- Start the connections ---
console.log(`[Listener] PID: ${process.pid} --- Listener starting...`);
console.log(`[Listener] Binance AggTrade: ${BINANCE_STREAM_URL}, Bybit Orderbook: ${BYBIT_SPOT_STREAM_URL}`);
console.log(`[Listener] General Price Update Thresholds: Binance=${PRICE_CHANGE_THRESHOLD_BINANCE}, Bybit=${PRICE_CHANGE_THRESHOLD_BYBIT}`);
console.log(`[Listener] Signal Reversal Threshold: ${SIGNAL_REVERSAL_THRESHOLD} units.`); // NEW Log
console.log(`[Listener] Ticker Direction signal logic enabled. Client payload: {o, p, signal?}. Consecutive identical signals suppressed.`);
console.log(`[Listener] General price updates client payload: {o, p}.`);

connectToInternalReceiver();
connectToBinance();
connectToBybit();