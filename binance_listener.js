/**
 * @file binance_listener.js
 * @purpose A predictive leading indicator for short-term price movements on Binance Spot.
 * @philosophy This script operates on the principle that quantity behavior at stable price levels
 *             precedes and predicts price movement. It is intentionally NOT reactive to price ticks.
 *             Instead, it analyzes the setup (the cause) before the price move (the effect).
 *             It generates two types of messages for the client:
 *             1. Predictive signals ('PRED') based on a cumulative score of market microstructure events.
 *             2. Immediate price updates ('p') whenever the best bid price changes.
 */

const WebSocket = require('ws');

// --- Global Error Handlers ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`);
    console.error(err.stack || err);
    cleanupAndExit(1);
});
process.on('unhandledRejection', (reason, promise) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNHANDLED PROMISE REJECTION`);
    console.error('[Listener] Unhandled Rejection at:', promise);
    console.error('[Listener] Reason:', reason instanceof Error ? reason.stack : reason);
    cleanupAndExit(1);
});

// --- State Management ---
function cleanupAndExit(exitCode = 1) {
    const clientsToTerminate = [internalWsClient, spotWsClient];
    console.error('[Listener] Initiating cleanup...');
    clientsToTerminate.forEach(client => {
        if (client && (client.readyState === WebSocket.OPEN || client.readyState === WebSocket.CONNECTING)) {
            try { client.terminate(); } catch (e) { console.error(`[Listener] Error during WebSocket termination: ${e.message}`); }
        }
    });
    setTimeout(() => {
        console.error(`[Listener] Exiting with code ${exitCode}.`);
        process.exit(exitCode);
    }, 1000).unref();
}

// --- Listener Configuration ---
const SYMBOL = 'btcusdt';
const RECONNECT_INTERVAL_MS = 5000;

// --- Prediction Model Configuration ---
const N_WINDOW_SIZE = 40;
// "Imbalance Dominator" Weights. Bullish: 60%, Bearish: 40%.
// This model heavily prioritizes order book imbalance as the main driver for signals.
const ALPHA = 4.0,   // Imbalance (Represents 50% of influence)
      BETA = 1.0,    // Absorption (Represents 7.5% of influence)
      GAMMA = 8.0,   // Disappearance Risk (Weight for normalized 0-1 score). Ask risk is bullish, Bid risk is bearish.
      DELTA = 2.0,   // Stacking (Represents 10% of influence)
      EPSILON = 2.5; // Sequential Rug Pull (Represents 25% of influence)
const FUZZY_MATCH_PERCENTAGE = 0.15;
const RUG_PULL_WINDOW_TICKS = 5;
const RUG_PULL_SPIKE_THRESHOLD = 8.0;

// --- Combined Score Configuration ---
const CUMULATIVE_SCORE_THRESHOLD_BUY = 70.0;  // Increased threshold to reduce signal frequency
const CUMULATIVE_SCORE_THRESHOLD_SELL = -70.0; // Increased threshold to reduce signal frequency
const SMOOTHING_FACTOR = 0.1; // (Range 0-1) How much weight to give the new score. Lower = smoother.

// --- Connection URLs ---
const internalReceiverUrl = 'ws://localhost:8082';
const SPOT_STREAM_URL = `wss://stream.binance.com:9443/ws/${SYMBOL}@bookTicker`;

// --- Listener State Variables ---
let internalWsClient, spotWsClient;
let tickHistory = [];
let askLevelWatch = null, bidLevelWatch = null;
let askSpikeWatch = null, bidSpikeWatch = null;
let cumulativeScore = 0;
let lastBestBidPrice = null;

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) return;
    internalWsClient = new WebSocket(internalReceiverUrl);
    internalWsClient.on('error', (err) => console.error(`[Internal] WebSocket error: ${err.message}`));
    internalWsClient.on('close', () => {
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
}

// --- Data Forwarding ---
function sendToInternalClient(payload) {
    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        try {
            internalWsClient.send(JSON.stringify(payload));
        } catch (e) { console.error(`[Internal] Failed to send message: ${e.message}`); }
    }
}

// --- Score Calculation Functions ---
function calculateAvgImbalance() {
    if (tickHistory.length === 0) return 0;
    const totalImbalance = tickHistory.reduce((sum, tick) => sum + tick.imbalance, 0);
    return totalImbalance / tickHistory.length;
}

function calculateAbsorptionScore() {
    if (tickHistory.length === 0) return 0;
    const totalReductions = tickHistory.filter(t => t.isReduction).length;
    const fuzzyRefills = tickHistory.filter(t => t.isRefill).length;
    return totalReductions > 0 ? fuzzyRefills / totalReductions : 0;
}

// *** FIXED FUNCTION ***
function calculateDisappearanceRisk(currentTick) {
    if (tickHistory.length < 2) return { bidRisk: 0, askRisk: 0 }; // Not enough data

    // Find the max quantities within the lookback window
    const maxBidQty = Math.max(...tickHistory.map(t => t.bidQty));
    const maxAskQty = Math.max(...tickHistory.map(t => t.askQty));

    // Calculate the percentage of liquidity that has disappeared from the peak
    // This value is now stable and bounded (0 to 1)
    const bidRisk = maxBidQty > 0 ? (maxBidQty - currentTick.bidQty) / maxBidQty : 0;
    const askRisk = maxAskQty > 0 ? (maxAskQty - currentTick.askQty) / maxAskQty : 0;
    
    // Return both risks so we can treat them differently.
    // Disappearance on the ask side is BULLISH (removes sellers).
    // Disappearance on the bid side is BEARISH (removes buyers).
    return { bidRisk, askRisk };
}

function calculateSequentialRugPullScore() {
    if (tickHistory.length === 0) return 0;
    const pullEvents = tickHistory.filter(t => t.isRugPull).length;
    return pullEvents > 0 ? 1 : 0;
}

function calculateStackingScore() {
    if (tickHistory.length === 0) return 0;
    const bidStacks = tickHistory.filter(t => t.isBidStack).length;
    const askStacks = tickHistory.filter(t => t.isAskStack).length;
    return (bidStacks - askStacks) / tickHistory.length;
}

// --- Spot Exchange Connection (Main Logic Engine) ---
function connectToSpot() {
    spotWsClient = new WebSocket(SPOT_STREAM_URL);
    spotWsClient.on('open', () => {
        console.log('[Spot] Connection opened. Analyzing predictive quantity behavior...');
        tickHistory = [];
        askLevelWatch = null; bidLevelWatch = null;
        askSpikeWatch = null; bidSpikeWatch = null;
        cumulativeScore = 0;
        lastBestBidPrice = null;
    });

    spotWsClient.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());
            const currentBestBidPrice = parseFloat(message.b);

            // --- IMMEDIATE PRICE CHANGE LOGIC ---
            // Check if the best bid price has changed since the last tick.
            if (lastBestBidPrice !== null && currentBestBidPrice !== lastBestBidPrice) {
                // Immediately send the new price to the client with key "p".
                sendToInternalClient({ p: currentBestBidPrice });
            }
            // Always update the last known price for the next comparison.
            lastBestBidPrice = currentBestBidPrice;

            // --- PREDICTIVE MODEL LOGIC ---
            const prevTick = tickHistory.length > 0 ? tickHistory[tickHistory.length - 1] : null;

            const currentTick = {
                bidPrice: currentBestBidPrice,
                bidQty: parseFloat(message.B),
                askPrice: parseFloat(message.a),
                askQty: parseFloat(message.A),
                imbalance: 0, isReduction: false, isRefill: false,
                isBidStack: false, isAskStack: false, isRugPull: false,
            };

            const totalQty = currentTick.bidQty + currentTick.askQty;
            currentTick.imbalance = totalQty > 0 ? (currentTick.bidQty - currentTick.askQty) / totalQty : 0;
            
            if (prevTick) {
                // Absorption Logic
                if (askLevelWatch && currentTick.askPrice !== askLevelWatch.price) askLevelWatch = null;
                if (bidLevelWatch && currentTick.bidPrice !== bidLevelWatch.price) bidLevelWatch = null;
                if (askLevelWatch && Math.abs(currentTick.askQty - askLevelWatch.originalQty) <= askLevelWatch.originalQty * FUZZY_MATCH_PERCENTAGE) {
                    currentTick.isRefill = true; askLevelWatch = null;
                }
                if (bidLevelWatch && Math.abs(currentTick.bidQty - bidLevelWatch.originalQty) <= bidLevelWatch.originalQty * FUZZY_MATCH_PERCENTAGE) {
                    currentTick.isRefill = true; bidLevelWatch = null;
                }
                if (!askLevelWatch && currentTick.askPrice === prevTick.askPrice && currentTick.askQty < prevTick.askQty) {
                    currentTick.isReduction = true; askLevelWatch = { price: currentTick.askPrice, originalQty: prevTick.askQty };
                }
                if (!bidLevelWatch && currentTick.bidPrice === prevTick.bidPrice && currentTick.bidQty < prevTick.bidQty) {
                    currentTick.isReduction = true; bidLevelWatch = { price: currentTick.bidPrice, originalQty: prevTick.bidQty };
                }

                // Sequential Rug Pull Logic
                if (askSpikeWatch) {
                    askSpikeWatch.ticksLeft--;
                    if (askSpikeWatch.ticksLeft <= 0 || currentTick.askPrice !== askSpikeWatch.price) askSpikeWatch = null;
                }
                if (bidSpikeWatch) {
                    bidSpikeWatch.ticksLeft--;
                    if (bidSpikeWatch.ticksLeft <= 0 || currentTick.bidPrice !== bidSpikeWatch.price) bidSpikeWatch = null;
                }
                if (askSpikeWatch && currentTick.askQty <= askSpikeWatch.preSpikeQty * 1.25) {
                    currentTick.isRugPull = true; askSpikeWatch = null;
                }
                if (bidSpikeWatch && currentTick.bidQty <= bidSpikeWatch.preSpikeQty * 1.25) {
                    currentTick.isRugPull = true; bidSpikeWatch = null;
                }
                if (!askSpikeWatch && currentTick.askPrice === prevTick.askPrice && currentTick.askQty > prevTick.askQty * RUG_PULL_SPIKE_THRESHOLD) {
                    askSpikeWatch = { price: currentTick.askPrice, preSpikeQty: prevTick.askQty, ticksLeft: RUG_PULL_WINDOW_TICKS };
                }
                if (!bidSpikeWatch && currentTick.bidPrice === prevTick.bidPrice && currentTick.bidQty > prevTick.bidQty * RUG_PULL_SPIKE_THRESHOLD) {
                    bidSpikeWatch = { price: currentTick.bidPrice, preSpikeQty: prevTick.bidQty, ticksLeft: RUG_PULL_WINDOW_TICKS };
                }

                // Stacking Logic
                if (currentTick.bidPrice === prevTick.bidPrice && currentTick.bidQty > prevTick.bidQty) currentTick.isBidStack = true;
                if (currentTick.askPrice === prevTick.askPrice && currentTick.askQty > prevTick.askQty) currentTick.isAskStack = true;
            }
            
            tickHistory.push(currentTick);
            if (tickHistory.length > N_WINDOW_SIZE) tickHistory.shift();
            if (tickHistory.length < N_WINDOW_SIZE) return;

            // *** FIXED SCORING LOGIC ***
            const avgImbalance = calculateAvgImbalance();
            const absorptionScore = calculateAbsorptionScore();
            const { bidRisk, askRisk } = calculateDisappearanceRisk(currentTick);
            const sequentialRugPullScore = calculateSequentialRugPullScore();
            const stackingScore = calculateStackingScore();

            // --- REVISED SCORING FORMULA ---
            // Ask-side risk (sellers disappearing) is BULLISH, so we ADD it.
            // Bid-side risk (buyers disappearing) is BEARISH, so we SUBTRACT it.
            const realTickPredictScore = 
                (ALPHA * avgImbalance)             // Bullish if bids > asks
              + (GAMMA * askRisk)                  // Bullish if sellers disappear
              - (GAMMA * bidRisk)                  // Bearish if buyers disappear
              - (BETA * absorptionScore)           // Bearish (cautious interpretation)
              - (EPSILON * sequentialRugPullScore) // Bearish if rug pulls detected
              + (DELTA * stackingScore);           // Bullish if bids are stacking
            
            // Update the cumulative score using an EMA formula for smoothing
            cumulativeScore = (realTickPredictScore * SMOOTHING_FACTOR) + (cumulativeScore * (1 - SMOOTHING_FACTOR));

            let decision = 'HOLD';
            if (cumulativeScore > CUMULATIVE_SCORE_THRESHOLD_BUY) {
                decision = 'BUY';
            } else if (cumulativeScore < CUMULATIVE_SCORE_THRESHOLD_SELL) {
                decision = 'SELL';
            }
            
            if (decision !== 'HOLD') {
                // *** FIXED LOGGING AND PAYLOAD ***
                console.log(`[Signal] ${decision} | C.Score: ${cumulativeScore.toFixed(3)} | Tick Score: ${realTickPredictScore.toFixed(3)} (Imb:${avgImbalance.toFixed(2)}, AskRsk:${askRisk.toFixed(2)}, BidRsk:${bidRisk.toFixed(2)}, Rug:${sequentialRugPullScore}, Stk:${stackingScore.toFixed(2)})`);
                
                const payload = {
                    type: "PRED",
                    signal: decision,
                    score: parseFloat(cumulativeScore.toFixed(4)),
                    detail: {
                        tick_score: parseFloat(realTickPredictScore.toFixed(4)),
                        imb: parseFloat(avgImbalance.toFixed(2)),
                        abs: parseFloat(absorptionScore.toFixed(2)),
                        ask_risk: parseFloat(askRisk.toFixed(2)),
                        bid_risk: parseFloat(bidRisk.toFixed(2)),
                        rug: sequentialRugPullScore,
                        stack: parseFloat(stackingScore.toFixed(2))
                    }
                };
                sendToInternalClient(payload);

                // *** CRITICAL FIX: REMOVED SCORE RESET ***
                // The score is no longer reset to zero. This allows it to track
                // sustained pressure and avoid reacting to single, noisy ticks.
                // cumulativeScore = 0; 
            }

        } catch (e) { console.error(`[Spot] Error processing message: ${e.message}, Data: ${data}`); }
    });

    spotWsClient.on('error', (err) => console.error('[Spot] Connection error:', err.message));
    spotWsClient.on('close', () => {
        spotWsClient = null;
        console.log(`[Spot] Connection closed. Reconnecting in ${RECONNECT_INTERVAL_MS / 1000}s...`);
        setTimeout(connectToSpot, RECONNECT_INTERVAL_MS);
    });
}

// --- Start all connections ---
console.log(`[Listener] Starting Predictive Quantity Engine.`);
console.log(`-- Config: Window Size=${N_WINDOW_SIZE}, Cumulative Signal Threshold=±${CUMULATIVE_SCORE_THRESHOLD_BUY}`);
console.log(`-- Config: Weights (α,β,γ,δ,ε)=(${ALPHA}, ${BETA}, ${GAMMA}, ${DELTA}, ${EPSILON})`);
connectToInternalReceiver();
connectToSpot();
