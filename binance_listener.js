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
const N_WINDOW_SIZE = 15;
const ALPHA = 1.0, BETA = 1.5, GAMMA = 2.0, DELTA = 1.2, EPSILON = 2.5; // ALPHA [ Imbalance],BETA [ Absorption Weight ] ,GAMMA  (Disappearance Risk Weight),DELTA (Stacking Weight),EPSILON (Sequential Rug )
const FUZZY_MATCH_PERCENTAGE = 0.15;
const RUG_PULL_WINDOW_TICKS = 5;
const RUG_PULL_SPIKE_THRESHOLD = 3.0;

// --- Combined Score Configuration ---
const CUMULATIVE_SCORE_THRESHOLD_BUY = 1.0;  // Threshold for the combined score to trigger a BUY
const CUMULATIVE_SCORE_THRESHOLD_SELL = -1.0; // Threshold for the combined score to trigger a SELL
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

function calculateDisappearanceRisk(currentTick) {
    if (tickHistory.length === 0) return 0;
    const maxBidQty = Math.max(...tickHistory.map(t => t.bidQty));
    const maxAskQty = Math.max(...tickHistory.map(t => t.askQty));
    const disappearanceRateBid = currentTick.bidQty > 0 ? (maxBidQty / currentTick.bidQty) - 1 : Infinity;
    const disappearanceRateAsk = currentTick.askQty > 0 ? (maxAskQty / currentTick.askQty) - 1 : Infinity;
    return Math.max(disappearanceRateBid, disappearanceRateAsk);
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

            const avgImbalance = calculateAvgImbalance();
            const absorptionScore = calculateAbsorptionScore();
            const disappearanceRisk = calculateDisappearanceRisk(currentTick);
            const sequentialRugPullScore = calculateSequentialRugPullScore();
            const stackingScore = calculateStackingScore();

            const realTickPredictScore = (ALPHA * avgImbalance) - (BETA * absorptionScore) - (GAMMA * disappearanceRisk) - (EPSILON * sequentialRugPullScore) + (DELTA * stackingScore);
            
            // Update the cumulative score using an EMA formula
            cumulativeScore = (realTickPredictScore * SMOOTHING_FACTOR) + (cumulativeScore * (1 - SMOOTHING_FACTOR));

            let decision = 'HOLD';
            if (cumulativeScore > CUMULATIVE_SCORE_THRESHOLD_BUY) {
                decision = 'BUY';
            } else if (cumulativeScore < CUMULATIVE_SCORE_THRESHOLD_SELL) {
                decision = 'SELL';
            }
            
            if (decision !== 'HOLD') {
                console.log(`[Signal] ${decision} | C.Score: ${cumulativeScore.toFixed(3)} | Tick Score: ${realTickPredictScore.toFixed(3)} (Imb:${avgImbalance.toFixed(2)}, Abs:${absorptionScore.toFixed(2)}, Risk:${disappearanceRisk.toFixed(2)}, Rug:${sequentialRugPullScore}, Stk:${stackingScore.toFixed(2)})`);
                
                const payload = {
                    type: "PRED",
                    signal: decision,
                    score: parseFloat(cumulativeScore.toFixed(4)),
                    detail: {
                        tick_score: parseFloat(realTickPredictScore.toFixed(4)),
                        imb: parseFloat(avgImbalance.toFixed(2)),
                        abs: parseFloat(absorptionScore.toFixed(2)),
                        risk: parseFloat(disappearanceRisk.toFixed(2)),
                        rug: sequentialRugPullScore,
                        stack: parseFloat(stackingScore.toFixed(2))
                    }
                };
                sendToInternalClient(payload);

                // Reset the cumulative score after a signal is sent to begin accumulating fresh pressure
                cumulativeScore = 0; 
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
