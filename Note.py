============================================================
      HFT Signal Gen v10.10 (Corrected & Cleaned)      
============================================================

Core Triggers:
 - 'MAX_SHOCK': 100th percentile trade size (bypasses confirmation)
 - 'SHOCK': > 99.98th percentile trade size
 - 'STEALTH': Using 'Forgiving Streak' detection

Confirmation Logic (for standard signals):
 - TFI Threshold: ADAPTIVE (> 3.75 std devs)
 - Price Impact: Trade must move price in its favor (> 0.8)

Context Layer: REGIME ANALYSIS
 - Multi-Timeframe Volume Delta (3m, 5m, 15m) determines market regime.
 - Logs 'Conviction %' for trends or 'Divergence %' for neutral markets.

Meta-Layer: ORDER PUNCH LOGIC
 - Verification: Net flow of next 17 trades in signal direction.
 - <<< CONVICTION ANOMALY BYPASS (FIXED) >>>
   - If a signal's conviction is > the 95.0th percentile value of the last 25 signals,
     it BYPASSES the Order Punch verification for immediate action.
   - NOTE: Logic now compares to a threshold value, not a strict rank, which is more robust.

<<< PERFORMANCE TRACKING >>>
 - Take Profit: 0.06% | Stop Loss: 0.01% | Timeout: 120s
 - Reporting metrics every 25 signals.
------------------------------------------------------------
Connecting to wss://fstream.binance.com/stream?streams=btcusdt@bookTicker/btcusdt@trade...
HFT Logic Warm-up will last for 4.0 minutes.
Connection successful!

--- WARM-UP COMPLETE at Wed Oct 15 15:26:33 2025. HFT engine online. ---
--- Conviction Anomaly Detector is now populating its history... ---

15:26:36 [CONV_DEBUG] New: 57.8% | Rank: 0.0% | HistSize: 1/25 | HistMax: 57.8%
15:26:36 | Mid:111254.95 | StrL:137 | Strength:137.38 | [3m:60.8% 5m:56.3% 15m:56.3%] -> BULLISH (Conviction: 57.8%) | > PULSE (FORGIVING_BUY)
15:26:42 [CONV_DEBUG] New: 58.5% | Rank: 100.0% | HistSize: 2/25 | HistMax: 58.5%
15:26:42 | Mid:111296.55 | StrL:302 | Strength:302.50 | [3m:61.5% 5m:57.0% 15m:57.0%] -> BULLISH (Conviction: 58.5%) | > PULSE (FORGIVING_BUY)
15:26:42 | CLUSTER DETECTED (BUY 302.50)... PENDING VERIFICATION...
15:26:42 | Mid:111296.55 | Flow:17 | Strength:302.50 | Trend Following | >>> ORDER PUNCH: BUY VERIFIED! (FORGIVING_BUY)
15:26:43 [CONV_DEBUG] New: 59.7% | Rank: 100.0% | HistSize: 3/25 | HistMax: 59.7%
15:26:43 | Mid:111409.45 | StrL:15 | Strength:15.40 | [3m:62.5% 5m:58.2% 15m:58.2%] -> BULLISH (Conviction: 59.7%) | > PULSE (FORGIVING_BUY)
15:26:43 | CLUSTER DETECTED (BUY 15.40)... PENDING VERIFICATION...
15:26:43 | Mid:111409.45 | Flow:17 | Strength:15.40 | Trend Following | >>> ORDER PUNCH: BUY VERIFIED! (FORGIVING_BUY)
15:26:46 [CONV_DEBUG] New: 57.7% | Rank: 0.0% | HistSize: 4/25 | HistMax: 59.7%
15:26:46 | Mid:111403.85 | StrL:17 | Strength:17.09 | [3m:60.4% 5m:56.4% 15m:56.4%] -> BULLISH (Conviction: 57.7%) | > PULSE (FORGIVING_SELL)
15:26:52 [CONV_DEBUG] New: 53.8% | Rank: 0.0% | HistSize: 5/25 | HistMax: 59.7%
15:26:52 | Mid:111306.95 | StrL:181 | Strength:181.88 | [3m:56.2% 5m:52.6% 15m:52.6%] -> BULLISH (Conviction: 53.8%) | > PULSE (FORGIVING_BUY)
15:26:59 [CONV_DEBUG] New: 55.1% | Rank: 20.0% | HistSize: 6/25 | HistMax: 59.7%
15:26:59 | Mid:111373.45 | StrL:171 | Strength:171.51 | [3m:58.1% 5m:53.7% 15m:53.7%] -> BULLISH (Conviction: 55.1%) | > PULSE (FORGIVING_BUY)
15:26:59 | CLUSTER DETECTED (BUY 171.51)... PENDING VERIFICATION...
15:26:59 | Mid:111373.45 | Flow:17 | Strength:171.51 | Trend Following | >>> ORDER PUNCH: BUY VERIFIED! (FORGIVING_BUY)
15:27:06 [CONV_DEBUG] New: 54.5% | Rank: 16.7% | HistSize: 7/25 | HistMax: 59.7%
15:27:06 | Mid:111433.65 | StrL:28 | Strength:28.33 | [3m:57.7% 5m:52.9% 15m:52.9%] -> BULLISH (Conviction: 54.5%) | > PULSE (FORGIVING_BUY)
15:27:06 | CLUSTER DETECTED (BUY 28.33)... PENDING VERIFICATION...
15:27:06 | Mid:111433.65 | Flow:17 | Strength:28.33 | Trend Following | >>> ORDER PUNCH: BUY VERIFIED! (FORGIVING_BUY)
15:27:08 [CONV_DEBUG] New: 54.9% | Rank: 28.6% | HistSize: 8/25 | HistMax: 59.7%
15:27:08 | Mid:111432.65 | StrL:155 | Strength:155.91 | [3m:58.1% 5m:53.2% 15m:53.2%] -> BULLISH (Conviction: 54.9%) | > PULSE (FORGIVING_BUY)
15:27:08 | CLUSTER DETECTED (BUY 155.91)... PENDING VERIFICATION...
15:27:08 | Mid:111432.65 | Flow:17 | Strength:155.91 | Trend Following | >>> ORDER PUNCH: BUY VERIFIED! (FORGIVING_BUY)
15:27:16 [CONV_DEBUG] New: 55.3% | Rank: 50.0% | HistSize: 9/25 | HistMax: 59.7%
15:27:16 | Mid:111491.55 | StrL:14 | Strength:14.55 | [3m:58.4% 5m:53.8% 15m:53.8%] -> BULLISH (Conviction: 55.3%) | > PULSE (FORGIVING_BUY)
15:27:16 | CLUSTER DETECTED (BUY 14.55)... PENDING VERIFICATION...
15:27:16 | Mid:111491.55 | Flow:17 | Strength:14.55 | COUNTER-TREND | >>> ORDER PUNCH: BUY VERIFIED! (FORGIVING_BUY)
15:27:20 [CONV_DEBUG] New: 55.1% | Rank: 33.3% | HistSize: 10/25 | HistMax: 59.7%
15:27:20 | Mid:111483.20 | StrL:41 | Strength:41.44 | [3m:58.1% 5m:53.6% 15m:53.6%] -> BULLISH (Conviction: 55.1%) | > PULSE (FORGIVING_SELL)
15:27:24 [CONV_DEBUG] New: 55.0% | Rank: 30.0% | HistSize: 11/25 | HistMax: 59.7%
15:27:24 | Mid:111475.95 | StrL:40 | Strength:40.68 | [3m:58.1% 5m:53.4% 15m:53.4%] -> BULLISH (Conviction: 55.0%) | > PULSE (FORGIVING_SELL)
15:27:24 | CLUSTER DETECTED (SELL 40.68)... PENDING VERIFICATION...
15:27:25 | Mid:111472.95 | Flow:-15 | Strength:40.68 | Trend Following | >>> ORDER PUNCH: SELL VERIFIED! (FORGIVING_SELL)
15:27:27 [CONV_DEBUG] New: 54.3% | Rank: 9.1% | HistSize: 12/25 | HistMax: 59.7%
15:27:27 | Mid:111442.05 | StrL:64 | Strength:64.27 | [3m:57.3% 5m:52.8% 15m:52.8%] -> BULLISH (Conviction: 54.3%) | > PULSE (FORGIVING_SELL)
15:27:27 | CLUSTER DETECTED (SELL 64.27)... PENDING VERIFICATION...
15:27:27 | Mid:111435.15 | Flow:-13 | Strength:64.27 | Trend Following | >>> ORDER PUNCH: SELL VERIFIED! (FORGIVING_SELL)
15:27:49 [CONV_DEBUG] New: 48.8% | Rank: 0.0% | HistSize: 13/25 | HistMax: 59.7%
15:27:49 | Mid:111307.05 | Sz%:100.0 | Strength:50.00 | [3m:51.7% 5m:47.3% 15m:47.4%] -> BULLISH (Conviction: 48.8%) | > PULSE (MAX_SHOCK_SELL)
[CONV_DEBUG] Detector is now READY. History size: 13
15:27:52 [CONV_DEBUG] New: 44.5% | Rank: 0.0% | HistSize: 14/25 | HistMax: 59.7%
15:27:52 | Mid:111269.90 | Sz%:100.0 | Strength:50.00 | [3m:47.3% 5m:43.0% 15m:43.3%] -> BULLISH (Conviction: 44.5%) | > PULSE (MAX_SHOCK_SELL)
15:27:52 | CLUSTER DETECTED (SELL 50.00)... PENDING VERIFICATION...
15:27:52 | Mid:111269.90 | Flow:-17 | Strength:50.00 | Trend Following | >>> ORDER PUNCH: SELL VERIFIED! (MAX_SHOCK_SELL)
15:28:13 [CONV_DEBUG] New: 43.4% | Rank: 0.0% | HistSize: 15/25 | HistMax: 59.7%
15:28:13 | Mid:111232.80 | StrL:71 | Strength:71.92 | [3m:45.7% 5m:42.7% 15m:41.8%] -> BULLISH (Conviction: 43.4%) | > PULSE (FORGIVING_SELL)
15:29:05 [CONV_DEBUG] New: 33.6% | Rank: 0.0% | HistSize: 16/25 | HistMax: 59.7%
15:29:05 | Mid:111128.25 | StrL:29 | Strength:29.74 | [3m:23.1% 5m:40.0% 15m:37.7%] -> BULLISH (Conviction: 33.6%) | > PULSE (FORGIVING_BUY)
15:29:29 [CONV_DEBUG] New: 28.6% | Rank: 0.0% | HistSize: 17/25 | HistMax: 59.7%
15:29:29 | Mid:111084.05 | StrL:16 | Strength:16.51 | [3m:10.2% 5m:39.0% 15m:36.4%] -> BULLISH (Conviction: 28.6%) | > PULSE (FORGIVING_BUY)
15:29:58 [CONV_DEBUG] New: 37.9% | Rank: 11.8% | HistSize: 18/25 | HistMax: 59.7%
15:29:58 | Mid:111112.40 | StrL:136 | Strength:136.05 | [3m:-2.2% 5m:39.4% 15m:36.5%] -> BULLISH (Conviction: 37.9%) | > PULSE (FORGIVING_BUY)
15:30:14 [CONV_DEBUG] New: 38.0% | Rank: 16.7% | HistSize: 19/25 | HistMax: 59.7%
15:30:14 | Mid:111151.40 | StrL:85 | Strength:85.91 | [3m:-20.5% 5m:39.5% 15m:36.5%] -> BULLISH (Conviction: 38.0%) | > PULSE (FORGIVING_BUY)
15:30:14 | CLUSTER DETECTED (BUY 85.91)... PENDING VERIFICATION...
15:30:14 | Mid:111154.15 | Flow:17 | Strength:85.91 | Trend Following | >>> ORDER PUNCH: BUY VERIFIED! (FORGIVING_BUY)
15:30:18 [CONV_DEBUG] New: 37.3% | Rank: 10.5% | HistSize: 20/25 | HistMax: 59.7%
15:30:18 | Mid:111196.55 | StrL:147 | Strength:147.16 | [3m:-31.6% 5m:37.8% 15m:36.8%] -> BULLISH (Conviction: 37.3%) | > PULSE (FORGIVING_BUY)
15:30:18 | CLUSTER DETECTED (BUY 147.16)... PENDING VERIFICATION...
15:30:18 | Mid:111196.55 | Flow:17 | Strength:147.16 | Trend Following | >>> ORDER PUNCH: BUY VERIFIED! (FORGIVING_BUY)
15:30:48 [CONV_DEBUG] New: 32.6% | Rank: 5.0% | HistSize: 21/25 | HistMax: 59.7%
15:30:48 | Mid:111187.15 | StrL:48 | Strength:48.59 | [3m:-22.0% 5m:28.7% 15m:36.5%] -> BULLISH (Conviction: 32.6%) | > PULSE (FORGIVING_BUY)
15:31:01 [CONV_DEBUG] New: 30.5% | Rank: 4.8% | HistSize: 22/25 | HistMax: 59.7%
15:31:01 | Mid:111187.15 | Sz%:100.0 | Strength:50.00 | [3m:-1.1% 5m:24.6% 15m:36.3%] -> BULLISH (Conviction: 30.5%) | > PULSE (MAX_SHOCK_SELL)
15:31:06 [CONV_DEBUG] New: 29.6% | Rank: 4.5% | HistSize: 23/25 | HistMax: 59.7%
15:31:06 | Mid:111233.35 | StrL:63 | Strength:63.47 | [3m:-0.0% 5m:22.8% 15m:36.4%] -> BULLISH (Conviction: 29.6%) | > PULSE (FORGIVING_BUY)
15:31:09 [CONV_DEBUG] New: 29.2% | Rank: 4.3% | HistSize: 24/25 | HistMax: 59.7%
15:31:09 | Mid:111278.25 | StrL:122 | Strength:122.92 | [3m:4.2% 5m:21.7% 15m:36.8%] -> BULLISH (Conviction: 29.2%) | > PULSE (FORGIVING_BUY)
15:31:09 | CLUSTER DETECTED (BUY 122.92)... PENDING VERIFICATION...
15:31:09 | Mid:111278.25 | Flow:17 | Strength:122.92 | Trend Following | >>> ORDER PUNCH: BUY VERIFIED! (FORGIVING_BUY)
15:31:11 [CONV_DEBUG] New: 23.9% | Rank: 0.0% | HistSize: 25/25 | HistMax: 59.7%
15:31:11 | Mid:111312.40 | StrL:133 | Strength:133.75 | [3m:11.2% 5m:23.1% 15m:37.4%] -> BULLISH (Conviction: 23.9%) | > PULSE (FORGIVING_BUY)

================================================================================
PERFORMANCE REPORT @ 25 SIGNALS (Time: Wed Oct 15 15:31:11 2025)
================================================================================
Signal Reason                       |  Count |   Hit Rate |  Avg PnL % |  Total PnL %
--------------------------------------------------------------------------------
FORGIVING_BUY                       |     15 |     60.00% |    0.0328% |      0.4920%
FORGIVING_SELL                      |      5 |     60.00% |    0.0328% |      0.1640%
MAX_SHOCK_SELL                      |      3 |     66.67% |    0.0373% |      0.1120%
--------------------------------------------------------------------------------
OVERALL                             |     23 |     60.87% |    0.0334% |      0.7680%
================================================================================

15:31:11 | CLUSTER DETECTED (BUY 133.75)... PENDING VERIFICATION...
15:31:11 | Mid:111312.40 | Flow:17 | Strength:133.75 | Trend Following | >>> ORDER PUNCH: BUY VERIFIED! (FORGIVING_BUY)
15:32:03 [CONV_DEBUG] New: 24.0% | Rank: 4.0% | HistSize: 25/25 | HistMax: 59.7%
15:32:03 | Mid:111132.95 | StrL:78 | Strength:78.25 | [3m:13.2% 5m:-0.4% 15m:34.7%] -> BULLISH (Conviction: 24.0%) | > PULSE (FORGIVING_SELL)
15:33:03 | Mid:111000.05 | Sz%:100.0 | Strength:50.00 | [3m:7.3% 5m:-1.4% 15m:32.9%] -> NEUTRAL (Divergence: 34.3%) | > PULSE (MAX_SHOCK_SELL)
15:33:05 | Mid:111008.05 | Sz%:100.0 | Strength:50.00 | [3m:7.7% 5m:-1.5% 15m:32.8%] -> NEUTRAL (Divergence: 34.3%) | > PULSE (MAX_SHOCK_BUY)
15:33:30 | Mid:111036.85 | StrL:21 | Strength:21.35 | [3m:1.0% 5m:-3.0% 15m:32.4%] -> NEUTRAL (Divergence: 35.4%) | > PULSE (FORGIVING_BUY)
15:34:18 | Mid:110941.45 | StrL:21 | Strength:21.63 | [3m:-29.3% 5m:2.5% 15m:31.0%] -> NEUTRAL (Divergence: 60.3%) | > PULSE (FORGIVING_SELL)
15:34:52 | Mid:110921.95 | Sz%:100.0 | Strength:50.00 | [3m:-22.5% 5m:1.4% 15m:30.0%] -> NEUTRAL (Divergence: 52.5%) | > PULSE (MAX_SHOCK_BUY)
15:35:00 | Mid:110937.00 | StrL:57 | Strength:57.42 | [3m:-17.3% 5m:-0.4% 15m:30.1%] -> NEUTRAL (Divergence: 47.4%) | > PULSE (FORGIVING_SELL)
15:35:08 | Mid:110925.60 | StrL:71 | Strength:71.45 | [3m:-17.0% 5m:-2.7% 15m:29.8%] -> NEUTRAL (Divergence: 46.9%) | > PULSE (FORGIVING_SELL)
15:35:08 | CLUSTER DETECTED (SELL 71.45)... PENDING VERIFICATION...
15:35:08 | Mid:110921.85 | Flow:-13 | Strength:71.45 | Trend Following | >>> ORDER PUNCH: SELL VERIFIED! (FORGIVING_SELL)
15:35:12 | Mid:110912.05 | StrL:17 | Strength:17.25 | [3m:-20.7% 5m:-2.4% 15m:29.8%] -> NEUTRAL (Divergence: 50.5%) | > PULSE (FORGIVING_SELL)
15:35:12 | CLUSTER DETECTED (SELL 17.25)... PENDING VERIFICATION...
15:35:14 | VERIFICATION FAILED for SELL cluster. (Flow:3). Resetting.
15:35:47 [CONV_DEBUG] New: 21.2% | Rank: 0.0% | HistSize: 25/25 | HistMax: 59.7%
15:35:47 | Mid:110836.70 | StrL:112 | Strength:112.48 | [3m:-31.3% 5m:-11.2% 15m:28.5%] -> BEARISH (Conviction: -21.2%) | > PULSE (FORGIVING_SELL)
15:36:24 [CONV_DEBUG] New: 23.8% | Rank: 4.0% | HistSize: 25/25 | HistMax: 57.7%
15:36:24 | Mid:110849.05 | StrL:74 | Strength:74.42 | [3m:-23.0% 5m:-24.6% 15m:27.5%] -> BEARISH (Conviction: -23.8%) | > PULSE (FORGIVING_BUY)
15:37:23 [CONV_DEBUG] New: 17.0% | Rank: 0.0% | HistSize: 25/25 | HistMax: 55.3%
15:37:23 | Mid:110836.95 | Sz%:100.0 | Strength:50.00 | [3m:-12.9% 5m:-21.1% 15m:26.8%] -> BEARISH (Conviction: -17.0%) | > PULSE (MAX_SHOCK_BUY)
15:38:24 | Mid:110858.05 | StrL:14 | Strength:14.74 | [3m:-7.5% 5m:-11.8% 15m:27.2%] -> NEUTRAL (Divergence: 39.0%) | > PULSE (FORGIVING_BUY)
15:38:28 | Mid:110943.30 | Sz%:100.0 | Strength:50.00 | [3m:-1.0% 5m:-8.8% 15m:27.5%] -> NEUTRAL (Divergence: 36.3%) | > PULSE (MAX_SHOCK_BUY)
15:38:28 | CLUSTER DETECTED (BUY 50.00)... PENDING VERIFICATION...
15:38:28 | Mid:110943.30 | Flow:17 | Strength:50.00 | Trend Following | >>> ORDER PUNCH: BUY VERIFIED! (MAX_SHOCK_BUY)
15:38:37 | Mid:110988.50 | StrL:46 | Strength:46.91 | [3m:3.1% 5m:-6.1% 15m:27.8%] -> NEUTRAL (Divergence: 33.8%) | > PULSE (FORGIVING_BUY)
15:38:37 | CLUSTER DETECTED (BUY 46.91)... PENDING VERIFICATION...
15:38:38 | Mid:110989.65 | Flow:13 | Strength:46.91 | Trend Following | >>> ORDER PUNCH: BUY VERIFIED! (FORGIVING_BUY)
15:39:03 | Mid:110963.95 | StrL:34 | Strength:34.50 | [3m:9.0% 5m:-3.8% 15m:28.1%] -> NEUTRAL (Divergence: 31.9%) | > PULSE (FORGIVING_BUY)
15:39:33 [CONV_DEBUG] New: 27.4% | Rank: 20.0% | HistSize: 25/25 | HistMax: 55.3%
15:39:33 | Mid:110985.65 | StrL:21 | Strength:21.08 | [3m:26.2% 5m:7.1% 15m:28.5%] -> BULLISH (Conviction: 27.4%) | > PULSE (FORGIVING_BUY)
15:39:47 [CONV_DEBUG] New: 29.7% | Rank: 36.0% | HistSize: 25/25 | HistMax: 55.3%
15:39:47 | Mid:110928.65 | StrL:66 | Strength:66.89 | [3m:31.0% 5m:6.8% 15m:28.5%] -> BULLISH (Conviction: 29.7%) | > PULSE (FORGIVING_BUY)
15:39:47 | CLUSTER DETECTED (BUY 66.89)... PENDING VERIFICATION...
15:39:47 | Mid:110928.65 | Flow:17 | Strength:66.89 | COUNTER-TREND | >>> ORDER PUNCH: BUY VERIFIED! (FORGIVING_BUY)
15:40:03 [CONV_DEBUG] New: 27.2% | Rank: 20.0% | HistSize: 25/25 | HistMax: 55.3%
15:40:03 | Mid:110921.80 | StrL:188 | Strength:188.52 | [3m:26.2% 5m:1.2% 15m:28.2%] -> BULLISH (Conviction: 27.2%) | > PULSE (FORGIVING_SELL)
15:40:05 [CONV_DEBUG] New: 24.3% | Rank: 20.0% | HistSize: 25/25 | HistMax: 55.3%
15:40:05 | Mid:110893.85 | StrL:78 | Strength:78.08 | [3m:20.7% 5m:-0.5% 15m:27.9%] -> BULLISH (Conviction: 24.3%) | > PULSE (FORGIVING_SELL)
15:40:05 | CLUSTER DETECTED (SELL 78.08)... PENDING VERIFICATION...
15:40:06 | VERIFICATION FAILED for SELL cluster. (Flow:-1). Resetting.
15:40:09 [CONV_DEBUG] New: 24.9% | Rank: 24.0% | HistSize: 25/25 | HistMax: 55.1%
15:40:09 | Mid:110924.05 | StrL:29 | Strength:29.45 | [3m:21.8% 5m:1.1% 15m:27.9%] -> BULLISH (Conviction: 24.9%) | > PULSE (FORGIVING_BUY)
15:40:36 [CONV_DEBUG] New: 20.6% | Rank: 4.0% | HistSize: 25/25 | HistMax: 55.0%
15:40:36 | Mid:110858.95 | StrL:87 | Strength:87.16 | [3m:20.1% 5m:3.4% 15m:21.0%] -> BULLISH (Conviction: 20.6%) | > PULSE (FORGIVING_SELL)
15:41:30 | Mid:110713.85 | StrL:34 | Strength:34.48 | [3m:-0.2% 5m:8.1% 15m:5.6%] -> NEUTRAL (Divergence: 8.4%) | > PULSE (FORGIVING_SELL)
15:41:40 | Mid:110685.00 | StrL:32 | Strength:32.99 | [3m:-10.7% 5m:5.4% 15m:3.6%] -> NEUTRAL (Divergence: 16.0%) | > PULSE (FORGIVING_SELL)
15:41:40 | CLUSTER DETECTED (SELL 32.99)... PENDING VERIFICATION...
15:41:40 | VERIFICATION FAILED for SELL cluster. (Flow:-11). Resetting.
15:41:44 | Mid:110692.45 | Sz%:100.0 | Strength:50.00 | [3m:-10.1% 5m:4.2% 15m:-0.8%] -> NEUTRAL (Divergence: 14.3%) | > PULSE (MAX_SHOCK_SELL)

================================================================================
PERFORMANCE REPORT @ 50 SIGNALS (Time: Wed Oct 15 15:41:44 2025)
================================================================================
Signal Reason                       |  Count |   Hit Rate |  Avg PnL % |  Total PnL %
--------------------------------------------------------------------------------
FORGIVING_BUY                       |     25 |     40.00% |    0.0192% |      0.4800%
FORGIVING_SELL                      |     15 |     46.67% |    0.0237% |      0.3560%
MAX_SHOCK_SELL                      |      4 |     50.00% |    0.0260% |      0.1040%
MAX_SHOCK_BUY                       |      4 |      0.00% |   -0.0080% |     -0.0320%
--------------------------------------------------------------------------------
OVERALL                             |     48 |     39.58% |    0.0189% |      0.9080%
================================================================================

