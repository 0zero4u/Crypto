import asyncio
import websockets
import json
import time
import bisect
import math
from collections import deque, defaultdict
from dataclasses import dataclass, field
from typing import Deque, Optional, Tuple, List, Dict, Any
import numpy as np

# Optional: nest_asyncio for notebooks
try:
    import nest_asyncio
    nest_asyncio.apply()
except ImportError:
    pass

#
# 1. DATA STRUCTURES & MULTI-ARMED BANDIT MANAGER
#
@dataclass
class Tick:
    ts: float; bid: float; ask: float; last_price: float; last_size: float; last_side: int; pre_trade_mid: float
    @property
    def mid(self) -> float: return (self.bid + self.ask) * 0.5
    @property
    def spread(self) -> float: return self.ask - self.bid
    @property
    def price_impact(self) -> float:
        if self.pre_trade_mid == 0: return 0.0
        return (self.last_price - self.pre_trade_mid) * self.last_side

@dataclass
class Config:
    benchmark_warmup_minutes: float = 4.0
    benchmark_lookback_minutes: float = 7.0
    live_run_minutes: float = 60.0
    lts_percentile_thresh: float = 99.8
    tfi_lookback_trades: int = 18
    sv_lookback_ticks: int = 10
    sv_max_abs_thresh: float = 0.50
    tfi_lookback_for_std_dev: int = 200
    tfi_std_dev_multiplier: float = 3.75
    signal_cooldown_ms: int = 1500
    min_signal_strength_thresh: float = 2.9
    min_price_impact_for_confirmation: float = 0.8
    cluster_max_lookback_ms: int = 20000
    weak_signal_strength_thresh: float = 2.9
    strong_escalation_thresh: float = 20.0
    verification_trade_lookahead: int = 17
    verification_min_net_flow: int = 12
    dominant_flow_lookback_trades: int = 1000
    forgiving_streak_length_thresh: int = 45
    forgiving_streak_max_lives: int = 8
    forgiving_streak_max_counter_volume_ratio: float = 0.30
    target_return: float = 0.0006
    stop_loss_return: float = -0.0001
    max_holding_time_seconds: int = 120
    reporting_interval_signals: int = 25

# --- Bandit manager for parameter set selection
class BanditManager:
    def __init__(self, n_arms, temperature=0.3, decay=0.995, pnl_window=30):
        self.n_arms = n_arms
        self.temperature = temperature
        self.decay = decay
        self.pnl_histories = [deque(maxlen=pnl_window) for _ in range(n_arms)]
        self.current_arm = 0
    def select_arm(self):
        avg_pnls = [np.mean(h) if h else 0.0 for h in self.pnl_histories]
        temp = max(self.temperature, 0.05)
        exps = np.exp(np.array(avg_pnls) / temp)
        ps = exps / np.sum(exps)
        self.current_arm = np.random.choice(self.n_arms, p=ps)
        self.temperature *= self.decay
        return self.current_arm
    def record_pnl(self, pnl, arm=None):
        idx = self.current_arm if arm is None else arm
        self.pnl_histories[idx].append(pnl)
    def show_avgs(self):
        return [np.mean(h) if h else 0 for h in self.pnl_histories]

#
# 2. PERFORMANCE TRACKERS (Unchanged)
#
@dataclass
class PendingSignal:
    entry_ts: float; entry_price: float; side: int; reason: str; strength: float; tp_price: float; sl_price: float
class PerformanceTracker:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.pending_signals: Deque[PendingSignal] = deque()
        self.signal_stats: Dict[str, Dict[str, Any]] = defaultdict(lambda: {'count': 0, 'hits': 0, 'misses': 0, 'timeouts': 0, 'total_pnl_return': 0.0})
        self.total_signals_generated = 0
        self.last_reported_signal_count = 0
    def add_signal(self, signal_info: Dict[str, Any], entry_price: float):
        self.total_signals_generated += 1
        side = signal_info['signal_pulse']
        tp_price = entry_price * (1 + self.cfg.target_return * side)
        sl_price = entry_price * (1 + self.cfg.stop_loss_return * side)
        pending = PendingSignal(entry_ts=signal_info['ts'], entry_price=entry_price, side=side, reason=signal_info['reason'], strength=signal_info['strength'], tp_price=tp_price, sl_price=sl_price)
        self.pending_signals.append(pending)
    def evaluate_signals(self, current_ts: float, current_mid_price: float):
        signals_to_keep = deque()
        for signal in self.pending_signals:
            pnl_return, outcome = 0.0, None
            if signal.side == 1:
                if current_mid_price >= signal.tp_price: pnl_return, outcome = self.cfg.target_return, 'HIT'
                elif current_mid_price <= signal.sl_price: pnl_return, outcome = self.cfg.stop_loss_return, 'MISS'
            else:
                if current_mid_price <= signal.tp_price: pnl_return, outcome = self.cfg.target_return, 'HIT'
                elif current_mid_price >= signal.sl_price: pnl_return, outcome = self.cfg.stop_loss_return, 'MISS'
            if not outcome and (current_ts - signal.entry_ts) > self.cfg.max_holding_time_seconds:
                pnl_return, outcome = ((current_mid_price - signal.entry_price) / signal.entry_price) * signal.side, 'TIMEOUT'
            if outcome: self._update_stats(signal.reason, pnl_return, outcome)
            else: signals_to_keep.append(signal)
        self.pending_signals = signals_to_keep
    def _update_stats(self, reason: str, pnl_return: float, outcome: str):
        stats = self.signal_stats[reason]
        stats['count'] += 1; stats['total_pnl_return'] += pnl_return
        if outcome == 'HIT': stats['hits'] += 1
        elif outcome == 'MISS': stats['misses'] += 1
        elif outcome == 'TIMEOUT': stats['timeouts'] += 1
    def maybe_report_metrics(self):
        if self.total_signals_generated > 0 and self.total_signals_generated // self.cfg.reporting_interval_signals > self.last_reported_signal_count // self.cfg.reporting_interval_signals:
            self.last_reported_signal_count = self.total_signals_generated
            print("\n" + "="*80 + f"\nPERFORMANCE REPORT @ {self.total_signals_generated} SIGNALS (Time: {time.ctime()})\n" + "="*80)
            print(f"{'Signal Reason':<30} | {'Count':>6} | {'Hit Rate':>10} | {'Avg PnL %':>10} | {'Total PnL %':>12}"); print("-"*80)
            total_pnl, total_count = 0.0, 0
            sorted_reasons = sorted(self.signal_stats.keys(), key=lambda r: self.signal_stats[r]['count'], reverse=True)
            for reason in sorted_reasons:
                stats = self.signal_stats[reason]; count = stats['count']
                if count == 0: continue
                total_pnl += stats['total_pnl_return']; total_count += count
                hit_rate = (stats['hits'] / count) * 100 if count > 0 else 0
                avg_pnl = (stats['total_pnl_return'] / count) * 100 if count > 0 else 0
                total_pnl_reason = stats['total_pnl_return'] * 100
                print(f"{reason:<30} | {count:>6} | {hit_rate:>9.2f}% | {avg_pnl:>9.4f}% | {total_pnl_reason:>11.4f}%")
            print("-"*80)
            overall_hit_rate = (sum(s['hits'] for s in self.signal_stats.values()) / total_count) * 100 if total_count > 0 else 0
            overall_avg_pnl = (total_pnl / total_count) * 100 if total_count > 0 else 0
            print(f"{'OVERALL':<30} | {total_count:>6} | {overall_hit_rate:>9.2f}% | {overall_avg_pnl:>9.4f}% | {total_pnl * 100:>11.4f}%"); print("="*80 + "\n")

#
# 3. HFT CORE LOGIC [same as your script, not edited -- omitted for brevity]
#
# ... RollingPercentile, RollingStandardDeviation, FeatureEngine, StealthDetector, SignalEngine, OrderPunchEngine ...
#   [Insert your code here -- no change.]

#
# 4. LIVE WEBSOCKET HANDLER WITH BANDIT WRAPPING
#

async def live_bandit_signal_generator(parameter_sets, switch_every=20):
    n_strategies = len(parameter_sets)
    bandit = BanditManager(n_strategies)
    current_param_idx = 0
    active_config = parameter_sets[0]
    signal_counter = 0
    all_signal_pnls = []  # Optionally store (signal_number, param_idx, pnl)
    G, R, Y, C, M, B, W, END = '\033[92m', '\033[91m', '\033[93m', '\033[96m', '\033[95m', '\033[94m', '\033[97m', '\033[0m'

    uri = "wss://fstream.binance.com/stream?streams=btcusdt@bookTicker/btcusdt@trade"

    print(f"{W}Connecting to {uri}... using {n_strategies} strategy arms.{END}")
    async with websockets.connect(uri) as websocket:
        print(f"{G}WebSocket connected. Bandit system live!{END}")

        fe_state = FeatureState()
        fe = FeatureEngine(active_config)
        stealth_detector = StealthDetector(active_config)
        performance_tracker = PerformanceTracker(active_config)
        sig_engine = SignalEngine(active_config)
        punch_engine = OrderPunchEngine(active_config)

        latest_bid_price, latest_ask_price = None, None
        start_time = time.time()
        run_time_minutes = active_config.live_run_minutes
        run_end_ts = start_time + run_time_minutes * 60.0

        while time.time() < run_end_ts:
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=10.0)
            except asyncio.TimeoutError:
                print("Websocket timeout.")
                continue
            data = json.loads(message)
            stream, payload = data.get('stream'), data.get('data')
            if stream == 'btcusdt@bookTicker':
                latest_bid_price, latest_ask_price = float(payload['b']), float(payload['a'])
            elif stream == 'btcusdt@trade':
                if latest_bid_price is None: continue
                pre_trade_mid = (latest_bid_price + latest_ask_price) * 0.5
                current_ts = time.time()

                tick = Tick(
                    ts=current_ts,
                    bid=latest_bid_price, ask=latest_ask_price,
                    last_price=float(payload['p']),
                    last_size=float(payload['q']),
                    last_side=int(1 if payload['m'] == False else -1),
                    pre_trade_mid=pre_trade_mid
                )
                features = fe.update(tick, fe_state)
                stealth_info = stealth_detector.update(tick)
                signal_info = sig_engine.step(current_ts, features, stealth_info)

                # --------- Bandit PARAMETER SWITCHING EVERY N SIGNALS
                if (signal_counter % switch_every) == 0:
                    idx = bandit.select_arm()  # Pick next config index
                    active_config = parameter_sets[idx]
                    print(f"{Y}{time.ctime(current_ts)[11:19]} Bandit switched to parameter set #{idx + 1}: {active_config}{END}")

                    # re-init all state with new config for next switch_every signals
                    fe = FeatureEngine(active_config)
                    stealth_detector = StealthDetector(active_config)
                    performance_tracker = PerformanceTracker(active_config)
                    sig_engine = SignalEngine(active_config)
                    punch_engine = OrderPunchEngine(active_config)

                # --- Run rest of main trade logic using current active_config (no changes in HFT triggers)
                if features and signal_info and signal_info.get('signal_pulse', 0) != 0:
                    # Logging, reporting, etc (insert as before)
                    # Optionally: performance_tracker.add_signal(signal_info, features['mid']),
                    # punch_engine handling, etc.

                    # For demonstration, simulate closing trade and compute PnL:
                    # Here PnL logic should be linked to your entry/exit reporting
                    realized_pnl = np.random.normal(0.0004, 0.001)  # DEMO: Replace with your actual trade PnL result logic
                    bandit.record_pnl(realized_pnl)
                    all_signal_pnls.append((signal_counter, idx, realized_pnl))

                    # Optional: show bandit stats every 25 signals
                    if (signal_counter % 25) == 0:
                        print(f"{M}Bandit rolling avg PnL per arm: {bandit.show_avgs()}{END}")

                signal_counter += 1

#
# 5. PARAMETER SETUP & EXECUTION
#
if __name__ == "__main__":
    # EXAMPLE ARMS: Add/modify as needed (3 configs here)
    parameter_sets = [
        Config(tfi_std_dev_multiplier=3.5, forgiving_streak_length_thresh=30, min_signal_strength_thresh=4.0, lts_percentile_thresh=99.7, verification_min_net_flow=8),
        Config(tfi_std_dev_multiplier=4.5, forgiving_streak_length_thresh=45, min_signal_strength_thresh=5.0, lts_percentile_thresh=99.8, verification_min_net_flow=12),
        Config(tfi_std_dev_multiplier=5.5, forgiving_streak_length_thresh=25, min_signal_strength_thresh=6.5, lts_percentile_thresh=99.85, verification_min_net_flow=20),
    ]
    print("="*60)
    print("      HFT Signal Gen v10.3 w/ Softmax Bandit Tuner      ")
    print("="*60)
    print("Testing configs:")
    for i, cfg in enumerate(parameter_sets): print(f" Arm {i + 1}: {cfg}")
    print("-"*60)
    try:
        asyncio.run(live_bandit_signal_generator(parameter_sets, switch_every=20))
    except KeyboardInterrupt:
        print("\nScript interrupted by user. Exiting.")
    except Exception as e:
        print(f"\nAn error occurred: {e}")
  
