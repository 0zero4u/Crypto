
import asyncio
import websockets
import json
import time
import bisect
import math
from collections import deque, defaultdict
from dataclasses import dataclass, field
from typing import Deque, Optional, Tuple, List, Dict, Any

try:
    import nest_asyncio
    nest_asyncio.apply()
except ImportError:
    print("nest_asyncio not found. This is fine for standard Python scripts.")


#
# 1. DATA STRUCTURES & CONFIGURATION
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
class RegimeInfo:
    state: str; metric_value: float; metric_name: str; deltas: List[Tuple[str, float]]

@dataclass
class Config:
    """Config v10.14: Logic Fix for Exhaustion Filter & NameError."""
    benchmark_warmup_minutes: float = 4.0
    benchmark_lookback_minutes: float = 8.0
    live_run_minutes: float = 60.0
    lts_percentile_thresh: float = 99.98
    tfi_lookback_trades: int = 18
    sv_lookback_ticks: int = 10
    sv_max_abs_thresh: float = 0.50
    tfi_lookback_for_std_dev: int = 200
    tfi_std_dev_multiplier: float = 3.75
    absorption_z_score_thresh: float = 4.0
    signal_cooldown_ms: int = 1000
    min_signal_strength_thresh: float = 2.9
    min_price_impact_for_confirmation: float = 0.8
    cluster_max_lookback_ms: int = 20000
    weak_signal_strength_thresh: float = 2.9
    strong_escalation_thresh: float = 15.0
    verification_trade_lookahead: int = 17
    verification_min_net_flow: int = 12
    dominant_flow_lookback_trades: int = 1000
    forgiving_streak_length_thresh: int = 25
    forgiving_streak_max_lives: int = 5
    forgiving_streak_max_counter_volume_ratio: float = 0.30
    exhaustion_streak_min_length: int = 150
    ### FIX ### - Cooldown to prevent spamming the exhaustion log message.
    exhaustion_log_cooldown_seconds: float = 5.0
    target_return: float = 0.0006
    stop_loss_return: float = -0.00008
    max_holding_time_seconds: int = 120
    reporting_interval_signals: int = 25
    conviction_anomaly_history_size: int = 25
    conviction_anomaly_bypass_percentile: float = 95.0

#
# 2. PERFORMANCE TRACKERS (No changes)
#
@dataclass
class PendingSignal:
    entry_ts: float; entry_price: float; side: int; reason: str; strength: float; tp_price: float; sl_price: float
class PerformanceTracker:
    def __init__(self, cfg: Config):
        self.cfg = cfg; self.pending_signals: Deque[PendingSignal] = deque()
        self.signal_stats: Dict[str, Dict[str, Any]] = defaultdict(lambda: {'count': 0, 'hits': 0, 'misses': 0, 'timeouts': 0, 'total_pnl_return': 0.0})
        self.total_signals_generated = 0; self.last_reported_signal_count = 0
    def add_signal(self, signal_info: Dict[str, Any], entry_price: float):
        self.total_signals_generated += 1; side = signal_info['signal_pulse']
        tp_price = entry_price * (1 + self.cfg.target_return * side); sl_price = entry_price * (1 + self.cfg.stop_loss_return * side)
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
        stats = self.signal_stats[reason]; stats['count'] += 1; stats['total_pnl_return'] += pnl_return
        if outcome == 'HIT': stats['hits'] += 1
        elif outcome == 'MISS': stats['misses'] += 1
        elif outcome == 'TIMEOUT': stats['timeouts'] += 1
    def maybe_report_metrics(self):
        if self.total_signals_generated > 0 and self.total_signals_generated // self.cfg.reporting_interval_signals > self.last_reported_signal_count // self.cfg.reporting_interval_signals:
            self.last_reported_signal_count = self.total_signals_generated
            print("\n" + "="*80 + f"\nPERFORMANCE REPORT @ {self.total_signals_generated} SIGNALS (Time: {time.ctime()})\n" + "="*80)
            print(f"{'Signal Reason':<35} | {'Count':>6} | {'Hit Rate':>10} | {'Avg PnL %':>10} | {'Total PnL %':>12}"); print("-"*80)
            total_pnl, total_count = 0.0, 0
            sorted_reasons = sorted(self.signal_stats.keys(), key=lambda r: self.signal_stats[r]['count'], reverse=True)
            for reason in sorted_reasons:
                stats = self.signal_stats[reason]; count = stats['count']
                if count == 0: continue
                total_pnl += stats['total_pnl_return']; total_count += count
                hit_rate = (stats['hits'] / count) * 100 if count > 0 else 0; avg_pnl = (stats['total_pnl_return'] / count) * 100 if count > 0 else 0
                total_pnl_reason = stats['total_pnl_return'] * 100
                print(f"{reason:<35} | {count:>6} | {hit_rate:>9.2f}% | {avg_pnl:>9.4f}% | {total_pnl_reason:>11.4f}%")
            print("-"*80)
            overall_hit_rate = (sum(s['hits'] for s in self.signal_stats.values()) / total_count) * 100 if total_count > 0 else 0
            overall_avg_pnl = (total_pnl / total_count) * 100 if total_count > 0 else 0
            print(f"{'OVERALL':<35} | {total_count:>6} | {overall_hit_rate:>9.2f}% | {overall_avg_pnl:>9.4f}% | {total_pnl * 100:>11.4f}%"); print("="*80 + "\n")

#
# 3. HFT CORE LOGIC
#
class RollingPercentile: # ... (No changes)
    def __init__(self, lookback_s: float, sampling_interval_s: float):
        self.max_size = int(lookback_s / sampling_interval_s); self.history_q: Deque[float] = deque(maxlen=self.max_size); self.sorted_history: List[float] = []
    def update(self, value: float):
        if len(self.history_q) == self.max_size:
            oldest_val = self.history_q[0]; old_idx = bisect.bisect_left(self.sorted_history, oldest_val)
            if old_idx < len(self.sorted_history) and self.sorted_history[old_idx] == oldest_val: self.sorted_history.pop(old_idx)
        self.history_q.append(value); bisect.insort_left(self.sorted_history, value)
    def get_percentile_rank(self, value: float) -> float:
        if not self.sorted_history: return 50.0
        return (bisect.bisect_left(self.sorted_history, value) / len(self.sorted_history)) * 100.0
    @property
    def is_ready(self) -> bool: return len(self.history_q) > self.max_size * 0.20
class RollingStandardDeviation: # ... (No changes)
    def __init__(self, window_size: int): self.window_size = window_size; self.q: Deque[float] = deque(maxlen=window_size); self.sum = 0.0; self.sum_sq = 0.0
    def update(self, value: float):
        if len(self.q) == self.window_size: oldest_val = self.q[0]; self.sum -= oldest_val; self.sum_sq -= oldest_val**2
        self.q.append(value); self.sum += value; self.sum_sq += value**2
    @property
    def mean(self) -> float: return self.sum / len(self.q) if self.q else 0.0
    @property
    def std_dev(self) -> float:
        n = len(self.q)
        if n < 2: return 0.0
        mean = self.mean; variance = (self.sum_sq / n) - (mean**2)
        return math.sqrt(variance) if variance > 0 else 0.0
    @property
    def is_ready(self) -> bool: return len(self.q) >= self.window_size * 0.5
@dataclass
class FeatureState: # ... (No changes)
    trade_flow_hist: Deque[Tuple[float, int]] = field(default_factory=deque); spread_history: Deque[float] = field(default_factory=deque); dominant_flow_hist: Deque[int] = field(default_factory=deque)
class FeatureEngine: # ... (No changes)
    def __init__(self, cfg: Config):
        self.cfg = cfg; self.trade_size_benchmarker = RollingPercentile(cfg.benchmark_lookback_minutes * 60.0, 1/20.0)
        self.tfi_benchmarker = RollingStandardDeviation(cfg.tfi_lookback_for_std_dev)
        self.price_impact_benchmarker = RollingStandardDeviation(cfg.tfi_lookback_for_std_dev)
    def update(self, tick: Tick, state: FeatureState) -> Dict[str, any]:
        state.trade_flow_hist.append((tick.last_size, tick.last_side))
        if len(state.trade_flow_hist) > self.cfg.tfi_lookback_trades: state.trade_flow_hist.popleft()
        tfi = sum(size * side for size, side in state.trade_flow_hist); self.tfi_benchmarker.update(tfi)
        self.trade_size_benchmarker.update(tick.last_size); size_pct_rank = self.trade_size_benchmarker.get_percentile_rank(tick.last_size)
        self.price_impact_benchmarker.update(tick.price_impact)
        is_large_trade = size_pct_rank > self.cfg.lts_percentile_thresh; state.spread_history.append(tick.spread)
        if len(state.spread_history) > self.cfg.sv_lookback_ticks: state.spread_history.popleft()
        spread_velocity = (tick.spread - state.spread_history[0]) if len(state.spread_history) > 1 else 0.0
        state.dominant_flow_hist.append(tick.last_side)
        if len(state.dominant_flow_hist) > self.cfg.dominant_flow_lookback_trades: state.dominant_flow_hist.popleft()
        dominant_flow = sum(state.dominant_flow_hist)
        return {'mid': tick.mid, 'last_trade_side': tick.last_side, 'size_pct_rank': size_pct_rank,'is_large_trade': is_large_trade, 'tfi': tfi, 'spread_velocity': spread_velocity,'adaptive_tfi_thresh': self.tfi_benchmarker.std_dev * self.cfg.tfi_std_dev_multiplier,'price_impact': tick.price_impact, 'dominant_flow': dominant_flow,'price_impact_mean': self.price_impact_benchmarker.mean,'price_impact_std_dev': self.price_impact_benchmarker.std_dev}
    def is_ready(self) -> bool: return self.trade_size_benchmarker.is_ready and self.tfi_benchmarker.is_ready and self.price_impact_benchmarker.is_ready
@dataclass
class ForgivingStreakState: # ... (No changes)
    side: int = 0; length: int = 0; lives_used: int = 0; total_volume: float = 0.0
    highest_price_in_streak: float = 0.0
    lowest_price_in_streak: float = field(default_factory=lambda: float('inf'))
class StealthDetector: # ... (No changes)
    def __init__(self, cfg: Config):
        self.cfg = cfg; self.streak = ForgivingStreakState()
    def _update_forgiving_streak(self, tick: Tick):
        if self.streak.side == tick.last_side:
            self.streak.length += 1; self.streak.total_volume += tick.last_size
            self.streak.highest_price_in_streak = max(self.streak.highest_price_in_streak, tick.last_price)
            self.streak.lowest_price_in_streak = min(self.streak.lowest_price_in_streak, tick.last_price)
        else:
            avg_streak_trade_size = (self.streak.total_volume / self.streak.length) if self.streak.length > 0 else 0
            is_small_counter = tick.last_size < (avg_streak_trade_size * self.cfg.forgiving_streak_max_counter_volume_ratio)
            if self.streak.lives_used < self.cfg.forgiving_streak_max_lives and is_small_counter and avg_streak_trade_size > 0:
                self.streak.lives_used += 1
            else:
                self.streak = ForgivingStreakState(side=tick.last_side, length=1, lives_used=0, total_volume=tick.last_size, highest_price_in_streak=tick.last_price, lowest_price_in_streak=tick.last_price)
    def _analyze_patterns(self) -> Dict[str, Any]:
        if self.streak.length >= self.cfg.forgiving_streak_length_thresh:
            return {'type': 'FORGIVING', 'side': self.streak.side, 'strength': float(self.streak.length), 'highest_price': self.streak.highest_price_in_streak, 'lowest_price': self.streak.lowest_price_in_streak}
        return {'type': None, 'side': 0, 'strength': 0.0}
    def update(self, tick: Tick) -> Dict[str, Any]:
        self._update_forgiving_streak(tick); return self._analyze_patterns()
class MultiTimeframeVolumeDelta: # ... (No changes)
    def __init__(self):
        self.timeframes_s = {'3m': 180, '5m': 300, '15m': 900}; self.delta_thresh_pct = 10.0; self.consensus_thresh = 2
        self.trade_history: Deque[Tuple[float, float, int]] = deque(); self.max_lookback_s = max(self.timeframes_s.values())
    def update(self, tick: Tick):
        self.trade_history.append((tick.ts, tick.last_size, tick.last_side)); cutoff_ts = tick.ts - self.max_lookback_s
        while self.trade_history and self.trade_history[0][0] < cutoff_ts: self.trade_history.popleft()
    def _calculate_delta_pct(self, lookback_s: float, current_ts: float) -> float:
        cutoff_ts = current_ts - lookback_s; buy_volume, sell_volume = 0.0, 0.0
        for ts, size, side in reversed(self.trade_history):
            if ts < cutoff_ts: break
            if side == 1: buy_volume += size
            else: sell_volume += size
        total_volume = buy_volume + sell_volume
        if total_volume == 0: return 0.0
        return ((buy_volume - sell_volume) / total_volume) * 100.0
    def get_regime(self, current_ts: float) -> RegimeInfo:
        votes = {'BULLISH': 0, 'BEARISH': 0, 'NEUTRAL': 0}; delta_values: List[Tuple[str, float]] = []
        for label, lookback in self.timeframes_s.items():
            delta_pct = self._calculate_delta_pct(lookback, current_ts); delta_values.append((label, delta_pct))
            if delta_pct > self.delta_thresh_pct: votes['BULLISH'] += 1
            elif delta_pct < -self.delta_thresh_pct: votes['BEARISH'] += 1
            else: votes['NEUTRAL'] += 1
        if votes['BULLISH'] >= self.consensus_thresh:
            final_state, metric_name = 'BULLISH', 'Conviction'
            winning_deltas = [d for _, d in delta_values if d > self.delta_thresh_pct]; metric_value = sum(winning_deltas) / len(winning_deltas) if winning_deltas else 0.0
        elif votes['BEARISH'] >= self.consensus_thresh:
            final_state, metric_name = 'BEARISH', 'Conviction'
            winning_deltas = [d for _, d in delta_values if d < -self.delta_thresh_pct]; metric_value = sum(winning_deltas) / len(winning_deltas) if winning_deltas else 0.0
        else:
            final_state, metric_name = 'NEUTRAL', 'Divergence'
            all_deltas = [d for _, d in delta_values]; metric_value = (max(all_deltas) - min(all_deltas)) if all_deltas else 0.0
        return RegimeInfo(state=final_state, metric_value=metric_value, metric_name=metric_name, deltas=delta_values)
class ConvictionAnomalyDetector: # ... (No changes)
    def __init__(self, cfg: Config):
        self.cfg = cfg; self.history: Deque[float] = deque(maxlen=cfg.conviction_anomaly_history_size); self.sorted_history: List[float] = []
    def update_and_check_anomaly(self, regime_info: RegimeInfo) -> Tuple[bool, float]:
        if regime_info.metric_name != 'Conviction': return False, 0.0
        current_conviction = abs(regime_info.metric_value); is_anomaly = False; percentile_rank = 0.0
        if self.is_ready:
            cutoff_index = int(len(self.sorted_history) * (self.cfg.conviction_anomaly_bypass_percentile / 100.0))
            if cutoff_index < len(self.sorted_history):
                threshold_value = self.sorted_history[cutoff_index]; is_anomaly = current_conviction > threshold_value
        if self.sorted_history: percentile_rank = (bisect.bisect_left(self.sorted_history, current_conviction) / len(self.sorted_history)) * 100.0
        if len(self.history) == self.cfg.conviction_anomaly_history_size:
            oldest_val = self.history[0]; old_idx = bisect.bisect_left(self.sorted_history, oldest_val)
            if old_idx < len(self.sorted_history) and self.sorted_history[old_idx] == oldest_val: self.sorted_history.pop(old_idx)
        self.history.append(current_conviction); bisect.insort_left(self.sorted_history, current_conviction)
        return is_anomaly, percentile_rank
    @property
    def is_ready(self) -> bool: return len(self.history) >= self.cfg.conviction_anomaly_history_size * 0.5

@dataclass
class SignalState:
    last_pulse_ts: float = 0.0
    ### FIX ### - State to track the last time an exhaustion message was logged.
    last_exhaustion_print_ts: float = 0.0

### FIX ### - Moved SignalRecord definition BEFORE OrderPunchEngine
@dataclass
class SignalRecord:
    ts: float; side: int; strength: float; reason: str

class SignalEngine:
    def __init__(self, cfg: Config):
        self.cfg = cfg; self.state = SignalState()
    def _calculate_strength(self, features: Dict[str, any], reason_str: str, stealth_info: Dict[str, Any]) -> float:
        std_dev = (features['adaptive_tfi_thresh'] / self.cfg.tfi_std_dev_multiplier) + 1e-9; confirmation_strength = abs(features['tfi']) / std_dev
        trigger_strength = 0.0
        if "SHOCK" in reason_str or "COMBO" in reason_str: trigger_strength = (features['size_pct_rank'] - self.cfg.lts_percentile_thresh) * 5
        elif stealth_info['type'] is not None: trigger_strength = stealth_info['strength']
        return (0.5 * trigger_strength) + (0.5 * confirmation_strength)
    def step(self, ts: float, features: Dict[str, any], stealth_info: Dict[str, Any]) -> Dict[str, any]:
        if ts - self.state.last_pulse_ts < self.cfg.signal_cooldown_ms / 1000.0: return {'signal_pulse': 0}
        side = features['last_trade_side']; is_large_trade = features['is_large_trade']; is_stealth_triggered = stealth_info['type'] is not None
        price_impact_std_dev = features.get('price_impact_std_dev', 0.0)
        if price_impact_std_dev > 1e-9:
            price_impact_mean = features.get('price_impact_mean', 0.0); z_score = (features['price_impact'] - price_impact_mean) / price_impact_std_dev
            if z_score < -self.cfg.absorption_z_score_thresh:
                signal_side = -side; reason_prefix = "MAX_ABSORPTION" if features['size_pct_rank'] >= 99.98 else "ABSORPTION"
                reason = f"{reason_prefix}_{'BUY' if signal_side == 1 else 'SELL'}"; strength = abs(z_score) * 10.0
                self.state.last_pulse_ts = ts; return {'signal_pulse': signal_side, 'reason': reason, 'strength': strength, 'ts': ts}
        potential_reason = ''
        if is_large_trade and is_stealth_triggered and side == stealth_info['side']: potential_reason = f"COMBO-{stealth_info['type']}_{'BUY' if side == 1 else 'SELL'}"
        elif is_large_trade: potential_reason = f"SHOCK_{'BUY' if side == 1 else 'SELL'}"
        elif is_stealth_triggered and side == stealth_info['side']: potential_reason = f"{stealth_info['type']}_{'BUY' if side == 1 else 'SELL'}"
        if not potential_reason: return {'signal_pulse': 0}
        tfi_confirms = abs(features['tfi']) > features['adaptive_tfi_thresh']
        spread_is_stable = abs(features['spread_velocity']) < self.cfg.sv_max_abs_thresh
        price_impact_confirms = features['price_impact'] > self.cfg.min_price_impact_for_confirmation
        if not (tfi_confirms and spread_is_stable and price_impact_confirms): return {'signal_pulse': 0}
        strength = self._calculate_strength(features, potential_reason, stealth_info)
        if strength < self.cfg.min_signal_strength_thresh: return {'signal_pulse': 0}

        ### FIX ### - Exhaustion filter moved to be the FINAL check before returning a signal.
        if "FORGIVING" in potential_reason:
            is_exhausted = False
            streak_len = stealth_info.get('strength', 0.0)
            if streak_len > self.cfg.exhaustion_streak_min_length:
                if side == 1 and features['mid'] < stealth_info.get('highest_price', features['mid'] + 1): is_exhausted = True
                elif side == -1 and features['mid'] > stealth_info.get('lowest_price', features['mid'] - 1): is_exhausted = True
            if is_exhausted:
                # Only print the log message if enough time has passed to prevent spam.
                if ts - self.state.last_exhaustion_print_ts > self.cfg.exhaustion_log_cooldown_seconds:
                    self.state.last_exhaustion_print_ts = ts
                    ts_str = time.ctime(ts)[11:19]; Y, END = '\033[93m', '\033[0m'
                    print(f"{Y}{ts_str} | STREAK EXHAUSTION | Side:{side} Str:{streak_len:.0f} | Price is failing to make progress. Signal Invalidated.{END}")
                return {'signal_pulse': 0} # Invalidate the would-be signal

        self.state.last_pulse_ts = ts
        return {'signal_pulse': side, 'reason': potential_reason, 'strength': strength, 'ts': ts}
class OrderPunchEngine:
    def __init__(self, cfg: Config):
        self.cfg = cfg; self.recent_signals: Deque[SignalRecord] = deque(maxlen=10); self.pending_verification_signal: Optional[SignalRecord] = None
        self.verification_trade_counter: int = 0; self.verification_net_flow: int = 0
    def _reset_verification(self):
        self.pending_verification_signal = None; self.verification_trade_counter = 0; self.verification_net_flow = 0
    def step(self, signal_info: Dict[str, any], tick: Tick) -> Dict[str, any]:
        if self.pending_verification_signal:
            self.verification_trade_counter += 1; self.verification_net_flow += tick.last_side
            required_net_flow = self.cfg.verification_min_net_flow
            strength = self.pending_verification_signal.strength
            if strength > 200: required_net_flow = 6
            elif strength > 75: required_net_flow = 8
            elif strength > 25: required_net_flow = 10
            if self.verification_trade_counter >= self.cfg.verification_trade_lookahead:
                is_verified = (abs(self.verification_net_flow) >= required_net_flow and self.verification_net_flow * self.pending_verification_signal.side > 0)
                result = {'status': 'VERIFIED' if is_verified else 'INVALIDATED', 'signal': self.pending_verification_signal, 'net_flow': self.verification_net_flow}
                self._reset_verification(); return result
            return {'status': 'PENDING'}
        if signal_info.get('signal_pulse', 0) != 0:
            new_signal = SignalRecord(ts=signal_info['ts'], side=signal_info['signal_pulse'], strength=signal_info['strength'], reason=signal_info['reason'])
            self.recent_signals.append(new_signal)
            if len(self.recent_signals) >= 2:
                last_signal, prev_signal = self.recent_signals[-1], self.recent_signals[-2]
                if (last_signal.side == prev_signal.side and (last_signal.ts - prev_signal.ts) * 1000 < self.cfg.cluster_max_lookback_ms):
                    is_absorption_setup = "ABSORPTION" in prev_signal.reason
                    valid_pattern = not (is_absorption_setup and ("ABSORPTION" in last_signal.reason or last_signal.strength < self.cfg.strong_escalation_thresh))
                    if valid_pattern:
                        is_strong_first = prev_signal.strength >= self.cfg.weak_signal_strength_thresh
                        is_escalated = (prev_signal.strength < self.cfg.weak_signal_strength_thresh and last_signal.strength >= self.cfg.strong_escalation_thresh)
                        if is_strong_first or is_escalated:
                            self.pending_verification_signal = last_signal; return {'status': 'CLUSTER_FOUND', 'signal': last_signal}
        return {'status': 'IDLE'}

#
# 4. LIVE WEBSOCKET HANDLER (No changes)
#
async def live_signal_generator(cfg: Config):
    uri = "wss://fstream.binance.com/stream?streams=btcusdt@bookTicker/btcusdt@trade"
    fe, sig, punch_engine, fe_state = FeatureEngine(cfg), SignalEngine(cfg), OrderPunchEngine(cfg), FeatureState()
    stealth_detector = StealthDetector(cfg); performance_tracker = PerformanceTracker(cfg)
    regime_analyzer = MultiTimeframeVolumeDelta(); conviction_detector = ConvictionAnomalyDetector(cfg)
    start_time, warmup_end_ts = time.time(), time.time() + cfg.benchmark_warmup_minutes * 60.0
    run_end_ts = start_time + cfg.live_run_minutes * 60.0
    latest_bid_price, latest_ask_price = None, None
    print(f"Connecting to {uri}...\nHFT Logic Warm-up will last for {cfg.benchmark_warmup_minutes} minutes.")
    G, R, Y, C, M, B, W, END = '\033[92m', '\033[91m', '\033[93m', '\033[96m', '\033[95m', '\033[94m', '\033[97m', '\033[0m'
    def _log_signal(ts: float, signal_info: Dict, features: Dict, regime_info: RegimeInfo):
        ts_str = time.ctime(ts)[11:19]; mid_str = f"Mid:{features['mid']:.2f}"; reason_str, strength = signal_info['reason'], signal_info['strength']; score_str = f"Strength:{strength:.2f}"
        delta_str = " ".join([f"{label}:{val:.1f}%" for label, val in regime_info.deltas]); regime_result_str = f"{regime_info.state} ({regime_info.metric_name}: {regime_info.metric_value:.1f}%)"; regime_full_str = f"[{delta_str}] -> {regime_result_str}"
        if "ABSORPTION" in reason_str:
            z_score = (features['price_impact'] - features['price_impact_mean']) / features['price_impact_std_dev']
            COLOR, trigger_info = C, f"Impact Z:{z_score:.2f}"
        else:
            trigger_info = f"Sz%:{features['size_pct_rank']:.1f}" if "SHOCK" in reason_str or "COMBO" in reason_str else f"StrL:{int(stealth_detector.streak.length)}"
            COLOR = Y if "COMBO" in reason_str else (G if signal_info['signal_pulse'] == 1 else R)
        print(f"{COLOR}{ts_str} | {mid_str} | {trigger_info} | {score_str} | {regime_full_str} | > PULSE ({reason_str}){END}")
    async with websockets.connect(uri) as websocket:
        print("Connection successful!")
        is_warmed_up = False
        while time.time() < run_end_ts:
            try: message = await asyncio.wait_for(websocket.recv(), timeout=10.0)
            except asyncio.TimeoutError: print("Websocket timeout."); continue
            data = json.loads(message); stream, payload = data.get('stream'), data.get('data')
            if stream == 'btcusdt@bookTicker': latest_bid_price, latest_ask_price = float(payload['b']), float(payload['a'])
            elif stream == 'btcusdt@trade':
                if latest_bid_price is None: continue
                pre_trade_mid = (latest_bid_price + latest_ask_price) * 0.5; current_ts = time.time()
                tick = Tick(ts=current_ts, bid=latest_bid_price, ask=latest_ask_price, last_price=float(payload['p']), last_size=float(payload['q']), last_side=-1 if payload['m'] else 1, pre_trade_mid=pre_trade_mid)
                regime_analyzer.update(tick); features = fe.update(tick, fe_state)
                if not is_warmed_up and current_ts > warmup_end_ts:
                    if fe.is_ready(): is_warmed_up = True; print(f"\n--- WARM-UP COMPLETE at {time.ctime(current_ts)}. HFT engine online. ---")
                    else: print(f"Warming up benchmarkers...", end='\r')
                if is_warmed_up:
                    performance_tracker.evaluate_signals(current_ts, features['mid'])
                    stealth_info = stealth_detector.update(tick)
                    signal_info = sig.step(current_ts, features, stealth_info)
                    punch_engine_signal = signal_info
                    if signal_info.get('signal_pulse', 0) != 0:
                        regime_info = regime_analyzer.get_regime(current_ts)
                        if regime_info.metric_name == 'Conviction':
                            is_anomaly, conviction_pct = conviction_detector.update_and_check_anomaly(regime_info)
                            if is_anomaly and conviction_detector.is_ready:
                                bypass_reason = f"CONV_BYPASS|{signal_info['reason']}"; bypass_signal = {**signal_info, 'reason': bypass_reason}
                                conviction_str = f"Conviction:{abs(regime_info.metric_value):.1f}% (Top {(100-conviction_pct):.2f}%)"
                                print(f"{M}{time.ctime(current_ts)[11:19]} | Mid:{features['mid']:.2f} | {conviction_str} | >>> CONVICTION ANOMALY BYPASS ({bypass_reason})! <<< {END}")
                                performance_tracker.add_signal(bypass_signal, features['mid']); punch_engine_signal = {'signal_pulse': 0}
                            else:
                                _log_signal(current_ts, signal_info, features, regime_info); performance_tracker.add_signal(signal_info, features['mid'])
                        else: _log_signal(current_ts, signal_info, features, regime_info); performance_tracker.add_signal(signal_info, features['mid'])
                        performance_tracker.maybe_report_metrics()
                    punch_result = punch_engine.step(punch_engine_signal, tick); status = punch_result.get('status')
                    if status == 'CLUSTER_FOUND':
                        sr = punch_result['signal']; print(f"{B}{time.ctime(current_ts)[11:19]} | CLUSTER DETECTED ({'BUY' if sr.side == 1 else 'SELL'} {sr.strength:.2f})... PENDING VERIFICATION...{END}")
                    elif status == 'VERIFIED':
                        sr = punch_result['signal']; is_trend_following = sr.side * features['dominant_flow'] > 0
                        context_str, CONTEXT_COLOR = ("Trend Following" if is_trend_following else "COUNTER-TREND"), (W if is_trend_following else Y)
                        print(f"{M}{time.ctime(current_ts)[11:19]} | Mid:{features['mid']:.2f} | Flow:{punch_result['net_flow']} | Strength:{sr.strength:.2f} | {CONTEXT_COLOR}{context_str}{M} | >>> ORDER PUNCH: {'BUY' if sr.side == 1 else 'SELL'} VERIFIED! ({sr.reason}){END}")
                    elif status == 'INVALIDATED':
                        sr = punch_result['signal']; print(f"{Y}{time.ctime(current_ts)[11:19]} | VERIFICATION FAILED for {'BUY' if sr.side == 1 else 'SELL'} cluster. (Flow:{punch_result['net_flow']}). Resetting.{END}")

#
# 5. EXECUTION
#
if __name__ == "__main__":
    live_config = Config()
    print("="*60); print("      HFT Signal Gen v10.14 (Logic Fixed)      "); print("="*60)
    print("\nCore Triggers:")
    print(f" - 'ABSORPTION': Trade with statistically significant negative price impact (Z < {-live_config.absorption_z_score_thresh}).")
    print(f" - 'SHOCK': > {live_config.lts_percentile_thresh}th percentile trade size (momentum signal).")
    print(f" - 'FORGIVING': Stealth 'Forgiving Streak' detection.")
    print("\nConfirmation & Filtering Logic:")
    print(f" - TFI & Price Impact confirmation required for standard momentum signals.")
    print(f" - <<< FIXED: 'Effort vs. Result' Streak Exhaustion Filter >>>")
    print(f"   - FORGIVING signals from very long streaks (> {live_config.exhaustion_streak_min_length}) are now INVALIDATED")
    print(f"     if the price is not making new highs/lows. Log spam has been fixed.")
    print("\nMeta-Layer: ORDER PUNCH LOGIC")
    print(f" - Verification: Net flow of next {live_config.verification_trade_lookahead} trades in signal direction.")
    print(f" - Dynamic Verification: Required net flow is lowered for higher strength signals.")
    print("\n<<< PERFORMANCE TRACKING >>>")
    print(f" - Take Profit: {live_config.target_return * 100:.2f}% | Stop Loss: {abs(live_config.stop_loss_return) * 100:.2f}% | Timeout: {live_config.max_holding_time_seconds}s")
    print("-"*60)
    try:
        asyncio.run(live_signal_generator(live_config))
    except KeyboardInterrupt:
        print("\nScript interrupted by user. Exiting.")
    except Exception as e:
        print(f"\nAn error occurred:  {e}")
  
