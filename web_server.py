#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
web_server.py (UPGRADED v2.0)
=============================
New features:
1. VWAP (Volume Weighted Average Price) per period
2. Key Levels (Pivot Points, Support/Resistance)
3. Volume Z-Score vs intraday seasonality
4. CVD (Cumulative Volume Delta) with slope
5. NOI Z-Score (net_delta/vol)
6. Spread tracking with spike detection
"""

import json
import time
import random
import threading
import asyncio
import os
import sys
import csv
import glob
import argparse
import socket
import math
from datetime import datetime, timedelta
from collections import deque, defaultdict, OrderedDict
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import uvicorn

from dotenv import load_dotenv

env_paths = ['.env', 'config/.env', os.path.expanduser('~/.ctp.env')]
for env_path in env_paths:
    if os.path.exists(env_path):
        load_dotenv(env_path, override=True)
        print(f"[Config] 已加载环境变量: {env_path}")
        break

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config'))

from instruments import (
    INSTRUMENTS as INSTRUMENTS_CONFIG,
    PERIODS as PERIODS_CONFIG,
    SIMNOW_CONFIG as _SIMNOW_FROM_FILE,
    get_front_address,
    get_period_seconds,
    get_instrument_info,
    MONITOR_SYMBOLS,
    MONITOR_INSTRUMENTS,
    MONITOR_NAMES,
    MONITOR_BASE_PRICES,
    DISPLAY_PERIODS,
)

SIMNOW_CONFIG = dict(_SIMNOW_FROM_FILE)
env_userid = os.environ.get('CTP_USER_ID')
env_password = os.environ.get('CTP_PASSWORD')
env_brokerid = os.environ.get('CTP_BROKER_ID')
env_appid = os.environ.get('CTP_APP_ID')
env_authcode = os.environ.get('CTP_AUTH_CODE')

if env_brokerid:
    SIMNOW_CONFIG["BrokerID"] = env_brokerid
if env_userid:
    SIMNOW_CONFIG["UserID"] = env_userid
    print(f"[Config] 从环境变量读取 UserID: {env_userid}")
if env_password:
    SIMNOW_CONFIG["Password"] = env_password
    print(f"[Config] 从环境变量读取 Password: {'*' * len(env_password)}")
if env_appid:
    SIMNOW_CONFIG["AppID"] = env_appid
if env_authcode:
    SIMNOW_CONFIG["AuthCode"] = env_authcode

ALL_INSTRUMENTS = [v["code"] for v in INSTRUMENTS_CONFIG.values()]
ALL_INSTRUMENT_NAMES = {v["code"]: v["name"] for v in INSTRUMENTS_CONFIG.values()}
ALL_BASE_PRICES = {v["code"]: v["base_price"] for v in INSTRUMENTS_CONFIG.values()}

INSTRUMENTS = MONITOR_INSTRUMENTS
INSTRUMENT_NAMES = MONITOR_NAMES
BASE_PRICES = MONITOR_BASE_PRICES

PERIODS = {k: v for k, v in PERIODS_CONFIG.items() if k in DISPLAY_PERIODS}

WINDOW_CONFIG = {
    "DEFAULT_MODE": 2,
    "MAX_ACTIVE": 6,
    "ORDERFLOW_MAXLEN": 2000,
}

DATA_DIR = "./data"
KLINE_1M_RETENTION_DAYS = 60
KLINE_OTHER_RETENTION_DAYS = 7
TICK_RETENTION_DAYS = 3
CSV_FLUSH_INTERVAL = 5

try:
    from openctp_ctp import mdapi
    CTP_AVAILABLE = True
except ImportError:
    mdapi = None
    CTP_AVAILABLE = False
    print("⚠️ openctp-ctp 未安装，CTP模式不可用")


from contextlib import asynccontextmanager
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', default='mock', choices=['mock', 'ctp'])
    parser.add_argument('--port', type=int, default=8080, help='监听端口')
    args, _ = parser.parse_known_args()

    print("=" * 60)
    print("CTP 订单流 Web 服务启动中...")
    print("=" * 60)
    print(f"基础品种库: {len(ALL_INSTRUMENTS)} 个")
    print(f"监控品种: {', '.join(INSTRUMENTS)}")
    print(f"活跃品种上限: {WINDOW_CONFIG['MAX_ACTIVE']}")
    print(f"窗口模式: {WINDOW_CONFIG['DEFAULT_MODE']}")
    print(f"数据目录: {os.path.abspath(DATA_DIR)}")
    print(f"启动模式: {args.source}")
    print(f"请用浏览器打开: http://localhost:{args.port}")
    print("=" * 60)

    store.init_bars(source=args.source)

    if args.source == "ctp":
        try:
            success = ctp_engine.start()
            if success:
                dsm.current = "ctp"
                print("[Startup] CTP 模式启动成功")
            else:
                print("[Startup] CTP 连接失败，切换到 Mock")
                engine.start()
                dsm.current = "mock"
        except Exception as e:
            print(f"[Startup] CTP启动失败，回退Mock: {e}")
            engine.start()
            dsm.current = "mock"
    else:
        engine.start()
        dsm.current = "mock"

    push_task = asyncio.create_task(data_pusher())
    try:
        yield
    except asyncio.CancelledError:
        pass  # 正常关闭，不需要报错
    finally:
        # Shutdown
        print("\n" + "=" * 60)
        print("服务关闭中...")

        # 取消 data_pusher 任务
        if push_task and not push_task.done():
            push_task.cancel()
            try:
                await push_task
            except asyncio.CancelledError:
                pass

        engine.stop()
        ctp_engine.stop()
        # [FIX] 等待 CTP 回调线程完全停止，避免 CHECKPOINT 时还有活跃事务
        import time
        time.sleep(0.5)
        try:
            store.db_manager.force_flush()
            store.close()
        except Exception as e:
            print(f"[Shutdown] 数据保存错误: {e}")
        print("数据已保存，服务已关闭")
        print("=" * 60)




app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if os.path.exists("./js"):
    app.mount("/js", StaticFiles(directory="./js"), name="js")



# ============ 技术指标计算工具 ============
class TechnicalIndicators:
    """技术指标计算：VWAP, CVD, NOI, Volume Z-Score, Key Levels, Spread"""

    @staticmethod
    def calc_vwap(bars):
        if not bars:
            return []
        cumulative_tpv = 0
        cumulative_vol = 0
        vwap_values = []
        for bar in bars:
            typical_price = (bar["high"] + bar["low"] + bar["close"]) / 3
            vol = bar.get("volume", 0)
            cumulative_tpv += typical_price * vol
            cumulative_vol += vol
            if cumulative_vol > 0:
                vwap_values.append(round(cumulative_tpv / cumulative_vol, 2))
            else:
                vwap_values.append(typical_price)
        return vwap_values

    @staticmethod
    def calc_cvd(bars):
        if not bars:
            return []
        cvd = 0
        cvd_values = []
        for bar in bars:
            delta = bar.get("delta", 0)
            cvd += delta
            cvd_values.append(cvd)
        return cvd_values

    @staticmethod
    def calc_cvd_slope(cvd_values, window=5):
        if len(cvd_values) < window:
            return [0] * len(cvd_values)
        slopes = [0] * (window - 1)
        for i in range(window - 1, len(cvd_values)):
            y = cvd_values[i - window + 1:i + 1]
            x = list(range(window))
            n = window
            sum_x = sum(x)
            sum_y = sum(y)
            sum_xy = sum(x[j] * y[j] for j in range(window))
            sum_x2 = sum(x[j] ** 2 for j in range(window))
            denominator = n * sum_x2 - sum_x ** 2
            slope = (n * sum_xy - sum_x * sum_y) / denominator if denominator != 0 else 0
            slopes.append(round(slope, 2))
        return slopes

    @staticmethod
    def calc_noi(bars):
        if not bars:
            return []
        noi_values = []
        for bar in bars:
            delta = bar.get("delta", 0)
            vol = bar.get("volume", 0)
            if vol > 0:
                noi_values.append(round(delta / vol, 4))
            else:
                noi_values.append(0)
        return noi_values

    @staticmethod
    def calc_noi_zscore(noi_values, window=20):
        if len(noi_values) < window:
            return [0] * len(noi_values)
        zscores = [0] * (window - 1)
        for i in range(window - 1, len(noi_values)):
            window_data = noi_values[i - window + 1:i + 1]
            mean = sum(window_data) / window
            variance = sum((x - mean) ** 2 for x in window_data) / window
            std = math.sqrt(variance) if variance > 0 else 1
            zscore = (noi_values[i] - mean) / std
            zscores.append(round(zscore, 2))
        return zscores

    @staticmethod
    def calc_volume_zscore(bars, window=20):
        if not bars:
            return []
        volumes = [b.get("volume", 0) for b in bars]
        if len(volumes) < window:
            return [0] * len(volumes)
        zscores = [0] * (window - 1)
        for i in range(window - 1, len(volumes)):
            window_data = volumes[i - window + 1:i + 1]
            mean = sum(window_data) / window
            variance = sum((x - mean) ** 2 for x in window_data) / window
            std = math.sqrt(variance) if variance > 0 else 1
            zscore = (volumes[i] - mean) / std
            zscores.append(round(zscore, 2))
        return zscores

    @staticmethod
    def calc_intraday_seasonality(bars):
        if not bars:
            return {}
        time_buckets = defaultdict(list)
        for bar in bars:
            time_str = bar.get("time", "")
            if len(time_str) >= 2:
                hour = time_str[:2]
                time_buckets[hour].append(bar.get("volume", 0))
        seasonality = {}
        for hour, volumes in time_buckets.items():
            seasonality[hour] = {
                "mean": sum(volumes) / len(volumes) if volumes else 0,
                "std": math.sqrt(sum((v - sum(volumes)/len(volumes))**2 for v in volumes) / len(volumes)) if len(volumes) > 1 else 1
            }
        return seasonality

    @staticmethod
    def calc_volume_zscore_seasonal(bars, seasonality):
        if not bars or not seasonality:
            return []
        zscores = []
        for bar in bars:
            time_str = bar.get("time", "")
            hour = time_str[:2] if len(time_str) >= 2 else "09"
            vol = bar.get("volume", 0)
            if hour in seasonality:
                mean = seasonality[hour]["mean"]
                std = seasonality[hour]["std"] if seasonality[hour]["std"] > 0 else 1
                zscore = (vol - mean) / std
                zscores.append(round(zscore, 2))
            else:
                zscores.append(0)
        return zscores

    @staticmethod
    def calc_key_levels(bars):
        if not bars:
            return {}
        last_bar = bars[-1]
        high = last_bar["high"]
        low = last_bar["low"]
        close = last_bar["close"]
        pivot = (high + low + close) / 3
        r1 = 2 * pivot - low
        r2 = pivot + (high - low)
        r3 = high + 2 * (pivot - low)
        s1 = 2 * pivot - high
        s2 = pivot - (high - low)
        s3 = low - 2 * (high - pivot)
        recent_highs = [b["high"] for b in bars[-20:]]
        recent_lows = [b["low"] for b in bars[-20:]]
        return {
            "pivot": round(pivot, 2),
            "r1": round(r1, 2), "r2": round(r2, 2), "r3": round(r3, 2),
            "s1": round(s1, 2), "s2": round(s2, 2), "s3": round(s3, 2),
            "recent_high": round(max(recent_highs), 2) if recent_highs else high,
            "recent_low": round(min(recent_lows), 2) if recent_lows else low,
        }

    @staticmethod
    def calc_spread(tick_data):
        bid = tick_data.get("bid", 0) or tick_data.get("BidPrice1", 0)
        ask = tick_data.get("ask", 0) or tick_data.get("AskPrice1", 0)
        if bid > 0 and ask > 0:
            return round(ask - bid, 4)
        return 0

    @staticmethod
    def detect_spread_spike(spread_history, current_spread, threshold_zscore=2.0):
        if len(spread_history) < 10:
            return False, 0
        mean = sum(spread_history) / len(spread_history)
        variance = sum((s - mean) ** 2 for s in spread_history) / len(spread_history)
        std = math.sqrt(variance) if variance > 0 else 1
        zscore = (current_spread - mean) / std if std > 0 else 0
        is_spike = zscore > threshold_zscore
        return is_spike, round(zscore, 2)


# ============ 订单流计算器 ============

    @staticmethod
    def calc_volume_profile(bars, price_buckets=20):
        """计算成交量分布 (Volume Profile)
        返回: POC, VAH, VAL, HVN列表, LVN列表
        POC: Point of Control - 成交量最大的价格
        VAH/VAL: Value Area High/Low - 成交量占70%的价格区间
        HVN: High Volume Node - 高成交量节点
        LVN: Low Volume Node - 低成交量节点
        """
        if not bars or len(bars) < 5:
            return {"poc": 0, "vah": 0, "val": 0, "hvn": [], "lvn": []}

        # 收集所有价格点和成交量
        price_vol = defaultdict(float)
        for bar in bars:
            # 将每根K线的高低价区间分成若干点，按成交量均匀分配
            high, low = bar["high"], bar["low"]
            vol = bar.get("volume", 0)
            if high > low and vol > 0:
                # 简化: 将成交量集中在收盘价附近
                close_p = bar["close"]
                # 在[low, high]区间内按价格加权分配成交量
                range_p = high - low
                if range_p > 0:
                    # 用三角形分布: 收盘价处权重最高
                    for i in range(price_buckets):
                        p = low + (high - low) * i / (price_buckets - 1)
                        weight = 1 - abs(p - close_p) / range_p if range_p > 0 else 1
                        weight = max(weight, 0.1)
                        price_vol[round(p, 2)] += vol * weight / price_buckets

        if not price_vol:
            return {"poc": 0, "vah": 0, "val": 0, "hvn": [], "lvn": []}

        # 按成交量排序
        sorted_prices = sorted(price_vol.items(), key=lambda x: x[1], reverse=True)
        total_vol = sum(price_vol.values())

        # POC: 成交量最大的价格
        poc = sorted_prices[0][0] if sorted_prices else 0

        # VAH/VAL: 从POC向两边扩展，直到覆盖70%成交量
        target_vol = total_vol * 0.7
        accumulated = 0
        included_prices = set()
        for price, vol in sorted_prices:
            accumulated += vol
            included_prices.add(price)
            if accumulated >= target_vol:
                break

        vah = max(included_prices) if included_prices else 0
        val = min(included_prices) if included_prices else 0

        # HVN: 成交量高于均值+1σ的价格节点
        mean_vol = total_vol / len(price_vol)
        variance = sum((v - mean_vol) ** 2 for v in price_vol.values()) / len(price_vol)
        std_vol = math.sqrt(variance) if variance > 0 else 1
        hvn_threshold = mean_vol + std_vol

        hvn = [p for p, v in price_vol.items() if v > hvn_threshold]
        lvn = [p for p, v in price_vol.items() if v < mean_vol - 0.5 * std_vol]

        return {
            "poc": round(poc, 2),
            "vah": round(vah, 2),
            "val": round(val, 2),
            "hvn": sorted(hvn),
            "lvn": sorted(lvn),
            "profile": dict(price_vol)
        }

    @staticmethod
    def calc_order_flow_rate(bars, window=5):
        """计算订单流速率 (Order Flow Rate)
        单位时间内主动买/卖量的变化率
        """
        if not bars or len(bars) < window + 1:
            return []
        rates = [0] * window
        for i in range(window, len(bars)):
            # 计算窗口内delta的变化率
            curr_delta = bars[i].get("delta", 0)
            prev_delta = bars[i - window].get("delta", 0)
            curr_vol = bars[i].get("volume", 0)
            prev_vol = bars[i - window].get("volume", 0)

            # 订单流速率 = (当前delta - N期前delta) / 平均成交量
            avg_vol = max((curr_vol + prev_vol) / 2, 1)
            rate = (curr_delta - prev_delta) / avg_vol
            rates.append(round(rate, 4))
        return rates

    @staticmethod
    def detect_large_orders(ticks, window=50, threshold_sigma=2.0):
        """检测大单: tick volume > mean + threshold_sigma * std
        返回大单列表和统计信息
        """
        if not ticks or len(ticks) < window:
            return [], {"mean": 0, "std": 0, "threshold": 0}

        volumes = [t.get("total_vol", 0) for t in ticks[-window:]]
        mean_vol = sum(volumes) / len(volumes)
        variance = sum((v - mean_vol) ** 2 for v in volumes) / len(volumes)
        std_vol = math.sqrt(variance) if variance > 0 else 1
        threshold = mean_vol + threshold_sigma * std_vol

        large_orders = []
        for tick in ticks:
            vol = tick.get("total_vol", 0)
            if vol > threshold and vol > 0:
                large_orders.append({
                    "time": tick.get("time", ""),
                    "price": tick.get("price", 0),
                    "volume": vol,
                    "buy_vol": tick.get("buy_vol", 0),
                    "sell_vol": tick.get("sell_vol", 0),
                    "delta": tick.get("delta", 0),
                    "aggressor": tick.get("aggressor", ""),
                    "zscore": round((vol - mean_vol) / std_vol, 2) if std_vol > 0 else 0
                })

        return large_orders, {"mean": round(mean_vol, 2), "std": round(std_vol, 2), "threshold": round(threshold, 2)}

    @staticmethod
    def calc_signal_confidence(indicators, current_bar, last_ticks):
        """计算高置信度交易信号
        返回: signal, confidence_score, reasons
        signal: 'LONG', 'SHORT', 'NEUTRAL'
        confidence: 0-100
        """
        score = 0
        reasons = []
        max_score = 0

        # 1. CVD方向 (权重20)
        max_score += 20
        cvd = indicators.get("cvd", [])
        cvd_slope = indicators.get("cvd_slope", [])
        if len(cvd) >= 2 and len(cvd_slope) >= 1:
            if cvd[-1] > cvd[-2] and cvd_slope[-1] > 0:
                score += 20
                reasons.append("CVD上升+斜率正")
            elif cvd[-1] < cvd[-2] and cvd_slope[-1] < 0:
                score -= 20
                reasons.append("CVD下降+斜率负")

        # 2. Volume Z-Score (权重20)
        max_score += 20
        vol_z = indicators.get("volume_zscore", [])
        if vol_z:
            last_vz = vol_z[-1]
            if last_vz > 2:
                score += 20
                reasons.append(f"放量异常(VolZ={last_vz})")
            elif last_vz < -2:
                score -= 10
                reasons.append(f"缩量(VolZ={last_vz})")

        # 3. NOI Z-Score (权重20)
        max_score += 20
        noi_z = indicators.get("noi_zscore", [])
        if noi_z:
            last_nz = noi_z[-1]
            if last_nz > 1.5:
                score += 20
                reasons.append(f"买方主导(NOI_Z={last_nz})")
            elif last_nz < -1.5:
                score -= 20
                reasons.append(f"卖方主导(NOI_Z={last_nz})")

        # 4. 价格与VWAP关系 (权重15)
        max_score += 15
        vwap = indicators.get("vwap", [])
        if vwap and current_bar:
            last_vwap = vwap[-1]
            close = current_bar.get("close", 0)
            if close > last_vwap * 1.001:
                score += 15
                reasons.append("价格>VWAP")
            elif close < last_vwap * 0.999:
                score -= 15
                reasons.append("价格<VWAP")

        # 5. Spread状态 (权重10)
        max_score += 10
        spread = current_bar.get("spread", 0) if current_bar else 0
        if last_ticks:
            tick_spread = last_ticks.get("spread", 0)
            if tick_spread < 0.02:  # 正常点差
                score += 5
                reasons.append("点差正常")
            elif tick_spread > 0.05:  # 点差过大
                score -= 10
                reasons.append("点差过大(流动性差)")

        # 6. Volume Profile位置 (权重15)
        max_score += 15
        vp = indicators.get("volume_profile", {})
        if vp and current_bar:
            poc = vp.get("poc", 0)
            vah = vp.get("vah", 0)
            val = vp.get("val", 0)
            close = current_bar.get("close", 0)
            if close > vah:
                score += 15
                reasons.append("价格突破VAH")
            elif close < val:
                score -= 15
                reasons.append("价格跌破VAL")
            elif poc * 0.995 < close < poc * 1.005:
                score += 5
                reasons.append("价格在POC附近")

        # 计算置信度
        confidence = abs(score) / max_score * 100 if max_score > 0 else 0

        if score >= 50:
            signal = "LONG"
        elif score <= -50:
            signal = "SHORT"
        else:
            signal = "NEUTRAL"

        return {
            "signal": signal,
            "score": score,
            "confidence": round(confidence, 1),
            "max_score": max_score,
            "reasons": reasons
        }

    @staticmethod
    def calc_exit_signal(position, indicators, current_bar, entry_price, stop_loss_pct=0.01, take_profit_pct=0.02):
        """计算出场信号
        position: 'LONG' or 'SHORT'
        返回: action, reason
        """
        if not current_bar or not entry_price:
            return "HOLD", "无数据"

        current_price = current_bar.get("close", 0)
        if current_price <= 0 or entry_price <= 0:
            return "HOLD", "价格无效"

        pnl_pct = (current_price - entry_price) / entry_price
        if position == "SHORT":
            pnl_pct = -pnl_pct

        # 止损
        if pnl_pct < -stop_loss_pct:
            return "EXIT", f"止损触发({pnl_pct*100:.2f}%)"

        # 止盈
        if pnl_pct > take_profit_pct:
            return "EXIT", f"止盈触发({pnl_pct*100:.2f}%)"

        # CVD反转出场
        cvd = indicators.get("cvd", [])
        cvd_slope = indicators.get("cvd_slope", [])
        if len(cvd) >= 3 and len(cvd_slope) >= 2:
            if position == "LONG" and cvd[-1] < cvd[-2] and cvd_slope[-1] < 0 and cvd_slope[-2] > 0:
                return "EXIT", "CVD动量反转(多转空)"
            if position == "SHORT" and cvd[-1] > cvd[-2] and cvd_slope[-1] > 0 and cvd_slope[-2] < 0:
                return "EXIT", "CVD动量反转(空转多)"

        # NOI极端反转
        noi_z = indicators.get("noi_zscore", [])
        if noi_z:
            last_nz = noi_z[-1]
            if position == "LONG" and last_nz < -2:
                return "EXIT", f"NOI极端负值({last_nz})"
            if position == "SHORT" and last_nz > 2:
                return "EXIT", f"NOI极端正值({last_nz})"

        return "HOLD", "持仓中"
class OrderFlowCalculator:
    def __init__(self):
        self.last_volume = 0
        self.last_turnover = 0.0

    def on_tick(self, tick_data):
        price = tick_data.get("price") or tick_data.get("LastPrice", 0)
        volume = tick_data.get("total_vol") or tick_data.get("Volume", 0)
        bid = tick_data.get("bid") or tick_data.get("BidPrice1", 0)
        ask = tick_data.get("ask") or tick_data.get("AskPrice1", 0)

        vol_delta = volume - self.last_volume if self.last_volume > 0 else 0
        if vol_delta < 0:
            vol_delta = 0
        self.last_volume = volume

        if price >= ask - 0.01:
            buy_v, sell_v, agg = vol_delta, 0, "BUY"
        elif price <= bid + 0.01:
            buy_v, sell_v, agg = 0, vol_delta, "SELL"
        else:
            buy_v = int(vol_delta * 0.5)
            sell_v = vol_delta - buy_v
            agg = "MIX"

        spread = round(ask - bid, 4) if ask > 0 and bid > 0 else 0

        return {
            "time": tick_data.get("time") or tick_data.get("UpdateTime", ""),
            "millisec": tick_data.get("millisec", 0) or tick_data.get("UpdateMillisec", 0),
            "instrument": tick_data.get("instrument") or tick_data.get("InstrumentID", ""),
            "price": price,
            "total_vol": vol_delta,
            "buy_vol": buy_v,
            "sell_vol": sell_v,
            "delta": buy_v - sell_v,
            "bid": bid,
            "ask": ask,
            "bid_vol": tick_data.get("bid_vol", 0) or tick_data.get("BidVolume1", 0),
            "ask_vol": tick_data.get("ask_vol", 0) or tick_data.get("AskVolume1", 0),
            "aggressor": agg,
            "volume_total": volume,
            "spread": spread,
        }


# ============ DuckDB 存储管理器 ============
class DuckDBManager:
    def __init__(self, data_dir=DATA_DIR):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self.db_path = os.path.join(data_dir, "ctp_data.duckdb")
        import duckdb
        self.conn = duckdb.connect(self.db_path)
        self._init_tables()
        print(f"[DuckDB] 数据库初始化: {os.path.abspath(self.db_path)}")

    def _init_tables(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS ticks (
                id BIGINT PRIMARY KEY,
                timestamp TIMESTAMP,
                date_str VARCHAR(8),
                instrument VARCHAR(20),
                price DOUBLE,
                volume INT,
                buy_vol INT,
                sell_vol INT,
                delta INT,
                bid DOUBLE,
                ask DOUBLE,
                bid_vol INT,
                ask_vol INT,
                aggressor VARCHAR(4),
                total_volume BIGINT,
                spread DOUBLE
            )
        """)
        # [MIGRATION] 添加旧表缺少的列
        try:
            self.conn.execute("ALTER TABLE ticks ADD COLUMN spread DOUBLE DEFAULT 0")
            print("[DuckDB] 迁移: ticks 表添加 spread 列")
        except Exception:
            pass  # 列已存在

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS klines (
                id BIGINT PRIMARY KEY,
                timestamp TIMESTAMP,
                date_str VARCHAR(8),
                instrument VARCHAR(20),
                period VARCHAR(5),
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume INT,
                buy_vol INT,
                sell_vol INT,
                delta INT,
                tick_count INT,
                vwap DOUBLE,
                cvd DOUBLE,
                noi DOUBLE
            )
        """)
        # [MIGRATION] 添加旧表缺少的列
        for col, dtype in [('vwap', 'DOUBLE'), ('cvd', 'DOUBLE'), ('noi', 'DOUBLE')]:
            try:
                self.conn.execute(f"ALTER TABLE klines ADD COLUMN {col} {dtype} DEFAULT 0")
                print(f"[DuckDB] 迁移: klines 表添加 {col} 列")
            except Exception:
                pass  # 列已存在

        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_ticks_inst_time ON ticks(instrument, timestamp)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_klines_inst_period_time ON klines(instrument, period, timestamp)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_ticks_date ON ticks(date_str)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_klines_date ON klines(date_str)")
        self.conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_tick_id START 1")
        self.conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_kline_id START 1")

    def save_tick(self, inst, tick):
        try:
            ts = self._parse_time(tick.get("time", ""))
            self.conn.execute("BEGIN TRANSACTION")
            self.conn.execute("""
                INSERT INTO ticks 
                (id, timestamp, date_str, instrument, price, volume, buy_vol, sell_vol, delta, 
                 bid, ask, bid_vol, ask_vol, aggressor, total_volume, spread)
                VALUES (nextval('seq_tick_id'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                ts,
                ts.strftime("%Y%m%d"),
                tick.get("instrument", inst),
                tick.get("price", 0),
                tick.get("total_vol", 0),
                tick.get("buy_vol", 0),
                tick.get("sell_vol", 0),
                tick.get("delta", 0),
                tick.get("bid", 0),
                tick.get("ask", 0),
                tick.get("bid_vol", 0),
                tick.get("ask_vol", 0),
                tick.get("aggressor", ""),
                tick.get("volume_total", 0),
                tick.get("spread", 0)
            ])
            self.conn.execute("COMMIT")
            return True
        except Exception as e:
            try:
                self.conn.execute("ROLLBACK")
            except:
                pass
            print(f"[DuckDB] Tick 写入错误: {e}")
            return False

    def save_kline(self, inst, period, bar):
        try:
            ts = self._parse_time(bar.get("time", ""))
            self.conn.execute("BEGIN TRANSACTION")
            self.conn.execute("""
                INSERT INTO klines 
                (id, timestamp, date_str, instrument, period, open, high, low, close,
                 volume, buy_vol, sell_vol, delta, tick_count, vwap, cvd, noi)
                VALUES (nextval('seq_kline_id'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                ts,
                ts.strftime("%Y%m%d"),
                inst,
                period,
                bar.get("open", 0),
                bar.get("high", 0),
                bar.get("low", 0),
                bar.get("close", 0),
                bar.get("volume", 0),
                bar.get("buy_vol", 0),
                bar.get("sell_vol", 0),
                bar.get("delta", 0),
                bar.get("tick_count", 1),
                bar.get("vwap", 0),
                bar.get("cvd", 0),
                bar.get("noi", 0)
            ])
            self.conn.execute("COMMIT")
            return True
        except Exception as e:
            try:
                self.conn.execute("ROLLBACK")
            except:
                pass
            print(f"[DuckDB] K线写入错误: {e}")
            return False

    def load_klines(self, inst, period, limit=200):
        try:
            result = self.conn.execute("""
                SELECT strftime(timestamp, '%H:%M') as time, epoch_ms(timestamp) as timestamp_ms,
                       open, high, low, close, volume, buy_vol, sell_vol, delta, tick_count,
                       vwap, cvd, noi
                FROM klines WHERE instrument = ? AND period = ? ORDER BY timestamp DESC LIMIT ?
            """, [inst, period, limit]).fetchall()
            klines = []
            for row in reversed(result):
                klines.append({
                    "time": row[0], "timestamp": row[1],
                    "open": float(row[2]), "high": float(row[3]), "low": float(row[4]), "close": float(row[5]),
                    "volume": int(row[6]), "buy_vol": int(row[7]), "sell_vol": int(row[8]),
                    "delta": int(row[9]), "tick_count": int(row[10]),
                    "vwap": float(row[11]) if row[11] else 0,
                    "cvd": float(row[12]) if row[12] else 0,
                    "noi": float(row[13]) if row[13] else 0,
                })
            return klines
        except Exception as e:
            print(f"[DuckDB] K线加载错误: {e}")
            return []

    def load_ticks(self, inst, limit=500):
        try:
            result = self.conn.execute("""
                SELECT strftime(timestamp, '%H:%M:%S') as time, '0' as millisec, instrument, price,
                       volume as total_vol, buy_vol, sell_vol, delta, bid, ask, bid_vol, ask_vol, aggressor, spread
                FROM ticks WHERE instrument = ? ORDER BY timestamp DESC LIMIT ?
            """, [inst, limit]).fetchall()
            ticks = []
            for row in reversed(result):
                ticks.append({
                    "time": row[0], "millisec": row[1], "instrument": row[2], "price": float(row[3]),
                    "total_vol": int(row[4]), "buy_vol": int(row[5]), "sell_vol": int(row[6]),
                    "delta": int(row[7]), "bid": float(row[8]), "ask": float(row[9]),
                    "bid_vol": int(row[10]), "ask_vol": int(row[11]), "aggressor": row[12],
                    "spread": float(row[13]) if row[13] else 0,
                })
            return ticks
        except Exception as e:
            print(f"[DuckDB] Tick加载错误: {e}")
            return []

    def cleanup_old_data(self, tick_days=3, kline_1m_days=60, kline_other_days=7):
        try:
            now = datetime.now()
            tick_cutoff = (now - timedelta(days=tick_days)).strftime("%Y-%m-%d")
            self.conn.execute("DELETE FROM ticks WHERE date_str < ?", [tick_cutoff.replace("-", "")])
            tick_deleted = self.conn.execute("SELECT changes()").fetchone()[0]
            kline_1m_cutoff = (now - timedelta(days=kline_1m_days)).strftime("%Y-%m-%d")
            self.conn.execute("DELETE FROM klines WHERE period = '1m' AND date_str < ?", [kline_1m_cutoff.replace("-", "")])
            kline_other_cutoff = (now - timedelta(days=kline_other_days)).strftime("%Y-%m-%d")
            self.conn.execute("DELETE FROM klines WHERE period IN ('5m', '15m', '1h') AND date_str < ?", [kline_other_cutoff.replace("-", "")])
            self.conn.execute("VACUUM")
            print(f"[DuckDB] 清理完成，删除 {tick_deleted} 条旧Tick，已VACUUM")
            return tick_deleted
        except Exception as e:
            print(f"[DuckDB] 清理错误: {e}")
            return 0

    def force_flush(self):
        try:
            self.conn.execute("FORCE CHECKPOINT")
            print("[DuckDB] 强制刷盘完成")
        except Exception as e:
            print(f"[DuckDB] 刷盘错误: {e}")

    def get_last_price(self, inst):
        """Get the most recent price for an instrument from ticks table."""
        try:
            result = self.conn.execute("""
                SELECT price FROM ticks 
                WHERE instrument = ? 
                ORDER BY timestamp DESC 
                LIMIT 1
            """, [inst]).fetchone()
            if result and result[0]:
                return float(result[0])
            return None
        except Exception as e:
            print(f"[DuckDB] 获取最新价格错误: {e}")
            return None

    def get_all_last_prices(self):
        """Get the most recent price for all instruments."""
        try:
            result = self.conn.execute("""
                SELECT instrument, price FROM (
                    SELECT instrument, price, 
                           ROW_NUMBER() OVER (PARTITION BY instrument ORDER BY timestamp DESC) as rn
                    FROM ticks
                ) WHERE rn = 1
            """).fetchall()
            return {row[0]: float(row[1]) for row in result if row[1]}
        except Exception as e:
            print(f"[DuckDB] 获取所有最新价格错误: {e}")
            return {}

    def close(self):
        try:
            self.conn.execute("FORCE CHECKPOINT")
            self.conn.close()
            print("[DuckDB] 连接已关闭，数据已持久化")
        except Exception as e:
            print(f"[DuckDB] 关闭错误: {e}")

    def _parse_time(self, time_str):
        try:
            today = datetime.now()
            if len(time_str) == 5:
                return today.replace(
                    hour=int(time_str[:2]), minute=int(time_str[3:5]),
                    second=0, microsecond=0
                )
            elif len(time_str) == 8:
                return today.replace(
                    hour=int(time_str[:2]), minute=int(time_str[3:5]),
                    second=int(time_str[6:8]), microsecond=0
                )
            else:
                return today.replace(second=0, microsecond=0)
        except Exception:
            return datetime.now().replace(second=0, microsecond=0)


# ============ 交易时间工具 ============
DAY_SESSIONS = [("09:00", "10:15"), ("10:30", "11:30"), ("13:30", "15:00")]

INSTRUMENT_NIGHT_SESSIONS = {
    "cu": ("21:00", "01:00"), "al": ("21:00", "01:00"), "zn": ("21:00", "01:00"),
    "pb": ("21:00", "01:00"), "ni": ("21:00", "01:00"), "sn": ("21:00", "01:00"),
    "ss": ("21:00", "01:00"), "ao": ("21:00", "01:00"), "bc": ("21:00", "01:00"),
    "au": ("21:00", "02:30"), "ag": ("21:00", "02:30"), "sc": ("21:00", "02:30"),
    "rb": ("21:00", "23:00"), "hc": ("21:00", "23:00"), "fu": ("21:00", "23:00"),
    "bu": ("21:00", "23:00"), "ru": ("21:00", "23:00"), "sp": ("21:00", "23:00"),
    "br": ("21:00", "23:00"), "nr": ("21:00", "23:00"), "lu": ("21:00", "23:00"),
    "i": ("21:00", "23:00"), "j": ("21:00", "23:00"), "jm": ("21:00", "23:00"),
    "p": ("21:00", "23:00"), "y": ("21:00", "23:00"), "m": ("21:00", "23:00"),
    "rm": ("21:00", "23:00"), "a": ("21:00", "23:00"), "b": ("21:00", "23:00"),
    "c": ("21:00", "23:00"), "cs": ("21:00", "23:00"), "eb": ("21:00", "23:00"),
    "eg": ("21:00", "23:00"), "l": ("21:00", "23:00"), "pp": ("21:00", "23:00"),
    "pvc": ("21:00", "23:00"), "pg": ("21:00", "23:00"),
    "cf": ("21:00", "23:00"), "sr": ("21:00", "23:00"), "ta": ("21:00", "23:00"),
    "ma": ("21:00", "23:00"), "fg": ("21:00", "23:00"), "oi": ("21:00", "23:00"),
    "sa": ("21:00", "23:00"), "sf": ("21:00", "23:00"), "sm": ("21:00", "23:00"),
    "ur": ("21:00", "23:00"), "cj": ("21:00", "23:00"), "ap": ("21:00", "23:00"),
    "pk": ("21:00", "23:00"),
    "lh": None, "jd": None, "ri": None, "lr": None, "jr": None,
    "rs": None, "pm": None, "wh": None, "cy": None,
}


def _time_in_range(time_str, start_str, end_str):
    def parse(t):
        h, m = map(int, t.split(":"))
        return h * 60 + m
    t = parse(time_str)
    s = parse(start_str)
    e = parse(end_str)
    if s <= e:
        return s <= t <= e
    else:
        return t >= s or t <= e


def get_instrument_night_session(inst):
    if not inst:
        return None
    prefix = ""
    for ch in inst.lower():
        if ch.isalpha():
            prefix += ch
        else:
            break
    if prefix in INSTRUMENT_NIGHT_SESSIONS:
        return INSTRUMENT_NIGHT_SESSIONS[prefix]
    if len(prefix) >= 2 and prefix[:2] in INSTRUMENT_NIGHT_SESSIONS:
        return INSTRUMENT_NIGHT_SESSIONS[prefix[:2]]
    if len(prefix) >= 1 and prefix[0] in INSTRUMENT_NIGHT_SESSIONS:
        return INSTRUMENT_NIGHT_SESSIONS[prefix[0]]
    return None


def is_trading_time(inst=None, now=None):
    if now is None:
        now = datetime.now()
    time_str = now.strftime("%H:%M")
    weekday = now.weekday()
    if weekday >= 5:
        return False
    for start, end in DAY_SESSIONS:
        if start <= time_str <= end:
            return True
    night_session = get_instrument_night_session(inst)
    if night_session is None:
        return False
    night_start, night_end = night_session
    return _time_in_range(time_str, night_start, night_end)


def is_trading_time_by_tick(inst, tick_time_str):
    if len(tick_time_str) >= 5:
        time_str = tick_time_str[:5]
    else:
        return is_trading_time(inst)
    now = datetime.now()
    weekday = now.weekday()
    if weekday >= 5:
        return False
    for start, end in DAY_SESSIONS:
        if start <= time_str <= end:
            return True
    night_session = get_instrument_night_session(inst)
    if night_session is None:
        return False
    night_start, night_end = night_session
    return _time_in_range(time_str, night_start, night_end)


# ============ 全局数据存储 ============
class DataStore:
    def __init__(self):
        self.max_active = WINDOW_CONFIG["MAX_ACTIVE"]
        self.active_instruments = set(INSTRUMENTS)
        self._access_time = OrderedDict()

        self.klines = {}
        self.current_bar = {}
        self.orderflow = {}
        self.last_ticks = {}
        self.macd = {}
        self.tick_count = 0

        # [NEW] 技术指标缓存
        self.vwap = {}
        self.cvd = {}
        self.cvd_slope = {}
        self.noi = {}
        self.noi_zscore = {}
        self.volume_zscore = {}
        self.key_levels = {}
        self.spread_history = {}
        self.intraday_seasonality = {}
        # [NEW] 新增指标缓存
        self.volume_profile = {}
        self.order_flow_rate = {}
        self.large_orders = {}
        self.trading_signals = {}

        self.window_mode = WINDOW_CONFIG["DEFAULT_MODE"]
        self.window_instruments = list(INSTRUMENTS)[:self.window_mode]
        self.selected_window = 0

        self.db_manager = DuckDBManager()
        self.calculators = {inst: OrderFlowCalculator() for inst in ALL_INSTRUMENTS}
        self.data_source = "mock"
        self.indicators = TechnicalIndicators()
        self.last_ctp_prices = {}  # Persist last CTP prices for mock mode base

        self._lock = threading.RLock()
        self._init_active_instruments()

    def _init_active_instruments(self):
        for inst in self.active_instruments:
            self.klines[inst] = defaultdict(list)
            self.current_bar[inst] = {}
            self.orderflow[inst] = deque(maxlen=WINDOW_CONFIG["ORDERFLOW_MAXLEN"])
            self.macd[inst] = {}
            self.vwap[inst] = {}
            self.cvd[inst] = {}
            self.cvd_slope[inst] = {}
            self.noi[inst] = {}
            self.noi_zscore[inst] = {}
            self.volume_zscore[inst] = {}
            self.key_levels[inst] = {}
            self.spread_history[inst] = deque(maxlen=100)
            self.intraday_seasonality[inst] = {}
            self.volume_profile[inst] = {}
            self.order_flow_rate[inst] = {}
            self.large_orders[inst] = []
            self.trading_signals[inst] = {}
            self._access_time[inst] = time.time()

    def touch(self, inst):
        with self._lock:
            self._access_time[inst] = time.time()
            self._access_time.move_to_end(inst)

    def set_window_instrument(self, window_idx, inst):
        with self._lock:
            if inst not in ALL_INSTRUMENTS:
                return {"success": False, "message": f"品种 {inst} 不存在", "evicted": None}
            while len(self.window_instruments) <= window_idx:
                self.window_instruments.append(None)
            self.window_instruments[window_idx] = inst
            evicted = None
            if inst not in self.active_instruments:
                if len(self.active_instruments) >= self.max_active:
                    oldest = next(iter(self._access_time))
                    if oldest in self.active_instruments:
                        evicted = oldest
                        self._remove_instrument(oldest)
                self.active_instruments.add(inst)
                self.klines[inst] = defaultdict(list)
                self.current_bar[inst] = {}
                self.orderflow[inst] = deque(maxlen=WINDOW_CONFIG["ORDERFLOW_MAXLEN"])
                self.macd[inst] = {}
                self.vwap[inst] = {}
                self.cvd[inst] = {}
                self.cvd_slope[inst] = {}
                self.noi[inst] = {}
                self.noi_zscore[inst] = {}
                self.volume_zscore[inst] = {}
                self.key_levels[inst] = {}
                self.spread_history[inst] = deque(maxlen=100)
                self.intraday_seasonality[inst] = {}
                # [NEW] 新增指标缓存
                self.volume_profile[inst] = {}
                self.order_flow_rate[inst] = {}
                self.large_orders[inst] = []
                self.trading_signals[inst] = {}
            self._access_time[inst] = time.time()
            self._access_time.move_to_end(inst)
            return {"success": True, "message": f"窗口 {window_idx} -> {inst}", "evicted": evicted}

    def _remove_instrument(self, inst):
        self.active_instruments.discard(inst)
        self.klines.pop(inst, None)
        self.current_bar.pop(inst, None)
        self.orderflow.pop(inst, None)
        self.last_ticks.pop(inst, None)
        self.macd.pop(inst, None)
        self.vwap.pop(inst, None)
        self.cvd.pop(inst, None)
        self.cvd_slope.pop(inst, None)
        self.noi.pop(inst, None)
        self.noi_zscore.pop(inst, None)
        self.volume_zscore.pop(inst, None)
        self.key_levels.pop(inst, None)
        self.spread_history.pop(inst, None)
        self.intraday_seasonality.pop(inst, None)
        self.volume_profile.pop(inst, None)
        self.order_flow_rate.pop(inst, None)
        self.large_orders.pop(inst, None)
        self.trading_signals.pop(inst, None)
        self._access_time.pop(inst, None)
        print(f"[LRU] 淘汰: {inst}，活跃: {len(self.active_instruments)}")

    def set_window_mode(self, mode):
        with self._lock:
            if mode not in [2, 4, 6]:
                return {"success": False, "message": "模式必须是4或6"}
            old_mode = self.window_mode
            self.window_mode = mode
            if mode > old_mode:
                available = [i for i in INSTRUMENTS if i not in self.window_instruments]
                while len(self.window_instruments) < mode and available:
                    self.window_instruments.append(available.pop(0))
            else:
                self.window_instruments = self.window_instruments[:mode]
            return {"success": True, "mode": mode, "windows": self.window_instruments}

    def set_selected_window(self, idx):
        with self._lock:
            if 0 <= idx < len(self.window_instruments):
                self.selected_window = idx
                inst = self.window_instruments[idx]
                if inst:
                    self.touch(inst)
                return {"success": True, "selected_inst": inst, "idx": idx}
            return {"success": False, "message": "窗口索引无效"}

    def load_last_prices_from_db(self):
        """Load last known prices from DuckDB for all instruments."""
        db_prices = self.db_manager.get_all_last_prices()
        for inst, price in db_prices.items():
            if price and price > 0:
                self.last_ctp_prices[inst] = price
        if db_prices:
            print(f"[DataStore] 从数据库加载 {len(db_prices)} 个品种的最新价格")
        return db_prices

    def init_bars(self, source="mock"):
        print(f"初始化历史数据... 活跃品种: {len(self.active_instruments)}")
        self.data_source = source
        self.db_manager.cleanup_old_data()
        # Load last prices from DB before initializing bars
        self.load_last_prices_from_db()
        for inst in self.active_instruments:
            self.init_instrument_bars(inst)
        print("历史数据初始化完成")

    def init_instrument_bars(self, inst):
        base = BASE_PRICES.get(inst, 3000.0)
        klines_1m = self.db_manager.load_klines(inst, "1m", limit=200)
        if klines_1m:
            with self._lock:
                self.klines[inst]["1m"] = klines_1m
            print(f"  [{inst}] DB加载 {len(klines_1m)} 根1分钟K线")
        elif self.data_source == "mock":
            print(f"  [{inst}] Mock生成历史K线...")
            bars = []
            # Use last DB price as base if available
            db_price = self.last_ctp_prices.get(inst)
            price = db_price if db_price and db_price > 0 else base
            now = datetime.now()
            for i in range(100, 0, -1):
                t = now - timedelta(seconds=i * 60)
                change = random.gauss(0, base * 0.002)
                open_p = round(price, 2)
                high_p = round(open_p + abs(random.gauss(0, base * 0.003)), 2)
                low_p = round(open_p - abs(random.gauss(0, base * 0.003)), 2)
                close_p = round(open_p + change, 2)
                vol = random.randint(100, 5000)
                buy_v = int(vol * random.uniform(0.3, 0.7))
                sell_v = vol - buy_v
                delta = buy_v - sell_v
                bar = {
                    "time": t.strftime("%H:%M"), "timestamp": int(t.timestamp() * 1000),
                    "open": open_p, "high": high_p, "low": low_p, "close": close_p,
                    "volume": vol, "buy_vol": buy_v, "sell_vol": sell_v,
                    "delta": delta, "tick_count": 1,
                    "vwap": 0, "cvd": 0, "noi": 0,
                }
                bars.append(bar)
                price = close_p
                # [FIX] Mock模式不写入数据库，只在内存中生成历史数据
            with self._lock:
                self.klines[inst]["1m"] = bars
        else:
            print(f"  [{inst}] CTP模式，等待实时推送...")
        for period_name in ["5m", "15m", "1h"]:
            self._aggregate_from_1m(inst, period_name)
        # [FIX] Also calculate indicators for 1m (not covered by _aggregate_from_1m)
        self._calc_all_indicators(inst, "1m")
        self._calc_macd(inst, "1m")
        hist_ticks = self.db_manager.load_ticks(inst, limit=100)
        if hist_ticks:
            with self._lock:
                self.orderflow[inst].extend(hist_ticks)
            print(f"  [{inst}] DB加载 {len(hist_ticks)} 笔历史Tick")
        print(f"[Init] {inst} 历史数据初始化完成")

    def _aggregate_from_1m(self, inst, period_name):
        seconds = PERIODS.get(period_name, 300)
        with self._lock:
            bars_1m = list(self.klines[inst]["1m"])
        if not bars_1m:
            return
        aggregated = []
        current = None
        today = datetime.now().date()
        for bar in bars_1m:
            try:
                bar_time = datetime.strptime(bar["time"], "%H:%M")
                bar_time = bar_time.replace(year=today.year, month=today.month, day=today.day)
            except:
                continue
            period_minutes = max(seconds // 60, 1)
            agg_minute = (bar_time.minute // period_minutes) * period_minutes
            agg_time = bar_time.replace(minute=agg_minute, second=0, microsecond=0)
            time_key = agg_time.strftime("%H:%M")
            if not current or current["time"] != time_key:
                if current:
                    aggregated.append(current)
                current = {
                    "time": time_key, "timestamp": int(agg_time.timestamp() * 1000),
                    "open": bar["open"], "high": bar["high"], "low": bar["low"], "close": bar["close"],
                    "volume": bar["volume"], "buy_vol": bar["buy_vol"],
                    "sell_vol": bar["sell_vol"], "delta": bar["delta"],
                    "vwap": bar.get("vwap", 0), "cvd": bar.get("cvd", 0), "noi": bar.get("noi", 0),
                }
            else:
                current["high"] = max(current["high"], bar["high"])
                current["low"] = min(current["low"], bar["low"])
                current["close"] = bar["close"]
                current["volume"] += bar["volume"]
                current["buy_vol"] += bar["buy_vol"]
                current["sell_vol"] += bar["sell_vol"]
                current["delta"] += bar["delta"]
        if current:
            aggregated.append(current)
        with self._lock:
            self.klines[inst][period_name] = aggregated[-100:]
        # [FIX] Calculate indicators for aggregated periods too
        self._calc_all_indicators(inst, period_name)
        self._calc_macd(inst, period_name)

    def _calc_all_indicators(self, inst, period):
        with self._lock:
            bars = list(self.klines[inst][period])
        if not bars:
            return
        vwap_values = self.indicators.calc_vwap(bars)
        cvd_values = self.indicators.calc_cvd(bars)
        cvd_slopes = self.indicators.calc_cvd_slope(cvd_values)
        noi_values = self.indicators.calc_noi(bars)
        noi_zscores = self.indicators.calc_noi_zscore(noi_values)
        vol_zscores = self.indicators.calc_volume_zscore(bars)
        key_levels = self.indicators.calc_key_levels(bars)
        seasonality = self.indicators.calc_intraday_seasonality(bars)
        # [NEW] Volume Profile
        volume_profile = self.indicators.calc_volume_profile(bars)
        # [NEW] Order Flow Rate
        order_flow_rate = self.indicators.calc_order_flow_rate(bars)
        with self._lock:
            self.vwap[inst][period] = vwap_values
            self.cvd[inst][period] = cvd_values
            self.cvd_slope[inst][period] = cvd_slopes
            self.noi[inst][period] = noi_values
            self.noi_zscore[inst][period] = noi_zscores
            self.volume_zscore[inst][period] = vol_zscores
            self.key_levels[inst] = key_levels
            self.intraday_seasonality[inst] = seasonality
            # [NEW]
            self.volume_profile[inst] = volume_profile
            self.order_flow_rate[inst][period] = order_flow_rate
            for i, bar in enumerate(bars):
                if i < len(vwap_values):
                    bar["vwap"] = vwap_values[i]
                if i < len(cvd_values):
                    bar["cvd"] = cvd_values[i]
                if i < len(noi_values):
                    bar["noi"] = noi_values[i]
    def _calc_macd(self, inst, period):
        with self._lock:
            bars = list(self.klines[inst][period])
        if len(bars) < 26:
            return
        closes = [b["close"] for b in bars]
        ema12 = self._ema(closes, 12)
        ema26 = self._ema(closes, 26)
        dif = [ema12[i] - ema26[i] for i in range(len(ema12))]
        dea = self._ema(dif, 9)
        macd_hist = [2 * (dif[i] - dea[i]) for i in range(len(dea))]
        warmup = 34
        n = len(bars)
        dif_padded = [None] * max(0, n - len(dif)) + [round(x, 4) for x in dif]
        dea_padded = [None] * max(0, n - len(dea)) + [round(x, 4) for x in dea]
        macd_padded = [None] * max(0, n - len(macd_hist)) + [round(x, 4) for x in macd_hist]
        with self._lock:
            self.macd[inst][period] = {
                "dif": dif_padded, "dea": dea_padded, "macd": macd_padded,
            }

    def get_live_macd(self, inst, period):
        with self._lock:
            bars = list(self.klines[inst][period])
            current = self.current_bar.get(inst, {}).get(period)
        if len(bars) < 26:
            return None
        closes = [b["close"] for b in bars]
        if current:
            closes.append(current["close"])
        ema12 = self._ema(closes, 12)
        ema26 = self._ema(closes, 26)
        dif = [ema12[i] - ema26[i] for i in range(len(ema12))]
        dea = self._ema(dif, 9)
        macd_hist = [2 * (dif[i] - dea[i]) for i in range(len(dea))]
        return {
            "dif": round(dif[-1], 4), "dea": round(dea[-1], 4), "macd": round(macd_hist[-1], 4),
        }

    def _ema(self, data, period):
        if len(data) < period:
            return data
        k = 2 / (period + 1)
        ema = [sum(data[:period]) / period]
        for i in range(period, len(data)):
            ema.append(data[i] * k + ema[-1] * (1 - k))
        result = [ema[0]] * (period - 1) + ema
        return result


    def update_tick(self, inst, tick_data):
        if inst not in self.active_instruments:
            return
        calc = self.calculators.get(inst)
        if not calc:
            calc = OrderFlowCalculator()
            self.calculators[inst] = calc
        flow = calc.on_tick(tick_data)
        if not flow:
            return
        spread = flow.get("spread", 0)
        if spread > 0:
            with self._lock:
                self.spread_history[inst].append(spread)
        tick_time = flow.get("time", "")
        is_trading = is_trading_time_by_tick(inst, tick_time)
        if not tick_time:
            is_trading = is_trading_time(inst)
        with self._lock:
            self.last_ticks[inst] = flow
            if self.data_source == "mock" or is_trading:
                self.orderflow[inst].append(flow)
            self.tick_count += 1
            self._access_time[inst] = time.time()
            self._access_time.move_to_end(inst)
            if self.data_source == "ctp":
                saved = self.db_manager.save_tick(inst, flow)
                if saved and self.tick_count % 1000 == 0:
                    print(f"[DuckDB] 已保存 {self.tick_count} 笔Tick，最新: {inst} @ {tick_time}")
                    # [FIX] 定期执行 CHECKPOINT，避免 WAL 过大
                    try:
                        self.db_manager.conn.execute("CHECKPOINT")
                    except:
                        pass
            if self.data_source == "mock" or is_trading:
                for period_name, seconds in PERIODS.items():
                    self._update_kline(inst, period_name, seconds, flow)

    def _update_kline(self, inst, period_name, seconds, flow):
        tick_time = flow.get("time", "")
        try:
            if len(tick_time) == 8:
                tick_dt = datetime.strptime(tick_time, "%H:%M:%S")
            elif len(tick_time) == 5:
                tick_dt = datetime.strptime(tick_time, "%H:%M")
            else:
                tick_dt = datetime.now()
        except:
            tick_dt = datetime.now()
        today = datetime.now()
        tick_dt = tick_dt.replace(year=today.year, month=today.month, day=today.day)
        period_minutes = max(seconds // 60, 1)
        bar_start = tick_dt.replace(second=0, microsecond=0)
        if period_minutes > 1:
            bar_start = bar_start.replace(minute=(bar_start.minute // period_minutes) * period_minutes)
        bar_key = bar_start.strftime("%H:%M")
        with self._lock:
            current = self.current_bar[inst].get(period_name)
            if not current or current.get("time") != bar_key:
                if current:
                    is_trading = is_trading_time_by_tick(inst, current.get("time", ""))
                    if period_name == "1m" and self.data_source == "ctp" and is_trading:
                        self.db_manager.save_kline(inst, period_name, current)
                    self.klines[inst][period_name].append(current)
                    if len(self.klines[inst][period_name]) > 200:
                        self.klines[inst][period_name].pop(0)
                    if period_name == "1m":
                        for p in ["5m", "15m", "1h"]:
                            self._aggregate_from_1m(inst, p)
                        self._calc_all_indicators(inst, "1m")
                        self._calc_macd(inst, "1m")
                # Carry forward cumulative CVD from previous bar (for ALL periods)
                last_cvd = 0
                if current and current.get("cvd") is not None:
                    last_cvd = current["cvd"]
                else:
                    closed_bars = self.klines.get(inst, {}).get(period_name, [])
                    if closed_bars and len(closed_bars) > 0:
                        last_cvd = closed_bars[-1].get("cvd", 0)
                self.current_bar[inst][period_name] = {
                    "time": bar_key, "timestamp": int(bar_start.timestamp() * 1000),
                    "open": flow["price"], "high": flow["price"], "low": flow["price"],
                    "close": flow["price"], "volume": flow["total_vol"],
                    "buy_vol": flow["buy_vol"], "sell_vol": flow["sell_vol"], "delta": flow["delta"],
                    "vwap": flow["price"], "cvd": last_cvd + flow["delta"],
                    "noi": flow["delta"] / flow["total_vol"] if flow["total_vol"] > 0 else 0,
                }
            else:
                bar = current
                bar["high"] = max(bar["high"], flow["price"])
                bar["low"] = min(bar["low"], flow["price"])
                bar["close"] = flow["price"]
                bar["volume"] += flow["total_vol"]
                bar["buy_vol"] += flow["buy_vol"]
                bar["sell_vol"] += flow["sell_vol"]
                bar["delta"] += flow["delta"]
                # Update CVD and NOI as the bar accumulates ticks
                # CVD is cumulative: add new tick delta to existing cumulative CVD
                bar["cvd"] = bar.get("cvd", 0) + flow["delta"]
                if bar["volume"] > 0:
                    bar["noi"] = round(bar["delta"] / bar["volume"], 4)
                    tp = (bar["high"] + bar["low"] + bar["close"]) / 3
                    bar["vwap"] = round(tp, 2)

    def get_klines(self, inst, period):
        with self._lock:
            if inst not in self.klines or period not in self.klines[inst]:
                return []
            bars = list(self.klines[inst][period])
            current = dict(self.current_bar[inst][period]) if self.current_bar[inst].get(period) else None
        if current:
            bars.append(current)
        return bars[-100:]

    def get_orderflow(self, inst, n=150):
        with self._lock:
            if inst not in self.orderflow:
                return []
            return list(self.orderflow[inst])[-n:]

    def get_macd(self, inst, period):
        with self._lock:
            if inst not in self.macd or period not in self.macd[inst]:
                return {"dif": [], "dea": [], "macd": []}
            macd_data = self.macd[inst][period]
            has_current = bool(self.current_bar.get(inst, {}).get(period))
            bars_count = len(self.klines[inst][period]) if inst in self.klines and period in self.klines[inst] else 0
        if has_current and bars_count >= 100:
            dif = (macd_data["dif"][-99:] + [None]) if len(macd_data["dif"]) >= 99 else macd_data["dif"]
            dea = (macd_data["dea"][-99:] + [None]) if len(macd_data["dea"]) >= 99 else macd_data["dea"]
            macd = (macd_data["macd"][-99:] + [None]) if len(macd_data["macd"]) >= 99 else macd_data["macd"]
        else:
            dif = macd_data["dif"][-100:] if macd_data["dif"] else []
            dea = macd_data["dea"][-100:] if macd_data["dea"] else []
            macd = macd_data["macd"][-100:] if macd_data["macd"] else []
        return {"dif": dif, "dea": dea, "macd": macd}

    def get_indicators(self, inst, period):
        with self._lock:
            bars = list(self.klines[inst][period]) if inst in self.klines and period in self.klines[inst] else []
            current = dict(self.current_bar[inst][period]) if self.current_bar[inst].get(period) else None
            vwap_data = list(self.vwap[inst][period]) if inst in self.vwap and period in self.vwap[inst] else []
            cvd_data = list(self.cvd[inst][period]) if inst in self.cvd and period in self.cvd[inst] else []
            cvd_slope_data = list(self.cvd_slope[inst][period]) if inst in self.cvd_slope and period in self.cvd_slope[inst] else []
            noi_data = list(self.noi[inst][period]) if inst in self.noi and period in self.noi[inst] else []
            noi_z_data = list(self.noi_zscore[inst][period]) if inst in self.noi_zscore and period in self.noi_zscore[inst] else []
            vol_z_data = list(self.volume_zscore[inst][period]) if inst in self.volume_zscore and period in self.volume_zscore[inst] else []
            key_lvls = dict(self.key_levels[inst]) if inst in self.key_levels else {}
            spread_hist = list(self.spread_history[inst]) if inst in self.spread_history else []
            seasonality = dict(self.intraday_seasonality[inst]) if inst in self.intraday_seasonality else {}
            # [NEW]
            vp_data = dict(self.volume_profile[inst]) if inst in self.volume_profile else {}
            ofr_data = list(self.order_flow_rate[inst][period]) if inst in self.order_flow_rate and period in self.order_flow_rate[inst] else []
            ticks = list(self.orderflow.get(inst, deque())) if inst in self.orderflow else []

        # 大单检测
        large_orders, large_order_stats = self.indicators.detect_large_orders(ticks)

        # 高置信度信号计算
        all_indicators = {
            "vwap": vwap_data,
            "cvd": cvd_data,
            "cvd_slope": cvd_slope_data,
            "noi": noi_data,
            "noi_zscore": noi_z_data,
            "volume_zscore": vol_z_data,
            "key_levels": key_lvls,
            "volume_profile": vp_data,
            "order_flow_rate": ofr_data,
        }

        signal_result = self.indicators.calc_signal_confidence(
            all_indicators, current, self.last_ticks.get(inst)
        )

        # 更新交易信号缓存
        with self._lock:
            self.trading_signals[inst] = signal_result
            self.large_orders[inst] = large_orders[-20:] if large_orders else []

        if current and bars:
            vwap_data = vwap_data[-99:] + [current.get("vwap", current["close"])]
            cvd_data = cvd_data[-99:] + [current.get("cvd", 0)]
            cvd_slope_data = cvd_slope_data[-99:] + [cvd_slope_data[-1] if cvd_slope_data else 0]
            noi_data = noi_data[-99:] + [current.get("noi", 0)]
            noi_z_data = noi_z_data[-99:] + [noi_z_data[-1] if noi_z_data else 0]
            vol_z_data = vol_z_data[-99:] + [vol_z_data[-1] if vol_z_data else 0]
            ofr_data = ofr_data[-99:] + [ofr_data[-1] if ofr_data else 0]

        current_spread = 0
        spread_spike = False
        spread_zscore = 0
        if spread_hist:
            current_spread = spread_hist[-1]
            is_spike, zscore = self.indicators.detect_spread_spike(list(spread_hist)[:-1], current_spread)
            spread_spike = is_spike
            spread_zscore = zscore

        return {
            "vwap": vwap_data[-100:] if vwap_data else [],
            "cvd": cvd_data[-100:] if cvd_data else [],
            "cvd_slope": cvd_slope_data[-100:] if cvd_slope_data else [],
            "noi": noi_data[-100:] if noi_data else [],
            "noi_zscore": noi_z_data[-100:] if noi_z_data else [],
            "volume_zscore": vol_z_data[-100:] if vol_z_data else [],
            "key_levels": key_lvls,
            "current_spread": current_spread,
            "spread_spike": spread_spike,
            "spread_zscore": spread_zscore,
            "seasonality": seasonality,
            # [NEW]
            "volume_profile": vp_data,
            "order_flow_rate": ofr_data[-100:] if ofr_data else [],
            "large_orders": large_orders[-10:] if large_orders else [],
            "large_order_stats": large_order_stats,
            "trading_signal": signal_result,
        }
    def get_window_config(self):
        with self._lock:
            return {
                "mode": self.window_mode,
                "windows": list(self.window_instruments),
                "selected": self.selected_window,
                "selected_inst": self.window_instruments[self.selected_window] if self.selected_window < len(self.window_instruments) else None,
            }

    def set_data_source(self, source):
        self.data_source = source
        print(f"[DataStore] 数据源: {source}")

    def close(self):
        self.db_manager.close()


store = DataStore()


# ============ Mock 模拟行情引擎 ============
class MockEngine:
    def __init__(self):
        self.prices = {inst: BASE_PRICES.get(inst, 3000.0) for inst in store.active_instruments}
        self.volumes = {inst: 0 for inst in store.active_instruments}
        self.running = False
        # [NEW] Price momentum state per instrument
        self.momentum = {inst: 0.0 for inst in store.active_instruments}
        self.last_prices = {inst: BASE_PRICES.get(inst, 3000.0) for inst in store.active_instruments}
        self.trend_direction = {inst: random.choice([-1, 1]) for inst in store.active_instruments}
        self.trend_duration = {inst: random.randint(20, 100) for inst in store.active_instruments}
        self.tick_counter = {inst: 0 for inst in store.active_instruments}

    def generate_tick(self, inst):
        # Use the LAST MOCK PRICE as base, not the original base price
        last_price = self.last_prices.get(inst, BASE_PRICES.get(inst, 3000.0))
        base = last_price  # THIS IS THE KEY FIX: use current price, not fixed base

        self.tick_counter[inst] += 1
        tc = self.tick_counter[inst]

        # Trend regime: periodically switch direction
        if tc >= self.trend_duration.get(inst, 50):
            self.trend_direction[inst] = random.choice([-1, 1])
            self.trend_duration[inst] = random.randint(30, 150)
            self.tick_counter[inst] = 0
            # Occasionally inject a spike
            if random.random() < 0.15:
                self.momentum[inst] += self.trend_direction[inst] * base * random.uniform(0.003, 0.012)

        # Mean-reverting momentum with trend bias
        trend_force = self.trend_direction[inst] * base * 0.0003
        mean_revert = -self.momentum[inst] * 0.08
        noise = random.gauss(0, base * 0.0015)  # wider noise for visible movement

        # Random spike injection (~5% chance per tick)
        spike = 0
        if random.random() < 0.05:
            spike = random.choice([-1, 1]) * base * random.uniform(0.005, 0.02)

        self.momentum[inst] = self.momentum[inst] * 0.92 + trend_force + mean_revert + noise + spike

        price = round(base + self.momentum[inst], 2)

        # Allow wider range before clamping (2% instead of 1%)
        original_base = BASE_PRICES.get(inst, 3000.0)
        price = max(original_base * 0.85, min(original_base * 1.25, price))

        self.prices[inst] = price
        self.last_prices[inst] = price
        spread = max(abs(price) * 0.0002, 0.01)
        bid = round(price - spread, 2)
        ask = round(price + spread, 2)
        vol = random.randint(5, 80) if random.random() > 0.15 else 0
        self.volumes[inst] += vol
        r = random.random()
        if r < 0.25 and vol > 0:
            price = ask
            buy_vol = vol
            sell_vol = 0
            aggressor = "BUY"
        elif r < 0.50 and vol > 0:
            price = bid
            buy_vol = 0
            sell_vol = vol
            aggressor = "SELL"
        else:
            # Directional bias to create trending CVD
            bias = random.choice([0.35, 0.65])
            buy_vol = int(vol * bias)
            sell_vol = vol - buy_vol
            aggressor = "MIX" if vol > 0 else "MIX"
        return {
            "time": datetime.now().strftime("%H:%M:%S"),
            "millisec": datetime.now().microsecond // 1000,
            "instrument": inst,
            "price": price,
            "total_vol": self.volumes[inst],
            "buy_vol": buy_vol,
            "sell_vol": sell_vol,
            "delta": buy_vol - sell_vol,
            "bid": bid,
            "ask": ask,
            "bid_vol": random.randint(50, 500),
            "ask_vol": random.randint(50, 500),
            "aggressor": aggressor,
            "volume_total": self.volumes[inst],
            "spread": round(ask - bid, 4),
        }

    def start(self):
        self.running = True
        def run():
            print("[Mock] 模拟行情引擎启动")
            while self.running:
                for inst in list(store.active_instruments):
                    if inst not in self.prices:
                        # Use last DB price if available, else fixed base price
                        db_price = store.last_ctp_prices.get(inst)
                        init_price = db_price if db_price and db_price > 0 else BASE_PRICES.get(inst, 3000.0)
                        self.prices[inst] = init_price
                        self.last_prices[inst] = init_price
                        self.volumes[inst] = 0
                        # [NEW] Initialize momentum state for new instrument
                        if inst not in self.momentum:
                            self.momentum[inst] = 0.0
                            self.trend_direction[inst] = random.choice([-1, 1])
                            self.trend_duration[inst] = random.randint(20, 100)
                            self.tick_counter[inst] = 0
                    tick = self.generate_tick(inst)
                    if tick is not None:
                        store.update_tick(inst, tick)
                time.sleep(0.5)
        threading.Thread(target=run, daemon=True).start()

    def stop(self):
        if not self.running:
            return
        self.running = False
        print("[Mock] 模拟行情引擎停止")


engine = MockEngine()


# ============ CTP 真实行情引擎 ============
class CtpMdSpi(mdapi.CThostFtdcMdSpi):
    def __init__(self, store, api):
        mdapi.CThostFtdcMdSpi.__init__(self)
        self.store = store
        self.api = api
        self.tick_count = 0
        self.connected = False
        self.logged_in = False

    def OnFrontConnected(self):
        print("[CTP] 前置机连接成功")
        self.connected = True
        try:
            req = mdapi.CThostFtdcReqUserLoginField()
            req.BrokerID = SIMNOW_CONFIG.get("BrokerID", "9999")
            req.UserID = SIMNOW_CONFIG.get("UserID", "")
            req.Password = SIMNOW_CONFIG.get("Password", "")
            print(f"[CTP] 登录: BrokerID={req.BrokerID}, UserID={req.UserID}")
            ret = self.api.ReqUserLogin(req, 0)
            print(f"[CTP] 登录请求发送，返回值: {ret}")
        except Exception as e:
            print(f"[CTP] 登录请求失败: {e}")
            import traceback
            traceback.print_exc()

    def OnFrontDisconnected(self, nReason):
        print(f"[CTP] 前置机断开: {nReason}")
        self.connected = False
        self.logged_in = False

    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
        if pRspInfo and pRspInfo.ErrorID == 0:
            print("[CTP] 登录成功")
            self.logged_in = True
            print(f"[CTP] 订阅合约: {', '.join(INSTRUMENTS)}")
            try:
                inst_list = [i.encode('utf-8') for i in INSTRUMENTS]
                ret = self.api.SubscribeMarketData(inst_list, len(inst_list))
                print(f"[CTP] 订阅请求发送，返回值: {ret}")
            except Exception as e:
                print(f"[CTP] 订阅失败: {e}")
                import traceback
                traceback.print_exc()
        else:
            err = pRspInfo.ErrorMsg if pRspInfo else "未知错误"
            error_id = pRspInfo.ErrorID if pRspInfo else -1
            print(f"[CTP] 登录失败: [ErrorID={error_id}] {err}")

    def OnRtnDepthMarketData(self, pDepthMarketData):
        self.tick_count += 1
        if self.tick_count == 1:
            print("[CTP] 收到首笔行情数据！")
        tick_data = {
            "InstrumentID": pDepthMarketData.InstrumentID,
            "UpdateTime": pDepthMarketData.UpdateTime,
            "UpdateMillisec": pDepthMarketData.UpdateMillisec,
            "LastPrice": pDepthMarketData.LastPrice,
            "Volume": pDepthMarketData.Volume,
            "BidPrice1": pDepthMarketData.BidPrice1,
            "AskPrice1": pDepthMarketData.AskPrice1,
            "BidVolume1": pDepthMarketData.BidVolume1,
            "AskVolume1": pDepthMarketData.AskVolume1,
        }
        inst = pDepthMarketData.InstrumentID
        # Cache last price in memory for quick mock access (also persisted to DB via update_tick)
        if pDepthMarketData.LastPrice and pDepthMarketData.LastPrice > 0:
            store.last_ctp_prices[inst] = float(pDepthMarketData.LastPrice)
        if inst in store.active_instruments:
            store.update_tick(inst, tick_data)
        if self.tick_count <= 10 or self.tick_count % 100 == 0:
            print(f"[CTP] #{self.tick_count} {inst} {pDepthMarketData.LastPrice}")

    def OnRspError(self, pRspInfo, nRequestID, bIsLast):
        err = pRspInfo.ErrorMsg if pRspInfo else "未知错误"
        print(f"[CTP] 错误: {err}")

    def OnRspSubMarketData(self, pSpecificInstrument, pRspInfo, nRequestID, bIsLast):
        if pRspInfo and pRspInfo.ErrorID == 0:
            print(f"[CTP] 订阅确认: {pSpecificInstrument.InstrumentID}")
        else:
            err = pRspInfo.ErrorMsg if pRspInfo else "未知错误"
            print(f"[CTP] 订阅失败: {err}")


class CTPEngine:
    def __init__(self):
        self.running = False
        self.api = None
        self.spi = None

    def start(self):
        print("=" * 60)
        print("[CTP] 启动真实行情引擎")
        print("=" * 60)
        userid = SIMNOW_CONFIG.get("UserID", "")
        password = SIMNOW_CONFIG.get("Password", "")
        if not userid or "您的" in userid or not password or "您的" in password:
            print("CTP 账号未配置！")
            raise ValueError("CTP 账号未配置")
        print(f"[CTP] 账号: {userid}")
        print(f"[CTP] 监控合约: {', '.join(INSTRUMENTS)}")
        try:
            flow_path = "./flow_md/"
            os.makedirs(flow_path, exist_ok=True)
            self.api = mdapi.CThostFtdcMdApi.CreateFtdcMdApi(flow_path)
            self.spi = CtpMdSpi(store, self.api)
            self.api.RegisterSpi(self.spi)
            front = get_front_address("simnow_7x24")["md"]
            print(f"[CTP] 前置机: {front}")
            self.api.RegisterFront(front)
            self.api.Init()
            self.running = True
            print("[CTP] 引擎初始化完成，等待连接...")
            wait_count = 0
            while wait_count < 20 and not self.spi.connected:
                time.sleep(0.5)
                wait_count += 1
            if not self.spi.connected:
                print("[CTP] 前置机连接超时")
                return False
            wait_count = 0
            while wait_count < 20 and not self.spi.logged_in:
                time.sleep(0.5)
                wait_count += 1
            if not self.spi.logged_in:
                print("[CTP] 登录超时")
                return False
            print("[CTP] 连接和登录完成")
            return True
        except Exception as e:
            print(f"[CTP] 启动失败: {e}")
            import traceback
            traceback.print_exc()
            raise

    def stop(self):
        if not self.running:
            return
        if self.api:
            self.api.Release()
        self.running = False
        print("[CTP] 引擎停止")


ctp_engine = CTPEngine()


# ============ 数据源管理 ============
class DataSourceManager:
    def __init__(self):
        self.current = "mock"

    def switch(self, source):
        if source == self.current:
            return {"status": "already", "source": source}
        if source == "mock":
            ctp_engine.stop()
            engine.start()
            store.set_data_source("mock")
            self.current = "mock"
            return {"status": "ok", "source": "mock"}
        elif source == "ctp":
            engine.stop()
            try:
                ctp_engine.start()
                store.set_data_source("ctp")
                self.current = "ctp"
                return {"status": "ok", "source": "ctp"}
            except Exception as e:
                print(f"[DSM] CTP启动失败: {e}")
                engine.start()
                return {"status": "error", "msg": str(e), "fallback": "mock"}
        return {"status": "error", "msg": f"未知数据源: {source}"}

    def get_status(self):
        return {"source": self.current, "tick_count": store.tick_count}


dsm = DataSourceManager()


# ============ WebSocket 连接管理 ============
class ConnectionManager:
    def __init__(self):
        self.active_connections = []
        self._lock = threading.RLock()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        with self._lock:
            self.active_connections.append(websocket)
        print(f"[WS] 连接建立，当前 {len(self.active_connections)} 个客户端")

    def disconnect(self, websocket: WebSocket):
        with self._lock:
            if websocket in self.active_connections:
                self.active_connections.remove(websocket)
        print(f"[WS] 断开，当前 {len(self.active_connections)} 个客户端")

    async def broadcast(self, message: dict):
        try:
            msg = json.dumps(message)
        except Exception as e:
            print(f"[WS] JSON序列化失败: {e}")
            return
        dead = []
        with self._lock:
            connections = list(self.active_connections)
        if not connections:
            return
        for connection in connections:
            try:
                await asyncio.wait_for(connection.send_text(msg), timeout=2.0)
            except asyncio.TimeoutError:
                print(f"[WS] 发送超时，标记为死亡连接")
                dead.append(connection)
            except Exception as e:
                print(f"[WS] 发送失败: {e}")
                dead.append(connection)
        for connection in dead:
            self.disconnect(connection)

    async def send_to(self, websocket: WebSocket, message: dict):
        try:
            await websocket.send_text(json.dumps(message))
        except:
            self.disconnect(websocket)


manager = ConnectionManager()


async def data_pusher():
    print("[Push] 数据推送启动")
    push_count = 0
    try:
        while True:
            await asyncio.sleep(1)
            push_count += 1
            if not manager.active_connections:
                if push_count % 10 == 0:
                    print(f"[Push] 等待客户端... (总Tick:{store.tick_count})")
                continue
            try:
                with store._lock:
                    main_inst = None
                    if store.selected_window < len(store.window_instruments):
                        main_inst = store.window_instruments[store.selected_window]
                    window_config = store.get_window_config()
                    active_insts = list(store.active_instruments)
                    last_ticks_copy = dict(store.last_ticks)
                    orderflow = []
                    if main_inst and main_inst in store.active_instruments:
                        orderflow = list(store.orderflow.get(main_inst, deque()))[-500:]
                    data_source = dsm.current

                of_count = len(orderflow)
                of_time_range = ""
                if orderflow and len(orderflow) > 0:
                    first_time = orderflow[0].get("time", "--")
                    last_time = orderflow[-1].get("time", "--")
                    of_time_range = first_time + " ~ " + last_time

                kline_update = {}
                with store._lock:
                    all_window_insts = set(store.window_instruments)
                    all_window_insts.update(store.active_instruments)
                    for inst_key in all_window_insts:
                        if not inst_key:
                            continue
                        kline_update[inst_key] = {}
                        for period_name in PERIODS.keys():
                            current = store.current_bar.get(inst_key, {}).get(period_name)
                            if current:
                                bar_data = dict(current)
                                live_macd = store.get_live_macd(inst_key, period_name)
                                if live_macd:
                                    bar_data["live_macd"] = live_macd
                                kline_update[inst_key][period_name] = bar_data

                data = {
                    "type": "update",
                    "timestamp": int(time.time() * 1000),
                    "instruments": {},
                    "selected_orderflow": orderflow,
                    "selected_inst": main_inst,
                    "selected_name": INSTRUMENT_NAMES.get(main_inst, main_inst) if main_inst else "--",
                    "window_config": window_config,
                    "data_source": data_source,
                    "orderflow_stats": {
                        "count": of_count,
                        "time_range": of_time_range,
                        "max_display": 150
                    },
                    "kline_update": kline_update,
                }

                for inst in active_insts:
                    tick = last_ticks_copy.get(inst)
                    if tick:
                        data["instruments"][inst] = {
                            "last_price": tick["price"],
                            "volume": tick.get("volume_total", 0),
                            "delta": tick["delta"],
                            "spread": tick.get("spread", 0),
                            "bid": tick.get("bid", 0),
                            "ask": tick.get("ask", 0),
                        }

                inst_count = len(data["instruments"])
                of_count = len(data["selected_orderflow"])
                if push_count <= 5 or push_count % 10 == 0:
                    print(f"[Push] #{push_count} 推送 {inst_count}个品种 {of_count}笔订单流 Tick:{store.tick_count}")

                await manager.broadcast(data)
            except asyncio.CancelledError:
                print("[Push] 数据推送任务已取消")
                raise  # 重新抛出以便上层捕获
    except Exception as e:
        print(f"[Push] 推送异常 (push #{push_count}): {e}")
        import traceback
        traceback.print_exc()


# ============ API 路由 ============
@app.get("/", response_class=HTMLResponse)
async def index():
    with open("page.html", "r", encoding="utf-8") as f:
        html = f.read()
    config_json = json.dumps({
        "all_instruments": ALL_INSTRUMENTS,
        "instrument_names": ALL_INSTRUMENT_NAMES,
        "periods": list(PERIODS.keys()),
        "default_mode": WINDOW_CONFIG["DEFAULT_MODE"],
        "default_windows": INSTRUMENTS,
    })
    html = html.replace("$SERVER_CONFIG", config_json)
    html = html.replace("$INSTRUMENTS_JSON", json.dumps(INSTRUMENTS))
    html = html.replace("$INSTRUMENT_NAMES_JSON", json.dumps(INSTRUMENT_NAMES))
    html = html.replace("$PERIODS_JSON", json.dumps(list(PERIODS.keys())))
    return HTMLResponse(content=html)


@app.get("/api/klines")
async def api_klines(inst: str = "rb2510", period: str = "1m"):
    print(f"[API] K线请求: {inst} {period}")
    if inst not in store.active_instruments:
        if inst in ALL_INSTRUMENTS:
            result = store.set_window_instrument(0, inst)
            if result["success"]:
                store.init_instrument_bars(inst)
                # set_window_instrument 已经设置了 _access_time 和 active_instruments
                if inst not in engine.prices:
                    engine.prices[inst] = BASE_PRICES.get(inst, 3000.0)
                    engine.volumes[inst] = 0
            else:
                return {
                    "instrument": inst, "period": period,
                    "klines": [], "macd": {"dif": [], "dea": [], "macd": []},
                    "indicators": {},
                    "error": f"初始化失败: {result.get('message', '未知错误')}",
                }
        else:
            return {
                "instrument": inst, "period": period,
                "klines": [], "macd": {"dif": [], "dea": [], "macd": []},
                "indicators": {},
                "error": "品种不存在",
            }
    klines = store.get_klines(inst, period)
    macd = store.get_macd(inst, period)
    indicators = store.get_indicators(inst, period)
    print(f"[API] 返回: {inst} {period}, {len(klines)} 根K线")
    return {
        "instrument": inst, "period": period,
        "klines": klines, "macd": macd,
        "indicators": indicators,
    }


@app.get("/api/instruments")
async def api_instruments():
    return {
        "instruments": ALL_INSTRUMENTS,
        "names": ALL_INSTRUMENT_NAMES,
        "active": list(store.active_instruments),
    }


@app.get("/api/window/config")
async def api_window_config():
    return store.get_window_config()


@app.post("/api/window/select")
async def api_window_select(idx: int = 0):
    return store.set_selected_window(idx)


@app.post("/api/window/mode")
async def api_window_mode(mode: int = 2):
    return store.set_window_mode(mode)


@app.post("/api/window/instrument")
async def api_window_instrument(window_idx: int = 0, inst: str = "rb2510"):
    result = store.set_window_instrument(window_idx, inst)
    if result["success"]:
        if not store.klines[inst].get("1m"):
            store.init_instrument_bars(inst)
        if inst not in engine.prices:
            engine.prices[inst] = BASE_PRICES.get(inst, 3000.0)
            engine.volumes[inst] = 0
    return result


@app.post("/api/select")
async def api_select(inst: str = "rb2510"):
    if inst in store.active_instruments:
        store.selected_instrument = inst
        print(f"[API] 选择品种: {inst}")
    return {"selected": getattr(store, 'selected_instrument', inst)}


@app.get("/api/source")
async def api_get_source():
    return dsm.get_status()


@app.post("/api/source")
async def api_set_source(source: str = "mock"):
    return dsm.switch(source)


# ============ 外部读取 API ============
@app.get("/api/read/last")
async def api_read_last(
    table: str = "ticks",
    instrument: str = Query(default=""),
    n: int = Query(default=10, ge=1, le=1000),
    period: str = Query(default=""),
    order_by: str = Query(default="timestamp DESC"),
):
    try:
        if table not in ("ticks", "klines"):
            return {"success": False, "error": "table 必须是 ticks 或 klines"}
        conditions = []
        params = []
        if instrument:
            conditions.append("instrument = ?")
            params.append(instrument)
        if table == "klines" and period:
            conditions.append("period = ?")
            params.append(period)
        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
        sql = f"SELECT * FROM {table} {where_clause} ORDER BY {order_by} LIMIT {n}"
        result = store.db_manager.conn.execute(sql, params).fetchdf()
        data = result.to_dict(orient='records')
        for row in data:
            for key, val in row.items():
                if hasattr(val, 'item'):
                    row[key] = val.item()
        return {"success": True, "table": table, "instrument": instrument or "all", "period": period or "all", "count": len(data), "data": data}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/read/klines")
async def api_read_klines(
    instrument: str = Query(...),
    period: str = Query(default="1m"),
    limit: int = Query(default=200, ge=1, le=5000),
    start_time: str = Query(default=""),
    end_time: str = Query(default=""),
):
    try:
        conditions = ["instrument = ?", "period = ?"]
        params = [instrument, period]
        if start_time:
            conditions.append("timestamp >= ?")
            params.append(start_time)
        if end_time:
            conditions.append("timestamp <= ?")
            params.append(end_time)
        where_clause = "WHERE " + " AND ".join(conditions)
        sql = f"""
            SELECT strftime(timestamp, '%H:%M') as time, epoch_ms(timestamp) as timestamp_ms,
                   open, high, low, close, volume, buy_vol, sell_vol, delta, tick_count,
                   vwap, cvd, noi
            FROM klines {where_clause} ORDER BY timestamp DESC LIMIT {limit}
        """
        result = store.db_manager.conn.execute(sql, params).fetchall()
        klines = []
        for row in reversed(result):
            klines.append({
                "time": row[0], "timestamp": row[1],
                "open": float(row[2]), "high": float(row[3]), "low": float(row[4]), "close": float(row[5]),
                "volume": int(row[6]), "buy_vol": int(row[7]), "sell_vol": int(row[8]),
                "delta": int(row[9]), "tick_count": int(row[10]),
                "vwap": float(row[11]) if row[11] else 0,
                "cvd": float(row[12]) if row[12] else 0,
                "noi": float(row[13]) if row[13] else 0,
            })
        return {"success": True, "instrument": instrument, "period": period, "count": len(klines), "klines": klines}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/read/ticks")
async def api_read_ticks(
    instrument: str = Query(...),
    limit: int = Query(default=100, ge=1, le=5000),
    start_time: str = Query(default=""),
    end_time: str = Query(default=""),
):
    try:
        conditions = ["instrument = ?"]
        params = [instrument]
        if start_time:
            conditions.append("timestamp >= ?")
            params.append(start_time)
        if end_time:
            conditions.append("timestamp <= ?")
            params.append(end_time)
        where_clause = "WHERE " + " AND ".join(conditions)
        sql = f"""
            SELECT strftime(timestamp, '%H:%M:%S') as time, instrument, price, volume,
                   buy_vol, sell_vol, delta, bid, ask, aggressor, spread
            FROM ticks {where_clause} ORDER BY timestamp DESC LIMIT {limit}
        """
        result = store.db_manager.conn.execute(sql, params).fetchall()
        ticks = []
        for row in reversed(result):
            ticks.append({
                "time": row[0], "instrument": row[1], "price": float(row[2]), "volume": int(row[3]),
                "buy_vol": int(row[4]), "sell_vol": int(row[5]), "delta": int(row[6]),
                "bid": float(row[7]), "ask": float(row[8]), "aggressor": row[9],
                "spread": float(row[10]) if row[10] else 0,
            })
        return {"success": True, "instrument": instrument, "count": len(ticks), "ticks": ticks}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/read/summary")
async def api_read_summary(date_str: str = Query(default="")):
    try:
        if not date_str:
            date_str = datetime.now().strftime("%Y%m%d")
        tick_stats = store.db_manager.conn.execute("""
            SELECT instrument, COUNT(*) as tick_count, MIN(price) as min_price,
                   MAX(price) as max_price, AVG(price) as avg_price,
                   SUM(volume) as total_volume, SUM(delta) as net_delta
            FROM ticks WHERE date_str = ? GROUP BY instrument ORDER BY tick_count DESC
        """, [date_str]).fetchdf()
        kline_stats = store.db_manager.conn.execute("""
            SELECT instrument, period, COUNT(*) as bar_count
            FROM klines WHERE date_str = ? GROUP BY instrument, period ORDER BY instrument, period
        """, [date_str]).fetchdf()
        return {"success": True, "date": date_str,
                "tick_summary": tick_stats.to_dict(orient='records'),
                "kline_summary": kline_stats.to_dict(orient='records')}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/read/query")
async def api_read_query(
    sql: str = Query(...),
    limit: int = Query(default=1000, ge=1, le=10000),
):
    try:
        sql_clean = sql.strip().upper()
        if not sql_clean.startswith("SELECT"):
            return {"success": False, "error": "只允许 SELECT 查询"}
        if "LIMIT" not in sql_clean:
            sql = sql.strip().rstrip(";") + f" LIMIT {limit}"
        result = store.db_manager.conn.execute(sql).fetchdf()
        data = result.to_dict(orient='records')
        for row in data:
            for key, val in row.items():
                if hasattr(val, 'item'):
                    row[key] = val.item()
        return {"success": True, "sql": sql, "count": len(data), "data": data}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/read/dbinfo")
async def api_read_dbinfo():
    try:
        tables = store.db_manager.conn.execute("SHOW TABLES").fetchdf()
        tick_count = store.db_manager.conn.execute("SELECT COUNT(*) FROM ticks").fetchone()[0]
        kline_count = store.db_manager.conn.execute("SELECT COUNT(*) FROM klines").fetchone()[0]
        db_size = os.path.getsize(store.db_manager.db_path)
        latest_tick = store.db_manager.conn.execute("SELECT MAX(timestamp) FROM ticks").fetchone()[0]
        return {"success": True, "db_path": store.db_manager.db_path,
                "db_size_mb": round(db_size / 1024 / 1024, 2),
                "tables": tables.to_dict(orient='records'),
                "tick_count": tick_count, "kline_count": kline_count,
                "latest_tick_time": str(latest_tick) if latest_tick else None,
                "active_instruments": list(store.active_instruments),
                "data_source": dsm.current}
    except Exception as e:
        return {"success": False, "error": str(e)}



# ============ WebSocket 路由 ============
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        initial_data = {
            "type": "init",
            "timestamp": int(time.time() * 1000),
            "instruments": {},
            "selected_orderflow": [],
            "window_config": store.get_window_config(),
            "all_instruments": ALL_INSTRUMENTS,
            "instrument_names": ALL_INSTRUMENT_NAMES,
            "data_source": dsm.current,
        }
        for inst in store.active_instruments:
            tick = store.last_ticks.get(inst)
            if tick:
                initial_data["instruments"][inst] = {
                    "last_price": tick["price"],
                    "volume": tick.get("volume_total", 0),
                    "delta": tick["delta"],
                    "spread": tick.get("spread", 0),
                    "bid": tick.get("bid", 0),
                    "ask": tick.get("ask", 0),
                }
        main_inst = None
        with store._lock:
            if store.selected_window < len(store.window_instruments):
                main_inst = store.window_instruments[store.selected_window]
        if main_inst and main_inst in store.active_instruments:
            initial_data["selected_orderflow"] = store.get_orderflow(main_inst, 150)
            initial_data["selected_inst"] = main_inst
            initial_data["selected_name"] = INSTRUMENT_NAMES.get(main_inst, main_inst)
        await websocket.send_text(json.dumps(initial_data))
        print("[WS] 发送初始数据")
    except Exception as e:
        print(f"[WS] 初始数据发送失败: {e}")

    try:
        while True:
            msg = await websocket.receive_text()
            if msg:
                try:
                    data = json.loads(msg)
                    action = data.get('action')
                    print(f"[WS] 收到客户端消息: action={action}")
                    if action == 'select_window':
                        idx = data.get('window_idx', 0)
                        print(f"[WS] 处理 select_window: idx={idx}")
                        result = store.set_selected_window(idx)
                        await manager.send_to(websocket, {
                            "type": "ack", "action": "select_window", "result": result
                        })
                    elif action == 'change_instrument':
                        window_idx = data.get('window_idx', 0)
                        inst = data.get('instrument')
                        print(f"[WS] 处理 change_instrument: window={window_idx}, inst={inst}")
                        if inst:
                            result = store.set_window_instrument(window_idx, inst)
                            if result["success"]:
                                if not store.klines[inst].get("1m"):
                                    store.init_instrument_bars(inst)
                                if inst not in engine.prices:
                                    engine.prices[inst] = BASE_PRICES.get(inst, 3000.0)
                                    engine.volumes[inst] = 0
                            await manager.send_to(websocket, {
                                "type": "ack", "action": "change_instrument", "result": result
                            })
                    elif action == 'change_mode':
                        mode = data.get('mode', 2)
                        print(f"[WS] 处理 change_mode: mode={mode}")
                        result = store.set_window_mode(mode)
                        await manager.send_to(websocket, {
                            "type": "ack", "action": "change_mode", "result": result
                        })
                    elif action == 'get_config':
                        print(f"[WS] 处理 get_config")
                        await manager.send_to(websocket, {
                            "type": "config",
                            "window_config": store.get_window_config(),
                            "active_instruments": list(store.active_instruments),
                            "data_source": dsm.current,
                        })
                    elif action == 'switch_source':
                        source = data.get('source', 'mock')
                        print(f"[WS] 处理 switch_source: source={source}")
                        result = dsm.switch(source)
                        await manager.broadcast({
                            "type": "source_changed",
                            "source": result.get("source", source),
                            "timestamp": int(time.time() * 1000),
                        })
                    elif action == 'ping':
                        print(f"[WS] 收到 ping，回复 pong")
                        await manager.send_to(websocket, {"type": "pong"})
                    else:
                        print(f"[WS] 未知 action: {action}")
                except Exception as e:
                    print(f"[WS] 消息处理错误: {e}")
                    import traceback
                    traceback.print_exc()
    except WebSocketDisconnect:
        print("[WS] 客户端断开连接")
        manager.disconnect(websocket)
    except Exception as e:
        print(f"[WS] WebSocket 异常: {e}")
        import traceback
        traceback.print_exc()
        manager.disconnect(websocket)


# ============ Lifespan (replaces deprecated @app.on_event) ============
def check_port_available(port=8080):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("0.0.0.0", port))
        s.close()
        return True
    except socket.error:
        return False


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', default='mock', choices=['mock', 'ctp'])
    parser.add_argument('--port', type=int, default=8080, help='监听端口')
    args = parser.parse_args()

    if not check_port_available(args.port):
        print(f"端口 {args.port} 已被占用！")
        print("请执行以下命令释放端口:")
        print("  Windows: netstat -ano | findstr :8080")
        print("  Linux/Mac: lsof -ti:8080 | xargs kill -9")
        print(f"或者使用其他端口: python web_server.py --port=8081")
        sys.exit(1)

    uvicorn.run(app, host="0.0.0.0", port=args.port)
