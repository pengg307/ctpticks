#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
web_server.py (CLEAN FIX)
===========================
基于原始代码，只修复必要问题：
1. get_macd KeyError 防御
2. get_klines/get_orderflow KeyError 防御
3. 删除 auto_fill_gaps 调用
4. Mock/CTP 切换按钮响应
5. 前端价格 classList 修复
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
from datetime import datetime, timedelta
from collections import deque, defaultdict, OrderedDict
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import uvicorn

# ============ 加载环境变量 ============
from dotenv import load_dotenv

env_paths = ['.env', 'config/.env', os.path.expanduser('~/.ctp.env')]
for env_path in env_paths:
    if os.path.exists(env_path):
        load_dotenv(env_path, override=True)
        print(f"[Config] 已加载环境变量: {env_path}")
        break

# ============ 导入合约配置 ============
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

# 从环境变量读取账号密码，覆盖配置文件
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

# ============ 运行配置（从 instruments.py 推导） ============
ALL_INSTRUMENTS = [v["code"] for v in INSTRUMENTS_CONFIG.values()]
ALL_INSTRUMENT_NAMES = {v["code"]: v["name"] for v in INSTRUMENTS_CONFIG.values()}
ALL_BASE_PRICES = {v["code"]: v["base_price"] for v in INSTRUMENTS_CONFIG.values()}

# 监控品种（从 MONITOR_SYMBOLS 推导）
INSTRUMENTS = MONITOR_INSTRUMENTS
INSTRUMENT_NAMES = MONITOR_NAMES
BASE_PRICES = MONITOR_BASE_PRICES

# 前端显示周期
PERIODS = {k: v for k, v in PERIODS_CONFIG.items() if k in DISPLAY_PERIODS}

# 窗口管理配置
WINDOW_CONFIG = {
    "DEFAULT_MODE": 6,           # 默认6窗口
    "MAX_ACTIVE": 6,             # 最大活跃品种数
    "ORDERFLOW_MAXLEN": 500,     # 订单流内存保留条数
}

# CSV 存储配置
DATA_DIR = "./data"
KLINE_1M_RETENTION_DAYS = 60
KLINE_OTHER_RETENTION_DAYS = 7
TICK_RETENTION_DAYS = 3
CSV_FLUSH_INTERVAL = 5  # 秒

# ============ openctp-ctp 导入 ============
try:
    from openctp_ctp import mdapi
    CTP_AVAILABLE = True
except ImportError:
    mdapi = None
    CTP_AVAILABLE = False
    print("⚠️ openctp-ctp 未安装，CTP模式不可用")

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 静态文件服务（本地JS）
if os.path.exists("./js"):
    app.mount("/js", StaticFiles(directory="./js"), name="js")


# ============ 订单流计算器 ============
class OrderFlowCalculator:
    """根据 Tick 数据计算主动买/卖量"""

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
        }


# ============ CSV 存储管理器（线程安全） ============
class CSVManager:
    def __init__(self, data_dir=DATA_DIR):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self._lock = threading.RLock()  # [FIX] RLock: 可重入，防止死锁
        self._buffers = defaultdict(list)
        self._running = True
        self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._flush_thread.start()
        print(f"[CSV] 存储初始化: {os.path.abspath(data_dir)}")

    def _get_kline_filepath(self, inst, period):
        today = datetime.now().strftime("%Y%m%d")
        return os.path.join(self.data_dir, f"{inst}_{period}_{today}_kline.csv")

    def _get_tick_filepath(self, inst):
        today = datetime.now().strftime("%Y%m%d")
        return os.path.join(self.data_dir, f"{inst}_{today}_tick.csv")

    def save_kline(self, inst, period, bar):
        key = f"{inst}_{period}_kline"
        row = [
            bar.get("time", ""),
            bar.get("open", 0), bar.get("high", 0),
            bar.get("low", 0), bar.get("close", 0),
            bar.get("volume", 0), bar.get("buy_vol", 0),
            bar.get("sell_vol", 0), bar.get("delta", 0),
            bar.get("tick_count", 1),
        ]
        with self._lock:
            self._buffers[key].append((inst, period, "kline", row))

    def save_tick(self, inst, tick):
        key = f"{inst}_tick"
        row = [
            tick.get("time", ""), tick.get("millisec", 0),
            tick.get("instrument", inst),
            tick.get("price", 0), tick.get("total_vol", 0),
            tick.get("buy_vol", 0), tick.get("sell_vol", 0),
            tick.get("delta", 0), tick.get("bid", 0),
            tick.get("ask", 0), tick.get("bid_vol", 0),
            tick.get("ask_vol", 0),
            round(tick.get("ask", 0) - tick.get("bid", 0), 4),
            tick.get("aggressor", ""),
        ]
        with self._lock:
            self._buffers[key].append((inst, None, "tick", row))

    def _flush_loop(self):
        while self._running:
            time.sleep(CSV_FLUSH_INTERVAL)
            self._flush()

    def _flush(self):
        with self._lock:
            buffers = dict(self._buffers)
            self._buffers.clear()

        if not buffers:
            return

        kline_headers = ["time", "open", "high", "low", "close", "volume", "buy_vol", "sell_vol", "delta", "tick_count"]
        tick_headers = ["time", "millisec", "instrument", "price", "volume", "buy_vol", "sell_vol", "delta", "bid", "ask", "bid_vol", "ask_vol", "spread", "aggressor"]

        for key, rows in buffers.items():
            if not rows:
                continue
            inst, period, row_type = rows[0][0], rows[0][1], rows[0][2]

            if row_type == "kline":
                filepath = self._get_kline_filepath(inst, period)
                headers = kline_headers
            else:
                filepath = self._get_tick_filepath(inst)
                headers = tick_headers

            try:
                exists = os.path.exists(filepath)
                mode = 'a' if exists else 'w'
                with open(filepath, mode, newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    if not exists:
                        writer.writerow(headers)
                    for _, _, _, row in rows:
                        writer.writerow(row)
                    f.flush()
                    os.fsync(f.fileno())
            except Exception as e:
                print(f"[CSV] 写入错误 {filepath}: {e}")

    def load_klines(self, inst, period, limit=200):
        klines = []
        for i in range(3):
            day = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
            filepath = os.path.join(self.data_dir, f"{inst}_{period}_{day}_kline.csv")
            if os.path.exists(filepath):
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            klines.append({
                                "time": row["time"],
                                "open": float(row["open"]), "high": float(row["high"]),
                                "low": float(row["low"]), "close": float(row["close"]),
                                "volume": int(row["volume"]), "buy_vol": int(row["buy_vol"]),
                                "sell_vol": int(row["sell_vol"]), "delta": int(row["delta"]),
                                "tick_count": int(row.get("tick_count", 1)),
                            })
                except Exception as e:
                    print(f"[CSV] 加载错误 {filepath}: {e}")
        return klines[-limit:] if limit else klines

    def load_ticks(self, inst, limit=500):
        ticks = []
        for i in range(3):
            day = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
            filepath = os.path.join(self.data_dir, f"{inst}_{day}_tick.csv")
            if os.path.exists(filepath):
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        rows = list(reader)
                        for row in rows:
                            ticks.append({
                                "time": row["time"], "millisec": row.get("millisec", "0"),
                                "instrument": row["instrument"], "price": float(row["price"]),
                                "total_vol": int(row["volume"]), "buy_vol": int(row["buy_vol"]),
                                "sell_vol": int(row["sell_vol"]), "delta": int(row["delta"]),
                                "bid": float(row["bid"]), "ask": float(row["ask"]),
                                "bid_vol": int(row["bid_vol"]), "ask_vol": int(row["ask_vol"]),
                                "aggressor": row.get("aggressor", ""),
                            })
                except Exception as e:
                    print(f"[CSV] 加载Tick错误 {filepath}: {e}")
        return ticks[-limit:] if limit else ticks

    def cleanup_old_files(self):
        now = datetime.now()
        print("[CSV] 开始清理过期数据...")
        cleaned = 0

        for filepath in glob.glob(os.path.join(self.data_dir, "*.csv")):
            try:
                mtime = datetime.fromtimestamp(os.path.getmtime(filepath))
                filename = os.path.basename(filepath)
                should_delete = False

                if "_1m_" in filename and "_kline" in filename:
                    if now - mtime > timedelta(days=KLINE_1M_RETENTION_DAYS):
                        should_delete = True
                elif any(f"_{p}_" in filename for p in ["5m", "15m", "1h"]) and "_kline" in filename:
                    if now - mtime > timedelta(days=KLINE_OTHER_RETENTION_DAYS):
                        should_delete = True
                elif "_tick" in filename and "_kline" not in filename:
                    if now - mtime > timedelta(days=TICK_RETENTION_DAYS):
                        should_delete = True

                if should_delete:
                    os.remove(filepath)
                    cleaned += 1
            except Exception as e:
                print(f"[CSV] 清理错误: {e}")

        print(f"[CSV] 清理完成，删除 {cleaned} 个文件")
        return cleaned

    def force_flush(self):
        self._flush()

    def close(self):
        self._running = False
        self._flush()


# ============ 交易时间工具 ============
TRADING_SESSIONS = {
    "day": [("09:00", "10:15"), ("10:30", "11:30"), ("13:30", "15:00")],
    "night": [("21:00", "23:00")],
}

def is_trading_time(now=None):
    if now is None:
        now = datetime.now()
    time_str = now.strftime("%H:%M")
    weekday = now.weekday()
    if weekday >= 5:
        return False
    for start, end in TRADING_SESSIONS["day"]:
        if start <= time_str <= end:
            return True
    for start, end in TRADING_SESSIONS["night"]:
        if start <= time_str <= end:
            return True
    return False


# ============ 全局数据存储（LRU活跃品种管理 + CSV持久化） ============
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

        self.window_mode = WINDOW_CONFIG["DEFAULT_MODE"]
        self.window_instruments = list(INSTRUMENTS)[:self.window_mode]
        self.selected_window = 0

        self.csv_manager = CSVManager()
        self.calculators = {inst: OrderFlowCalculator() for inst in ALL_INSTRUMENTS}
        self.data_source = "mock"

        self._lock = threading.RLock()  # [FIX] RLock: 可重入，防止死锁
        self._init_active_instruments()

    def _init_active_instruments(self):
        for inst in self.active_instruments:
            self.klines[inst] = defaultdict(list)
            self.current_bar[inst] = {}
            self.orderflow[inst] = deque(maxlen=WINDOW_CONFIG["ORDERFLOW_MAXLEN"])
            self.macd[inst] = {}
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

            self._access_time[inst] = time.time()
            self._access_time.move_to_end(inst)

            return {"success": True, "message": f"窗口 {window_idx} -> {inst}", "evicted": evicted}

    def init_instrument_bars(self, inst):
        base = BASE_PRICES.get(inst, 3000.0)

        klines_1m = self.csv_manager.load_klines(inst, "1m", limit=200)
        if klines_1m:
            with self._lock:
                self.klines[inst]["1m"] = klines_1m
            print(f"  [{inst}] CSV加载 {len(klines_1m)} 根1分钟K线")
        elif self.data_source == "mock":
            print(f"  [{inst}] Mock生成历史K线...")
            bars = []
            price = base
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

                bar = {
                    "time": t.strftime("%H:%M"),
                    "timestamp": int(t.timestamp() * 1000),
                    "open": open_p, "high": high_p, "low": low_p, "close": close_p,
                    "volume": vol, "buy_vol": buy_v, "sell_vol": sell_v,
                    "delta": buy_v - sell_v, "tick_count": 1,
                }
                bars.append(bar)
                price = close_p
                self.csv_manager.save_kline(inst, "1m", bar)

            with self._lock:
                self.klines[inst]["1m"] = bars
        else:
            print(f"  [{inst}] CTP模式，等待实时推送...")

        for period_name in ["5m", "15m", "1h"]:
            self._aggregate_from_1m(inst, period_name)

        self._calc_macd(inst, "1m")

        hist_ticks = self.csv_manager.load_ticks(inst, limit=100)
        if hist_ticks:
            with self._lock:
                self.orderflow[inst].extend(hist_ticks)
            print(f"  [{inst}] CSV加载 {len(hist_ticks)} 笔历史Tick")

        print(f"[Init] {inst} 历史数据初始化完成")

    def _remove_instrument(self, inst):
        self.active_instruments.discard(inst)
        self.klines.pop(inst, None)
        self.current_bar.pop(inst, None)
        self.orderflow.pop(inst, None)
        self.last_ticks.pop(inst, None)
        self.macd.pop(inst, None)
        self._access_time.pop(inst, None)
        print(f"[LRU] 淘汰: {inst}，活跃: {len(self.active_instruments)}")

    def set_window_mode(self, mode):
        with self._lock:
            if mode not in [4, 6]:
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

    def init_bars(self, source="mock"):
        print(f"初始化历史数据... 活跃品种: {len(self.active_instruments)}")
        self.data_source = source
        self.csv_manager.cleanup_old_files()

        for inst in self.active_instruments:
            self.init_instrument_bars(inst)

        print("历史数据初始化完成")

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

        with self._lock:
            self.macd[inst][period] = {
                "dif": [round(x, 4) for x in dif[-50:]],
                "dea": [round(x, 4) for x in dea[-50:]],
                "macd": [round(x, 4) for x in macd_hist[-50:]],
            }

    def _ema(self, data, period):
        if len(data) < period:
            return data
        k = 2 / (period + 1)
        ema = [sum(data[:period]) / period]
        for i in range(period, len(data)):
            ema.append(data[i] * k + ema[-1] * (1 - k))
        result = [ema[0]] * (period - 1) + ema
        return result[-50:]

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

        with self._lock:
            self.last_ticks[inst] = flow
            self.orderflow[inst].append(flow)
            self.tick_count += 1
            self._access_time[inst] = time.time()
            self._access_time.move_to_end(inst)

        self.csv_manager.save_tick(inst, flow)

        for period_name, seconds in PERIODS.items():
            self._update_kline(inst, period_name, seconds, flow)

    def _update_kline(self, inst, period_name, seconds, flow):
        now = datetime.now()
        period_minutes = max(seconds // 60, 1)
        bar_start = now.replace(second=0, microsecond=0)
        if period_minutes > 1:
            bar_start = bar_start.replace(minute=(bar_start.minute // period_minutes) * period_minutes)

        bar_key = bar_start.strftime("%H:%M")

        with self._lock:
            current = self.current_bar[inst].get(period_name)

            if not current or current.get("time") != bar_key:
                if current:
                    if period_name == "1m":
                        self.csv_manager.save_kline(inst, period_name, current)
                    self.klines[inst][period_name].append(current)
                    if len(self.klines[inst][period_name]) > 200:
                        self.klines[inst][period_name].pop(0)

                    if period_name == "1m":
                        for p in ["5m", "15m", "1h"]:
                            self._aggregate_from_1m(inst, p)
                        self._calc_macd(inst, "1m")

                self.current_bar[inst][period_name] = {
                    "time": bar_key, "timestamp": int(bar_start.timestamp() * 1000),
                    "open": flow["price"], "high": flow["price"], "low": flow["price"],
                    "close": flow["price"], "volume": flow["total_vol"],
                    "buy_vol": flow["buy_vol"], "sell_vol": flow["sell_vol"], "delta": flow["delta"],
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

    # [FIX] 防御性处理：如果品种或周期不存在，返回空列表
    def get_klines(self, inst, period):
        with self._lock:
            if inst not in self.klines or period not in self.klines[inst]:
                return []
            bars = list(self.klines[inst][period])
            current = dict(self.current_bar[inst][period]) if self.current_bar[inst].get(period) else None
        if current:
            bars.append(current)
        return bars[-100:]

    # [FIX] 防御性处理：如果品种不存在，返回空列表
    def get_orderflow(self, inst, n=30):
        with self._lock:
            if inst not in self.orderflow:
                return []
            return list(self.orderflow[inst])[-n:]

    # [FIX] 防御性处理：如果MACD未计算，返回空结构
    def get_macd(self, inst, period):
        with self._lock:
            if inst not in self.macd or period not in self.macd[inst]:
                return {"dif": [], "dea": [], "macd": []}
            return dict(self.macd[inst][period])

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
        self.csv_manager.close()


store = DataStore()


# ============ Mock 模拟行情引擎 ============
class MockEngine:
    def __init__(self):
        self.prices = {inst: BASE_PRICES.get(inst, 3000.0) for inst in store.active_instruments}
        self.volumes = {inst: 0 for inst in store.active_instruments}
        self.running = False

    def generate_tick(self, inst):
        base = self.prices.get(inst, BASE_PRICES.get(inst, 3000.0))
        change = random.gauss(0, base * 0.001)
        price = round(base + change, 2)
        price = max(BASE_PRICES.get(inst, 3000.0) * 0.97, min(BASE_PRICES.get(inst, 3000.0) * 1.03, price))
        self.prices[inst] = price

        vol = random.randint(1, 50) if random.random() > 0.3 else 0
        self.volumes[inst] += vol

        spread = base * 0.0005
        bid = round(price - spread, 2)
        ask = round(price + spread, 2)

        return {
            "time": datetime.now().strftime("%H:%M:%S"),
            "millisec": datetime.now().microsecond // 1000,
            "instrument": inst, "price": price, "total_vol": vol,
            "buy_vol": 0, "sell_vol": 0, "delta": 0,
            "bid": bid, "ask": ask, "bid_vol": random.randint(50, 500),
            "ask_vol": random.randint(50, 500), "aggressor": "MIX",
            "volume_total": self.volumes[inst],
        }

    def start(self):
        self.running = True
        def run():
            print("[Mock] 模拟行情引擎启动")
            while self.running:
                for inst in list(store.active_instruments):
                    if inst not in self.prices:
                        self.prices[inst] = BASE_PRICES.get(inst, 3000.0)
                        self.volumes[inst] = 0
                    tick = self.generate_tick(inst)
                    store.update_tick(inst, tick)
                time.sleep(0.5)
        threading.Thread(target=run, daemon=True).start()

    def stop(self):
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
        self._lock = threading.RLock()  # [FIX] RLock: 可重入，防止死锁

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
                # [FIX] 2秒超时，防止死连接挂死广播
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
    while True:
        await asyncio.sleep(1)
        push_count += 1

        if not manager.active_connections:
            if push_count % 10 == 0:
                print(f"[Push] 等待客户端... (总Tick:{store.tick_count})")
            continue

        try:
            # [FIX] 加锁复制数据，防止遍历Set时被并发修改
            with store._lock:
                main_inst = None
                if store.selected_window < len(store.window_instruments):
                    main_inst = store.window_instruments[store.selected_window]

                window_config = store.get_window_config()
                active_insts = list(store.active_instruments)
                last_ticks_copy = dict(store.last_ticks)

                orderflow = []
                if main_inst and main_inst in store.active_instruments:
                    orderflow = list(store.orderflow.get(main_inst, deque()))[-20:]

                data_source = dsm.current

            data = {
                "type": "update",
                "timestamp": int(time.time() * 1000),
                "instruments": {},
                "selected_orderflow": [],
                "selected_inst": main_inst,
                "selected_name": INSTRUMENT_NAMES.get(main_inst, main_inst) if main_inst else "--",
                "window_config": window_config,
                "data_source": data_source,
            }

            for inst in active_insts:
                tick = last_ticks_copy.get(inst)
                if tick:
                    data["instruments"][inst] = {
                        "last_price": tick["price"],
                        "volume": tick.get("volume_total", 0),
                        "delta": tick["delta"],
                    }

            if main_inst and main_inst in active_insts:
                data["selected_orderflow"] = orderflow

            inst_count = len(data["instruments"])
            of_count = len(data["selected_orderflow"])
            if push_count <= 5 or push_count % 10 == 0:
                print(f"[Push] #{push_count} 推送 {inst_count}个品种 {of_count}笔订单流 Tick:{store.tick_count}")

            await manager.broadcast(data)

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
                store.active_instruments.add(inst)
                store._access_time[inst] = time.time()
                if inst not in engine.prices:
                    engine.prices[inst] = BASE_PRICES.get(inst, 3000.0)
                    engine.volumes[inst] = 0
            else:
                return {
                    "instrument": inst,
                    "period": period,
                    "klines": [],
                    "macd": {"dif": [], "dea": [], "macd": []},
                    "error": f"初始化失败: {result.get('message', '未知错误')}",
                }
        else:
            return {
                "instrument": inst,
                "period": period,
                "klines": [],
                "macd": {"dif": [], "dea": [], "macd": []},
                "error": "品种不存在",
            }

    klines = store.get_klines(inst, period)
    macd = store.get_macd(inst, period)

    print(f"[API] 返回: {inst} {period}, {len(klines)} 根K线")
    return {"instrument": inst, "period": period, "klines": klines, "macd": macd}


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
async def api_window_mode(mode: int = 6):
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
                }

        main_inst = None
        with store._lock:
            if store.selected_window < len(store.window_instruments):
                main_inst = store.window_instruments[store.selected_window]
        if main_inst and main_inst in store.active_instruments:
            initial_data["selected_orderflow"] = store.get_orderflow(main_inst, 20)
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
                        mode = data.get('mode', 6)
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


# ============ 启动 ============
@app.on_event("startup")
async def startup_event():
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
    print(f"CSV目录: {os.path.abspath(DATA_DIR)}")
    print(f"启动模式: {args.source}")
    print(f"请用浏览器打开: http://localhost:{args.port}")
    print("=" * 60 + "\n")

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

    asyncio.create_task(data_pusher())


@app.on_event("shutdown")
async def shutdown_event():
    print("\n" + "=" * 60)
    print("服务关闭中...")
    engine.stop()
    ctp_engine.stop()
    store.csv_manager.force_flush()
    store.close()
    print("数据已保存，服务已关闭")
    print("=" * 60)


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
