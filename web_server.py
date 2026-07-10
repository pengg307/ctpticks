#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
CTP Order Flow Monitor — Optimized Hybrid Version
Combines Claude's clean architecture with GPT-52's bug fixes
"""

import json
import os
import argparse
import random
import threading
import asyncio
import time
from collections import OrderedDict, defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import duckdb
import uvicorn

# ============ Configuration ============
DATA_DIR = "./data"
PERIODS = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600}
WINDOW_CONFIG = {"DEFAULT_MODE": 6, "MAX_ACTIVE": 6, "ORDERFLOW_MAXLEN": 2000}

INSTRUMENTS = ["rb2510", "hc2510", "au2512", "ag2512", "cu2509", "ni2509"]
ALL_INSTRUMENTS = INSTRUMENTS + ["sc2509", "fu2509", "bu2509", "zn2509", "al2509", "pb2509"]

INSTRUMENT_NAMES = {
    "rb2510": "螺纹钢2510", "hc2510": "热卷2510", "au2512": "黄金2512",
    "ag2512": "白银2512", "cu2509": "铜2509", "ni2509": "镍2509",
    "sc2509": "原油2509", "fu2509": "燃料油2509", "bu2509": "沥青2509",
    "zn2509": "锌2509", "al2509": "铝2509", "pb2509": "铅2509",
}
BASE_PRICES = {
    "rb2510": 3350.0, "hc2510": 3300.0, "au2512": 780.0, "ag2512": 9000.0,
    "cu2509": 79000.0, "ni2509": 125000.0, "sc2509": 520.0,
}

# ============ Trading Time (Fixed for Night Sessions) ============
DAY_SESSIONS = [("09:00", "10:15"), ("10:30", "11:30"), ("13:30", "15:00")]
NIGHT_SESSIONS = {
    "au": ("21:00", "02:30"), "ag": ("21:00", "02:30"), "sc": ("21:00", "02:30"),
    "rb": ("21:00", "23:00"), "hc": ("21:00", "23:00"), "cu": ("21:00", "01:00"),
    "al": ("21:00", "01:00"), "zn": ("21:00", "01:00"), "ni": ("21:00", "01:00"),
}

def _to_min(t: str) -> int:
    h, m = map(int, t.split(":"))
    return h * 60 + m

def is_trading_time(inst: Optional[str] = None, now: Optional[datetime] = None) -> bool:
    """Fixed: correctly handles night sessions crossing midnight and Sunday night."""
    if now is None:
        now = datetime.now()
    minutes = now.hour * 60 + now.minute
    wd = now.weekday()  # Mon=0, Sun=6

    # Day sessions: Mon-Fri
    if wd < 5:
        for s, e in DAY_SESSIONS:
            if _to_min(s) <= minutes <= _to_min(e):
                return True

    # Night session
    night = None
    prefix = "".join(c for c in (inst or "").lower() if c.isalpha())
    for length in (len(prefix), 2, 1):
        if prefix[:length] in NIGHT_SESSIONS:
            night = NIGHT_SESSIONS[prefix[:length]]
            break
    if not night:
        return False

    ns, ne = map(_to_min, night)
    if ns <= ne:  # e.g., 21:00-23:00
        return wd < 5 and (ns <= minutes <= ne)

    # Overnight session: e.g., 21:00-01:00 or 21:00-02:30
    if minutes >= ns:  # 21:00-23:59, Mon-Sun
        # Sunday night is Monday's session
        return wd < 6  # Mon-Sat night (Sun night = Mon session)
    else:  # 00:00-01:00
        # This is the continuation of previous night's session
        prev_wd = (wd - 1) % 7
        return prev_wd < 5  # Must be Mon-Fri night continuing

# ============ Data Classes ============
@dataclass
class TickData:
    time: str
    millisec: int
    instrument: str
    price: float
    total_vol: int
    buy_vol: int
    sell_vol: int
    delta: int
    bid: float
    ask: float
    bid_vol: int
    ask_vol: int
    aggressor: str
    volume_total: int

@dataclass 
class BarData:
    time: str
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: int
    buy_vol: int
    sell_vol: int
    delta: int
    tick_count: int = 1

@dataclass
class MACDData:
    dif: List[Optional[float]]
    dea: List[Optional[float]]
    macd: List[Optional[float]]

# ============ MACD Calculator ============
class MACDCalculator:
    @staticmethod
    def ema(data: List[float], period: int) -> List[float]:
        if len(data) < period:
            return list(data)
        k = 2.0 / (period + 1)
        seed = sum(data[:period]) / period
        result = [seed]
        for v in data[period:]:
            result.append(v * k + result[-1] * (1 - k))
        return [result[0]] * (period - 1) + result

    @classmethod
    def compute(cls, bars: List[BarData]) -> MACDData:
        if len(bars) < 26:
            n = len(bars)
            return MACDData([None]*n, [None]*n, [None]*n)
        
        closes = [b.close for b in bars]
        ema12 = cls.ema(closes, 12)
        ema26 = cls.ema(closes, 26)
        dif = [f - s for f, s in zip(ema12, ema26)]
        dea = cls.ema(dif, 9)
        hist = [2 * (d - e) for d, e in zip(dif, dea)]
        
        warmup = 33  # (26-1) + (9-1) = 33 bars need before reliable
        n = len(bars)
        
        def pad(vals):
            return [None] * warmup + [round(v, 4) for v in vals[warmup:]]
        
        # Ensure same length as input
        def align(vals):
            return [None] * (n - len(vals)) + [round(v, 4) for v in vals]
        
        return MACDData(dif=align(dif), dea=align(dea), macd=align(hist))

    @classmethod
    def compute_live(cls, bars: List[BarData], current_close: float) -> Optional[Dict]:
        closes = [b.close for b in bars] + [current_close]
        if len(closes) < 26:
            return None
        ema12 = cls.ema(closes, 12)
        ema26 = cls.ema(closes, 26)
        dif = ema12[-1] - ema26[-1]
        dea = cls.ema([ema12[i] - ema26[i] for i in range(len(ema12))], 9)[-1]
        return {"dif": round(dif, 4), "dea": round(dea, 4), "macd": round(2*(dif-dea), 4)}

# ============ Order Flow Calculator (Fixed) ============
class OrderFlowCalculator:
    """Fixed: trusts mock-provided buy/sell fields when available."""
    
    def __init__(self):
        self.last_volume = 0

    def process(self, tick: dict) -> TickData:
        price = tick.get("price") or tick.get("LastPrice", 0.0)
        volume = tick.get("total_vol") or tick.get("Volume", 0)
        bid = tick.get("bid") or tick.get("BidPrice1", 0.0)
        ask = tick.get("ask") or tick.get("AskPrice1", 0.0)

        vol_delta = max(0, volume - self.last_volume) if self.last_volume > 0 else 0
        self.last_volume = volume

        # FIXED: Trust mock-provided buy/sell when available
        buy_raw = tick.get("buy_vol")
        sell_raw = tick.get("sell_vol")
        agg_raw = tick.get("aggressor")

        if buy_raw is not None and sell_raw is not None and vol_delta > 0:
            # Scale mock's buy/sell proportionally to actual vol_delta
            total_raw = max(int(buy_raw) + int(sell_raw), 1)
            buy_v = int(vol_delta * int(buy_raw) / total_raw)
            sell_v = vol_delta - buy_v
            aggressor = agg_raw or "MIX"
        elif ask > 0 and price >= ask - 0.01:
            buy_v, sell_v, aggressor = vol_delta, 0, "BUY"
        elif bid > 0 and price <= bid + 0.01:
            buy_v, sell_v, aggressor = 0, vol_delta, "SELL"
        else:
            buy_v = vol_delta // 2
            sell_v = vol_delta - buy_v
            aggressor = "MIX"

        return TickData(
            time=tick.get("time") or tick.get("UpdateTime", ""),
            millisec=tick.get("millisec", 0) or tick.get("UpdateMillisec", 0),
            instrument=tick.get("instrument") or tick.get("InstrumentID", ""),
            price=price, total_vol=vol_delta,
            buy_vol=buy_v, sell_vol=sell_v, delta=buy_v - sell_v,
            bid=bid, ask=ask,
            bid_vol=tick.get("bid_vol", 0) or tick.get("BidVolume1", 0),
            ask_vol=tick.get("ask_vol", 0) or tick.get("AskVolume1", 0),
            aggressor=aggressor, volume_total=volume,
        )

# ============ Kline Buffer ============
class KlineBuffer:
    MAX_BARS = 200
    
    def __init__(self):
        self.completed: Dict[str, List[BarData]] = {p: [] for p in PERIODS}
        self.current: Dict[str, Optional[BarData]] = {p: None for p in PERIODS}
        self.macd: Dict[str, MACDData] = {}

    def get_bars(self, period: str, limit: int = 100) -> List[BarData]:
        bars = list(self.completed.get(period, []))
        cur = self.current.get(period)
        if cur:
            bars.append(cur)
        return bars[-limit:]

    def push_completed(self, period: str, bar: BarData):
        lst = self.completed.setdefault(period, [])
        lst.append(bar)
        if len(lst) > self.MAX_BARS:
            lst.pop(0)

    def recompute_macd(self, period: str):
        self.macd[period] = MACDCalculator.compute(self.completed.get(period, []))

    def get_macd_aligned(self, period: str, limit: int = 100) -> MACDData:
        macd = self.macd.get(period)
        if macd is None:
            n = len(self.get_bars(period, limit))
            return MACDData([None]*n, [None]*n, [None]*n)
        
        has_current = self.current.get(period) is not None
        completed_count = len(self.completed.get(period, []))
        
        if has_current and completed_count >= limit:
            tail = limit - 1
            return MACDData(
                dif=macd.dif[-tail:] + [None],
                dea=macd.dea[-tail:] + [None],
                macd=macd.macd[-tail:] + [None],
            )
        return MACDData(
            dif=macd.dif[-limit:],
            dea=macd.dea[-limit:],
            macd=macd.macd[-limit:],
        )

# ============ Data Store ============
class DataStore:
    def __init__(self):
        self._lock = threading.RLock()
        self.active_instruments = set(INSTRUMENTS)
        self.window_instruments = list(INSTRUMENTS)[:WINDOW_CONFIG["DEFAULT_MODE"]]
        self.selected_window = 0
        self.data_source = "mock"
        self.tick_count = 0
        
        self.kbufs: Dict[str, KlineBuffer] = {}
        self.orderflow: Dict[str, deque] = {}
        self.calculators: Dict[str, OrderFlowCalculator] = {}
        self.last_ticks: Dict[str, TickData] = {}
        
        for inst in self.active_instruments:
            self._init_buffers(inst)

    def _init_buffers(self, inst: str):
        self.kbufs[inst] = KlineBuffer()
        self.orderflow[inst] = deque(maxlen=WINDOW_CONFIG["ORDERFLOW_MAXLEN"])
        self.calculators[inst] = OrderFlowCalculator()

    def set_window_instrument(self, window_idx: int, inst: str):
        with self._lock:
            if inst not in ALL_INSTRUMENTS:
                return {"success": False, "message": f"Unknown: {inst}"}
            
            while len(self.window_instruments) <= window_idx:
                self.window_instruments.append(None)
            self.window_instruments[window_idx] = inst
            
            if inst not in self.active_instruments:
                if len(self.active_instruments) >= WINDOW_CONFIG["MAX_ACTIVE"]:
                    evicted = next(iter(self.active_instruments))
                    self.active_instruments.discard(evicted)
                    self.kbufs.pop(evicted, None)
                    self.orderflow.pop(evicted, None)
                    self.calculators.pop(evicted, None)
                self.active_instruments.add(inst)
                self._init_buffers(inst)
            
            return {"success": True, "message": f"Win {window_idx} -> {inst}"}

    def set_selected_window(self, idx: int):
        with self._lock:
            if 0 <= idx < len(self.window_instruments):
                self.selected_window = idx
                inst = self.window_instruments[idx]
                return {"success": True, "selected_inst": inst, "idx": idx}
            return {"success": False}

    def get_window_config(self):
        with self._lock:
            return {
                "mode": WINDOW_CONFIG["DEFAULT_MODE"],
                "windows": list(self.window_instruments),
                "selected": self.selected_window,
                "selected_inst": self.window_instruments[self.selected_window] if self.selected_window < len(self.window_instruments) else None,
            }

    def init_bars(self, source: str = "mock"):
        self.data_source = source
        for inst in self.active_instruments:
            self._init_inst_bars(inst)

    def _init_inst_bars(self, inst: str):
        base = BASE_PRICES.get(inst, 3000.0)
        buf = self.kbufs[inst]
        
        # Generate mock history
        price = base
        now = datetime.now()
        for i in range(100, 0, -1):
            t = now - timedelta(seconds=i * 60)
            change = random.gauss(0, base * 0.002)
            o, h = round(price, 2), round(price + abs(random.gauss(0, base * 0.003)), 2)
            l, c = round(price - abs(random.gauss(0, base * 0.003)), 2), round(price + change, 2)
            vol = random.randint(100, 5000)
            bv = int(vol * random.uniform(0.3, 0.7))
            bar = BarData(
                time=t.strftime("%H:%M"), timestamp=int(t.timestamp() * 1000),
                open=o, high=h, low=l, close=c,
                volume=vol, buy_vol=bv, sell_vol=vol - bv, delta=bv - (vol - bv),
            )
            buf.completed["1m"].append(bar)
            price = c
        
        for p in ["5m", "15m", "1h"]:
            self._aggregate_from_1m(inst, p)
        buf.recompute_macd("1m")

    def _aggregate_from_1m(self, inst: str, period: str):
        buf = self.kbufs[inst]
        bars_1m = buf.completed.get("1m", [])
        if not bars_1m:
            return
        
        seconds = PERIODS[period]
        period_mins = max(seconds // 60, 1)
        today = datetime.now().date()
        agg = []
        current = None
        
        for bar in bars_1m:
            try:
                bt = datetime.strptime(bar.time, "%H:%M").replace(year=today.year, month=today.month, day=today.day)
            except:
                continue
            slot = (bt.minute // period_mins) * period_mins
            agg_t = bt.replace(minute=slot, second=0, microsecond=0)
            tkey = agg_t.strftime("%H:%M")
            
            if current is None or current.time != tkey:
                if current:
                    agg.append(current)
                current = BarData(
                    time=tkey, timestamp=int(agg_t.timestamp() * 1000),
                    open=bar.open, high=bar.high, low=bar.low, close=bar.close,
                    volume=bar.volume, buy_vol=bar.buy_vol, sell_vol=bar.sell_vol, delta=bar.delta,
                )
            else:
                current.high = max(current.high, bar.high)
                current.low = min(current.low, bar.low)
                current.close = bar.close
                current.volume += bar.volume
                current.buy_vol += bar.buy_vol
                current.sell_vol += bar.sell_vol
                current.delta += bar.delta
        
        if current:
            agg.append(current)
        buf.completed[period] = agg[-100:]

    def update_tick(self, inst: str, tick_data: dict):
        if inst not in self.active_instruments:
            return
        
        calc = self.calculators.get(inst)
        if not calc:
            calc = OrderFlowCalculator()
            self.calculators[inst] = calc
        
        flow = calc.process(tick_data)
        if not flow:
            return
        
        with self._lock:
            self.last_ticks[inst] = flow
            self.orderflow[inst].append(flow)
            self.tick_count += 1
            
            for period, seconds in PERIODS.items():
                self._update_kline(inst, period, seconds, flow)

    def _update_kline(self, inst: str, period: str, seconds: int, flow: TickData):
        """FIXED: Uses tick time instead of datetime.now() for bar alignment."""
        # Use tick's time to determine bar bucket
        try:
            # Parse tick time (HH:MM:SS or HH:MM)
            t_str = flow.time
            if len(t_str) == 5:  # HH:MM
                tick_dt = datetime.strptime(t_str, "%H:%M")
            else:  # HH:MM:SS
                tick_dt = datetime.strptime(t_str, "%H:%M:%S")
            tick_dt = tick_dt.replace(year=datetime.now().year, month=datetime.now().month, day=datetime.now().day)
        except:
            tick_dt = datetime.now()
        
        period_mins = max(seconds // 60, 1)
        bar_start = tick_dt.replace(second=0, microsecond=0)
        if period_mins > 1:
            bar_start = bar_start.replace(minute=(bar_start.minute // period_mins) * period_mins)
        
        bar_key = bar_start.strftime("%H:%M")
        
        buf = self.kbufs[inst]
        current = buf.current.get(period)
        
        if current is None or current.time != bar_key:
            if current:
                buf.push_completed(period, current)
                if period == "1m":
                    for p in ["5m", "15m", "1h"]:
                        self._aggregate_from_1m(inst, p)
                    buf.recompute_macd("1m")
            
            buf.current[period] = BarData(
                time=bar_key, timestamp=int(bar_start.timestamp() * 1000),
                open=flow.price, high=flow.price, low=flow.price, close=flow.price,
                volume=flow.total_vol, buy_vol=flow.buy_vol, sell_vol=flow.sell_vol, delta=flow.delta,
            )
        else:
            b = current
            b.high = max(b.high, flow.price)
            b.low = min(b.low, flow.price)
            b.close = flow.price
            b.volume += flow.total_vol
            b.buy_vol += flow.buy_vol
            b.sell_vol += flow.sell_vol
            b.delta += flow.delta

    def get_klines(self, inst: str, period: str):
        with self._lock:
            buf = self.kbufs.get(inst)
            if not buf:
                return []
            return [vars(b) for b in buf.get_bars(period, 100)]

    def get_orderflow(self, inst: str, n: int = 150):
        with self._lock:
            of = self.orderflow.get(inst)
            if not of:
                return []
            return [vars(t) for t in list(of)[-n:]]

    def get_macd(self, inst: str, period: str):
        with self._lock:
            buf = self.kbufs.get(inst)
            if not buf:
                return {"dif": [], "dea": [], "macd": []}
            macd = buf.get_macd_aligned(period, 100)
            return {"dif": macd.dif, "dea": macd.dea, "macd": macd.macd}

    def get_live_macd(self, inst: str, period: str):
        with self._lock:
            buf = self.kbufs.get(inst)
            if not buf:
                return None
            bars = list(buf.completed.get(period, []))
            cur = buf.current.get(period)
            return MACDCalculator.compute_live(bars, cur.close) if cur else None

    def get_current_bars(self, insts: List[str]):
        result = {}
        with self._lock:
            for inst in insts:
                buf = self.kbufs.get(inst)
                if not buf:
                    continue
                result[inst] = {}
                for period in PERIODS:
                    cur = buf.current.get(period)
                    if cur:
                        d = vars(cur)
                        live = self.get_live_macd(inst, period)
                        if live:
                            d["live_macd"] = live
                        result[inst][period] = d
        return result

# ============ Mock Engine ============
class MockEngine:
    def __init__(self, store: DataStore):
        self.store = store
        self.prices = {inst: BASE_PRICES.get(inst, 3000.0) for inst in store.active_instruments}
        self.volumes = {inst: 0 for inst in store.active_instruments}
        self.running = False

    def _generate_tick(self, inst: str):
        base = self.prices.get(inst, BASE_PRICES.get(inst, 3000.0))
        change = random.gauss(0, base * 0.0005)
        price = round(max(base * 0.99, min(base * 1.01, base + change)), 2)
        self.prices[inst] = price
        
        spread = max(base * 0.0002, 0.01)
        bid, ask = round(price - spread, 2), round(price + spread, 2)
        vol = random.randint(1, 30) if random.random() > 0.2 else 0
        self.volumes[inst] += vol
        
        r = random.random()
        if r < 0.3 and vol > 0:
            p, bv, sv, agg = ask, vol, 0, "BUY"
        elif r < 0.6 and vol > 0:
            p, bv, sv, agg = bid, 0, vol, "SELL"
        else:
            bv = int(vol * random.uniform(0.3, 0.7))
            sv = vol - bv
            p, agg = price, "MIX"
        
        now = datetime.now()
        return {
            "time": now.strftime("%H:%M:%S"),
            "millisec": now.microsecond // 1000,
            "instrument": inst,
            "price": p,
            "total_vol": self.volumes[inst],
            "buy_vol": bv,
            "sell_vol": sv,
            "delta": bv - sv,
            "bid": bid,
            "ask": ask,
            "bid_vol": random.randint(50, 500),
            "ask_vol": random.randint(50, 500),
            "aggressor": agg,
        }

    def start(self):
        self.running = True
        def run():
            while self.running:
                for inst in list(self.store.active_instruments):
                    if inst not in self.prices:
                        self.prices[inst] = BASE_PRICES.get(inst, 3000.0)
                        self.volumes[inst] = 0
                    tick = self._generate_tick(inst)
                    self.store.update_tick(inst, tick)
                time.sleep(0.5)
        threading.Thread(target=run, daemon=True).start()

    def stop(self):
        self.running = False

# ============ WebSocket Manager (Fixed: asyncio.Lock) ============
class ConnectionManager:
    def __init__(self):
        self.connections = []
        self._lock = asyncio.Lock()  # FIXED: was threading.RLock

    async def connect(self, ws: WebSocket):
        await ws.accept()
        async with self._lock:
            self.connections.append(ws)

    async def disconnect(self, ws: WebSocket):
        async with self._lock:
            try:
                self.connections.remove(ws)
            except ValueError:
                pass

    async def broadcast(self, message: dict):
        if not self.connections:
            return
        try:
            text = json.dumps(message)
        except:
            return
        
        async with self._lock:
            conns = list(self.connections)
        
        dead = []
        for ws in conns:
            try:
                await asyncio.wait_for(ws.send_text(text), timeout=2.0)
            except:
                dead.append(ws)
        
        for ws in dead:
            await self.disconnect(ws)

# ============ FastAPI App ============
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


# 静态文件服务（本地JS）
if os.path.exists("./js"):
    app.mount("/js", StaticFiles(directory="./js"), name="js")

store = DataStore()
engine = MockEngine(store)
manager = ConnectionManager()

@app.on_event("startup")
async def startup():
    store.init_bars("mock")
    engine.start()
    asyncio.create_task(data_pusher())

@app.on_event("shutdown")
async def shutdown():
    engine.stop()

@app.get("/", response_class=HTMLResponse)
async def index():
    with open("page.html", "r", encoding="utf-8") as f:
        html = f.read()
    config = json.dumps({
        "all_instruments": ALL_INSTRUMENTS,
        "instrument_names": INSTRUMENT_NAMES,
        "periods": list(PERIODS.keys()),
        "default_mode": WINDOW_CONFIG["DEFAULT_MODE"],
        "default_windows": INSTRUMENTS,
    })
    return HTMLResponse(html.replace("$SERVER_CONFIG", config))

@app.get("/api/klines")
async def api_klines(inst: str = "rb2510", period: str = "1m"):
    if inst not in store.active_instruments:
        if inst in ALL_INSTRUMENTS:
            store.set_window_instrument(0, inst)
            store._init_inst_bars(inst)
        else:
            return {"instrument": inst, "period": period, "klines": [], "macd": {"dif": [], "dea": [], "macd": []}, "error": "Unknown"}
    return {
        "instrument": inst, "period": period,
        "klines": store.get_klines(inst, period),
        "macd": store.get_macd(inst, period),
    }

@app.get("/api/source")
async def api_source():
    return {"source": store.data_source}

@app.post("/api/source")
async def api_set_source(source: str = "mock"):
    store.data_source = source
    return {"status": "ok", "source": source}

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        cfg = store.get_window_config()
        main_inst = cfg.get("selected_inst")
        init_data = {
            "type": "init",
            "timestamp": int(time.time() * 1000),
            "instruments": {},
            "selected_orderflow": [],
            "window_config": cfg,
            "data_source": store.data_source,
        }
        for inst in store.active_instruments:
            tick = store.last_ticks.get(inst)
            if tick:
                init_data["instruments"][inst] = {
                    "last_price": tick.price,
                    "volume": tick.volume_total,
                    "delta": tick.delta,
                }
        if main_inst:
            init_data["selected_orderflow"] = store.get_orderflow(main_inst, 150)
            init_data["selected_inst"] = main_inst
            init_data["selected_name"] = INSTRUMENT_NAMES.get(main_inst, main_inst)
        await ws.send_text(json.dumps(init_data))

        while True:
            msg = json.loads(await ws.receive_text())
            action = msg.get("action")
            if action == "select_window":
                result = store.set_selected_window(msg.get("window_idx", 0))
                await ws.send_text(json.dumps({"type": "ack", "action": action, "result": result}))
            elif action == "change_instrument":
                idx, inst = msg.get("window_idx", 0), msg.get("instrument")
                if inst:
                    result = store.set_window_instrument(idx, inst)
                    if result["success"] and not store.kbufs.get(inst, KlineBuffer()).completed.get("1m"):
                        store._init_inst_bars(inst)
                    await ws.send_text(json.dumps({"type": "ack", "action": action, "result": result}))
            elif action == "change_mode":
                pass  # Simplified
            elif action == "get_config":
                await ws.send_text(json.dumps({"type": "config", "window_config": store.get_window_config(), "data_source": store.data_source}))
            elif action == "ping":
                await ws.send_text(json.dumps({"type": "pong"}))
    except WebSocketDisconnect:
        pass
    finally:
        await manager.disconnect(ws)

async def data_pusher():
    while True:
        await asyncio.sleep(1)
        if not manager.connections:
            continue
        try:
            cfg = store.get_window_config()
            main_inst = cfg.get("selected_inst")
            active = list(store.active_instruments)
            
            orderflow = []
            if main_inst and main_inst in store.active_instruments:
                orderflow = store.get_orderflow(main_inst, 500)
            
            kline_update = store.get_current_bars([i for i in cfg["windows"] if i])
            
            instruments = {}
            for inst in active:
                tick = store.last_ticks.get(inst)
                if tick:
                    instruments[inst] = {"last_price": tick.price, "volume": tick.volume_total, "delta": tick.delta}
            
            await manager.broadcast({
                "type": "update",
                "timestamp": int(time.time() * 1000),
                "instruments": instruments,
                "selected_orderflow": orderflow,
                "selected_inst": main_inst,
                "selected_name": INSTRUMENT_NAMES.get(main_inst, main_inst or "--"),
                "window_config": cfg,
                "data_source": store.data_source,
                "orderflow_stats": {"count": len(orderflow), "time_range": "", "max_display": 150},
                "kline_update": kline_update,
            })
        except Exception as e:
            print(f"[Push] Error: {e}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', default='mock', choices=['mock', 'ctp'])
    parser.add_argument('--port', type=int, default=8080, help='监听端口')
    args = parser.parse_args()

    uvicorn.run(app, host="0.0.0.0", port=args.port)
