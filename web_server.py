#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
web_server.py
=============
FastAPI + Uvicorn 版本，原生 WebSocket 支持

运行方式：
  pip install fastapi uvicorn websockets
  python web_server.py
  浏览器打开 http://localhost:8080
"""

import json
import time
import random
import threading
import asyncio
from datetime import datetime, timedelta
from collections import deque, defaultdict

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from fastapi.responses import FileResponse

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============ 配置 ============
# 删掉原来的 INSTRUMENTS、INSTRUMENT_NAMES、BASE_PRICES、PERIODS
# 改成：
from config.instruments import (
    MONITOR_INSTRUMENTS as INSTRUMENTS,
    MONITOR_NAMES as INSTRUMENT_NAMES,
    MONITOR_BASE_PRICES as BASE_PRICES,
    PERIODS,
)


# ============ 全局数据存储 ============
class DataStore:
    def __init__(self):
        self.klines = defaultdict(lambda: defaultdict(list))
        self.current_bar = defaultdict(lambda: defaultdict(dict))
        self.orderflow = defaultdict(lambda: deque(maxlen=500))
        self.last_ticks = {}
        self.selected_instrument = INSTRUMENTS[0]
        self.macd = defaultdict(lambda: defaultdict(dict))
        self._lock = threading.Lock()
        self.tick_count = 0

    def init_bars(self):
        print("初始化历史K线数据...")
        for inst in INSTRUMENTS:
            base = BASE_PRICES[inst]
            for period_name, seconds in PERIODS.items():
                now = datetime.now()
                bars = []
                price = base
                for i in range(100, 0, -1):
                    t = now - timedelta(seconds=i * seconds)
                    change = random.gauss(0, base * 0.002)
                    open_p = round(price, 2)
                    high_p = round(open_p + abs(random.gauss(0, base * 0.003)), 2)
                    low_p = round(open_p - abs(random.gauss(0, base * 0.003)), 2)
                    close_p = round(open_p + change, 2)
                    vol = random.randint(100, 5000)
                    buy_v = int(vol * random.uniform(0.3, 0.7))
                    sell_v = vol - buy_v

                    bars.append({
                        "time": t.strftime("%H:%M"),
                        "timestamp": int(t.timestamp() * 1000),
                        "open": open_p, "high": high_p, "low": low_p, "close": close_p,
                        "volume": vol, "buy_vol": buy_v, "sell_vol": sell_v,
                        "delta": buy_v - sell_v,
                    })
                    price = close_p

                with self._lock:
                    self.klines[inst][period_name] = bars
                self._calc_macd(inst, period_name)
        print("历史K线初始化完成")

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

    def update_tick(self, inst, tick):
        with self._lock:
            self.last_ticks[inst] = tick
            self.orderflow[inst].append(tick)
            self.tick_count += 1

        for period_name, seconds in PERIODS.items():
            self._update_kline(inst, period_name, seconds, tick)

    def _update_kline(self, inst, period_name, seconds, tick):
        now = datetime.now()
        period_minutes = seconds // 60
        bar_start = now.replace(second=0, microsecond=0)
        if period_minutes > 0:
            bar_start = bar_start.replace(minute=(bar_start.minute // period_minutes) * period_minutes)

        bar_key = bar_start.strftime("%H:%M")

        with self._lock:
            current = self.current_bar[inst][period_name]

            if not current or current.get("time") != bar_key:
                if current:
                    self.klines[inst][period_name].append(current)
                    if len(self.klines[inst][period_name]) > 200:
                        self.klines[inst][period_name].pop(0)

                self.current_bar[inst][period_name] = {
                    "time": bar_key, "timestamp": int(bar_start.timestamp() * 1000),
                    "open": tick["price"], "high": tick["price"], "low": tick["price"],
                    "close": tick["price"], "volume": tick["total_vol"],
                    "buy_vol": tick["buy_vol"], "sell_vol": tick["sell_vol"], "delta": tick["delta"],
                }
            else:
                bar = current
                bar["high"] = max(bar["high"], tick["price"])
                bar["low"] = min(bar["low"], tick["price"])
                bar["close"] = tick["price"]
                bar["volume"] += tick["total_vol"]
                bar["buy_vol"] += tick["buy_vol"]
                bar["sell_vol"] += tick["sell_vol"]
                bar["delta"] += tick["delta"]

    def get_klines(self, inst, period):
        with self._lock:
            bars = list(self.klines[inst][period])
            current = dict(self.current_bar[inst][period]) if self.current_bar[inst][period] else None
        if current:
            bars.append(current)
        return bars[-100:]

    def get_orderflow(self, inst, n=30):
        with self._lock:
            return list(self.orderflow[inst])[-n:]

    def get_macd(self, inst, period):
        with self._lock:
            return dict(self.macd[inst][period])


store = DataStore()

# ============ 模拟行情引擎 ============
class MockEngine:
    def __init__(self):
        self.prices = {inst: BASE_PRICES[inst] for inst in INSTRUMENTS}
        self.volumes = {inst: 0 for inst in INSTRUMENTS}
        self.running = False

    def generate_tick(self, inst):
        base = self.prices[inst]
        change = random.gauss(0, base * 0.001)
        price = round(base + change, 2)
        price = max(BASE_PRICES[inst] * 0.97, min(BASE_PRICES[inst] * 1.03, price))
        self.prices[inst] = price

        vol = random.randint(1, 50) if random.random() > 0.3 else 0
        self.volumes[inst] += vol

        spread = base * 0.0005
        bid = round(price - spread, 2)
        ask = round(price + spread, 2)

        if price >= ask - 0.01:
            buy_v, sell_v, agg = vol, 0, "BUY"
        elif price <= bid + 0.01:
            buy_v, sell_v, agg = 0, vol, "SELL"
        else:
            buy_v = int(vol * 0.5)
            sell_v = vol - buy_v
            agg = "MIX"

        return {
            "time": datetime.now().strftime("%H:%M:%S"),
            "instrument": inst, "price": price, "total_vol": vol,
            "buy_vol": buy_v, "sell_vol": sell_v, "delta": buy_v - sell_v,
            "bid": bid, "ask": ask, "bid_vol": random.randint(50, 500),
            "ask_vol": random.randint(50, 500), "aggressor": agg,
            "volume_total": self.volumes[inst],
        }

    def start(self):
        self.running = True
        def run():
            print("模拟行情引擎启动")
            while self.running:
                for inst in INSTRUMENTS:
                    tick = self.generate_tick(inst)
                    store.update_tick(inst, tick)
                time.sleep(0.5)
        threading.Thread(target=run, daemon=True).start()


engine = MockEngine()

# ============ WebSocket 连接管理 ============
class ConnectionManager:
    def __init__(self):
        self.active_connections = []
        self._lock = threading.Lock()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        with self._lock:
            self.active_connections.append(websocket)
        print(f"WebSocket 连接建立，当前 {len(self.active_connections)} 个客户端")

    def disconnect(self, websocket: WebSocket):
        with self._lock:
            if websocket in self.active_connections:
                self.active_connections.remove(websocket)
        print(f"WebSocket 断开，当前 {len(self.active_connections)} 个客户端")

    async def broadcast(self, message: dict):
        msg = json.dumps(message)
        dead = []
        with self._lock:
            connections = list(self.active_connections)

        for connection in connections:
            try:
                await connection.send_text(msg)
            except:
                dead.append(connection)

        for connection in dead:
            self.disconnect(connection)


manager = ConnectionManager()


async def data_pusher():
    print("数据推送启动")
    push_count = 0
    while True:
        await asyncio.sleep(1)
        push_count += 1

        if not manager.active_connections:
            if push_count % 10 == 0:
                print(f"等待客户端连接... (总Tick:{store.tick_count})")
            continue

        data = {
            "type": "update",
            "timestamp": int(time.time() * 1000),
            "instruments": {},
            "selected_orderflow": [],
            "selected_inst": store.selected_instrument,
            "selected_name": INSTRUMENT_NAMES.get(store.selected_instrument, store.selected_instrument),
        }

        for inst in INSTRUMENTS:
            tick = store.last_ticks.get(inst)
            if tick:
                data["instruments"][inst] = {
                    "last_price": tick["price"],
                    "volume": tick["volume_total"],
                    "delta": tick["delta"],
                }

        sel = store.selected_instrument
        data["selected_orderflow"] = store.get_orderflow(sel, 20)

        await manager.broadcast(data)
        if push_count % 5 == 0:
            print(f"第{push_count}次推送，{len(manager.active_connections)}个客户端")


# ============ API 路由 ============
@app.get("/", response_class=HTMLResponse)
async def index():
    with open("page.html", "r", encoding="utf-8") as f:
        html = f.read()
    html = html.replace("$INSTRUMENTS_JSON", json.dumps(INSTRUMENTS))
    html = html.replace("$INSTRUMENT_NAMES_JSON", json.dumps(INSTRUMENT_NAMES))
    return HTMLResponse(content=html)

@app.get("/js/echarts.min.js")
async def serve_echarts():
    return FileResponse("js/echarts.min.js", media_type="application/javascript")

@app.get("/api/klines")
async def api_klines(inst: str = "rb2510", period: str = "1m"):
    print(f"API请求: {inst} {period}")

    klines = store.get_klines(inst, period)
    macd = store.get_macd(inst, period)

    print(f"返回: {inst} {period}, {len(klines)} 根K线")

    return {
        "instrument": inst,
        "period": period,
        "klines": klines,
        "macd": macd,
    }


@app.post("/api/select")
async def api_select(inst: str = "rb2510"):
    if inst in INSTRUMENTS:
        store.selected_instrument = inst
        print(f"用户选择品种: {inst}")
    return {"selected": store.selected_instrument}


# ============ WebSocket 路由 ============
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)

    try:
        initial_data = {
            "type": "update",
            "timestamp": int(time.time() * 1000),
            "instruments": {},
            "selected_orderflow": store.get_orderflow(store.selected_instrument, 20),
            "selected_inst": store.selected_instrument,
            "selected_name": INSTRUMENT_NAMES.get(store.selected_instrument, store.selected_instrument),
        }
        for inst in INSTRUMENTS:
            tick = store.last_ticks.get(inst)
            if tick:
                initial_data["instruments"][inst] = {
                    "last_price": tick["price"],
                    "volume": tick["volume_total"],
                    "delta": tick["delta"],
                }
        await websocket.send_text(json.dumps(initial_data))
        print(f"发送初始数据给客户端")
    except Exception as e:
        print(f"初始数据发送失败: {e}")

    try:
        while True:
            msg = await websocket.receive_text()
            if msg:
                try:
                    data = json.loads(msg)
                    if data.get('action') == 'select':
                        inst = data.get('instrument')
                        if inst in INSTRUMENTS:
                            store.selected_instrument = inst
                except:
                    pass
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        print(f"WebSocket 错误: {e}")
        manager.disconnect(websocket)


# ============ 启动 ============
@app.on_event("startup")
async def startup_event():
    print("=" * 60)
    print("CTP 订单流 Web 服务启动中...")
    print("=" * 60)
    print(f"监控品种: {', '.join(INSTRUMENTS)}")
    print(f"监控周期: 1m, 5m, 15m, 1h")
    print(f"\\n请用浏览器打开: http://localhost:8080")
    print("=" * 60 + "\\n")

    store.init_bars()
    engine.start()
    asyncio.create_task(data_pusher())


if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=8080)
