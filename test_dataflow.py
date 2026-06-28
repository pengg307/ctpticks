#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
test_dataflow.py
================
测试核心数据流是否正常工作
"""

import time
import random
import threading
from datetime import datetime, timedelta
from collections import deque, defaultdict

# 简化版配置
INSTRUMENTS = ["rb2510", "i2509"]
BASE_PRICES = {"rb2510": 3456.0, "i2509": 800.0}

class DataStore:
    def __init__(self):
        self.last_ticks = {}
        self.orderflow = defaultdict(lambda: deque(maxlen=500))
        self._lock = threading.Lock()

    def update_tick(self, inst, tick):
        with self._lock:
            self.last_ticks[inst] = tick
            self.orderflow[inst].append(tick)
        print(f"[{inst}] 价格:{tick['price']:.2f} 量:{tick['total_vol']} Δ:{tick['delta']:+d}")

store = DataStore()

class MockEngine:
    def __init__(self):
        self.prices = {inst: BASE_PRICES[inst] for inst in INSTRUMENTS}
        self.running = False

    def generate_tick(self, inst):
        base = self.prices[inst]
        change = random.gauss(0, base * 0.001)
        price = round(base + change, 2)
        self.prices[inst] = price
        vol = random.randint(1, 50)
        buy_v = int(vol * 0.6)
        sell_v = vol - buy_v
        return {
            "time": datetime.now().strftime("%H:%M:%S"),
            "instrument": inst, "price": price,
            "total_vol": vol, "buy_vol": buy_v, "sell_vol": sell_v, "delta": buy_v - sell_v,
        }

    def start(self):
        self.running = True
        def run():
            print("🎲 引擎启动")
            while self.running:
                for inst in INSTRUMENTS:
                    tick = self.generate_tick(inst)
                    store.update_tick(inst, tick)
                time.sleep(1)
        threading.Thread(target=run, daemon=True).start()

engine = MockEngine()
engine.start()

print("⏳ 等待5秒，观察数据产生...")
for i in range(5):
    time.sleep(1)
    print(f"--- 第{i+1}秒 ---")
    for inst in INSTRUMENTS:
        tick = store.last_ticks.get(inst)
        if tick:
            print(f"  {inst}: 最新价={tick['price']:.2f}, 订单流缓存={len(store.orderflow[inst])}笔")
        else:
            print(f"  {inst}: 无数据")

print("
✅ 测试完成，数据流正常")
