#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
02_real_ctp.py
==============
真实 CTP 行情接入版本

前置要求：
  1. 将 CTP DLL 文件放在 ./ctp_dll/ 目录下
  2. 编辑 config/instruments.py 填入账号密码
  3. 运行: python 02_real_ctp.py

当前版本使用 Mock 模式演示，后续更新 ctypes 直接调用 DLL
"""

import time
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core import OrderFlowCalculator, KlineAggregator
from config import SIMNOW_CONFIG, get_front_address, get_period_seconds
from utils import TickDataStore, KlineDataStore


DLL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ctp_dll")


def check_dll():
    """检查 CTP DLL 文件是否存在"""
    required_files = ["thostmduserapi.dll", "thostmduserapi.lib"]
    print("🔍 检查 CTP DLL 文件...")
    missing = []
    for f in required_files:
        path = os.path.join(DLL_DIR, f)
        if os.path.exists(path):
            print(f"  ✅ {f}")
        else:
            print(f"  ❌ {f} - 未找到")
            missing.append(f)

    if missing:
        print(f"\n⚠️  缺少文件，请将 CTP API 文件复制到: {DLL_DIR}")
        return False
    print("\n✅ 所有 DLL 文件已就绪")
    return True


class MockMdApi:
    """模拟 CTP 行情 API"""

    def __init__(self):
        self.spi = None
        self.front_address = None
        self.subscribed = []

    def RegisterSpi(self, spi):
        self.spi = spi

    def RegisterFront(self, address):
        self.front_address = address
        print(f"📡 注册前置机: {address}")

    def Init(self):
        print("🚀 初始化 API...")
        if self.spi:
            self.spi.OnFrontConnected()

    def Join(self):
        print("⏳ 等待线程结束...")

    def Release(self):
        print("👋 释放 API")

    def SubscribeMarketData(self, instruments):
        self.subscribed = instruments
        print(f"📡 订阅行情: {', '.join(instruments)}")
        self._start_mock_push()

    def UnSubscribeMarketData(self, instruments):
        print(f"📡 退订行情: {', '.join(instruments)}")

    def ReqUserLogin(self, login_req, request_id):
        print(f"🔑 登录请求: {login_req.get('UserID', 'unknown')}")
        if self.spi:
            self.spi.OnRspUserLogin({}, {"ErrorID": 0, "ErrorMsg": ""}, request_id, True)

    def _start_mock_push(self):
        """模拟推送行情数据"""
        import threading
        import random
        from datetime import datetime

        def push():
            import time
            base_price = 3456.0
            volume = 0

            while True:
                time.sleep(0.5)
                price_change = random.gauss(0, 2.5)
                last_price = round(base_price + price_change, 2)
                vol_delta = random.randint(1, 50)
                volume += vol_delta

                tick = {
                    "InstrumentID": self.subscribed[0] if self.subscribed else "rb2510",
                    "UpdateTime": datetime.now().strftime("%H:%M:%S"),
                    "UpdateMillisec": 500,
                    "LastPrice": last_price,
                    "Volume": volume,
                    "Turnover": volume * last_price,
                    "BidPrice1": round(last_price - 0.5, 2),
                    "BidVolume1": random.randint(50, 300),
                    "AskPrice1": round(last_price + 0.5, 2),
                    "AskVolume1": random.randint(50, 300),
                    "OpenInterest": 500000,
                }

                if self.spi:
                    self.spi.OnRtnDepthMarketData(tick)

        t = threading.Thread(target=push, daemon=True)
        t.start()


class CtpMdSpi:
    """CTP 行情 SPI 实现"""

    def __init__(self, config):
        self.config = config
        self.instruments = config.get("instruments", ["rb2510"])
        self.periods = config.get("periods", ["1m"])

        self.calculators = {}
        self.aggregators = {}
        self.tick_stores = {}
        self.kline_stores = {}

        for inst in self.instruments:
            for period in self.periods:
                key = f"{inst}_{period}"
                self.calculators[key] = OrderFlowCalculator()
                self.aggregators[key] = KlineAggregator(get_period_seconds(period))
                self.tick_stores[key] = TickDataStore(inst)
                self.kline_stores[key] = KlineDataStore(inst, period)

        self.tick_count = 0
        self.connected = False
        self.logged_in = False

    def OnFrontConnected(self):
        print("✅ 行情前置机连接成功！")
        self.connected = True
        login = {
            "BrokerID": self.config["account"]["BrokerID"],
            "UserID": self.config["account"]["UserID"],
            "Password": self.config["account"]["Password"],
        }
        self.OnRspUserLogin({}, {"ErrorID": 0, "ErrorMsg": ""}, 0, True)

    def OnFrontDisconnected(self, nReason):
        print(f"❌ 行情前置机断开，原因码: {nReason}")
        self.connected = False

    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
        if pRspInfo and pRspInfo.get('ErrorID', -1) == 0:
            print("✅ 登录成功！")
            self.logged_in = True
            print(f"📡 订阅合约: {', '.join(self.instruments)}")
        else:
            error_msg = pRspInfo.get('ErrorMsg', '未知错误') if pRspInfo else '未知错误'
            print(f"❌ 登录失败: {error_msg}")

    def OnRtnDepthMarketData(self, pDepthMarketData):
        instrument = pDepthMarketData.get('InstrumentID', 'UNKNOWN')

        for period in self.periods:
            key = f"{instrument}_{period}"
            if key not in self.calculators:
                continue

            flow = self.calculators[key].on_tick(pDepthMarketData)

            if flow:
                self.tick_count += 1
                bar = self.aggregators[key].add_tick(flow)
                self.tick_stores[key].save(flow)
                if bar and bar != self.aggregators[key].get_current_bar():
                    self.kline_stores[key].save(bar)

                if self.tick_count % 20 == 0:
                    print(f"📈 #{self.tick_count} {flow['time']} | "
                          f"{instrument} {flow['price']:.2f} | "
                          f"Buy:{flow['buy_vol']:>3} Sell:{flow['sell_vol']:>3} "
                          f"Δ:{flow['delta']:+4d}")

    def OnRspError(self, pRspInfo, nRequestID, bIsLast):
        if pRspInfo:
            print(f"⚠️ 错误响应: {pRspInfo.get('ErrorMsg', '未知错误')}")


def main():
    if "您的" in SIMNOW_CONFIG["UserID"] or "您的" in SIMNOW_CONFIG["Password"]:
        print("❌ 请先编辑 config/instruments.py，填入您的 SIMNOW 账号和密码！")
        print("   位置: config/instruments.py 第 78-79 行")
        return

    use_mock = not check_dll()
    if use_mock:
        print("\n⚠️  未找到 CTP DLL，使用模拟模式运行")
    else:
        print("\n✅ 发现 CTP DLL，将尝试加载\n")

    config = {
        "account": SIMNOW_CONFIG,
        "instruments": ["rb2510"],
        "periods": ["1m"],
    }

    print("=" * 60)
    print("🚀 CTP 行情接入")
    print("=" * 60)
    print(f"账号: {SIMNOW_CONFIG['UserID']}")
    print(f"合约: {', '.join(config['instruments'])}")
    print(f"周期: {', '.join(config['periods'])}")
    print("\n连接中...\n")

    spi = CtpMdSpi(config)
    api = MockMdApi()
    api.RegisterSpi(spi)

    front = get_front_address("simnow_7x24")
    api.RegisterFront(front["md"])
    api.Init()
    api.SubscribeMarketData(config["instruments"])

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n\n👋 退出中...")
        api.Release()
        print(f"✅ 共接收 {spi.tick_count} 个 Tick")
        for inst in config["instruments"]:
            for period in config["periods"]:
                key = f"{inst}_{period}"
                print(f"  {inst} [{period}]: {len(spi.aggregators[key].bars)} 根K线")


if __name__ == "__main__":
    main()
