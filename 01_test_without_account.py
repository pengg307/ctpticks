#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
01_test_without_account.py
==========================
纯 Python 实现，零外部依赖
验证订单流引擎和 K线聚合器，无需 CTP 账号

运行方式：
  python 01_test_without_account.py
"""

import time
import random
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core import OrderFlowCalculator, KlineAggregator
from config import get_instrument_info, PERIODS
from datetime import datetime


class MockTick:
    """模拟 CTP 行情数据结构，字段名与真实 CTP 完全一致"""
    def __init__(self, instrument="rb2510", base_price=3456.0):
        self.InstrumentID = instrument
        self.UpdateTime = datetime.now().strftime("%H:%M:%S")
        self.UpdateMillisec = 500
        self.LastPrice = base_price
        self.Volume = 0
        self.Turnover = 0.0
        self.BidPrice1 = base_price - 0.5
        self.BidVolume1 = 150
        self.AskPrice1 = base_price + 0.5
        self.AskVolume1 = 200
        self.BidPrice2 = base_price - 1.0
        self.BidVolume2 = 300
        self.AskPrice2 = base_price + 1.0
        self.AskVolume2 = 250
        self.OpenInterest = 500000
        self._base_price = base_price

    def next(self):
        """产生下一个模拟 Tick"""
        price_change = random.gauss(0, 2.5)
        self.LastPrice = round(self._base_price + price_change, 2)
        self.LastPrice = max(self._base_price - 60, min(self._base_price + 60, self.LastPrice))

        vol_increment = random.randint(0, 50)
        if random.random() > 0.3:
            vol_increment = random.randint(1, 100)
        self.Volume += vol_increment

        spread = 1.0
        self.BidPrice1 = round(self.LastPrice - spread/2, 2)
        self.AskPrice1 = round(self.LastPrice + spread/2, 2)
        self.BidVolume1 = random.randint(50, 300)
        self.AskVolume1 = random.randint(50, 300)

        now = datetime.now()
        self.UpdateTime = now.strftime("%H:%M:%S")
        self.UpdateMillisec = now.microsecond // 1000

        return self


class MultiInstrumentTester:
    """多合约测试器"""

    def __init__(self, instruments, periods):
        self.instruments = instruments
        self.periods = periods
        self.markets = {}
        self.calculators = {}
        self.aggregators = {}

        for inst in instruments:
            info = get_instrument_info(inst)
            base_price = info.get('base_price', 3456.0) if info else 3456.0
            self.markets[inst] = MockTick(inst, base_price)

            for period_name in periods:
                key = f"{inst}_{period_name}"
                self.calculators[key] = OrderFlowCalculator()
                self.aggregators[key] = KlineAggregator(PERIODS[period_name])

        self.tick_count = 0

    def run(self):
        print("=" * 70)
        print("🧪 CTP 订单流系统 - 模拟测试模式（纯Python，零依赖）")
        print("=" * 70)
        print(f"\n测试合约: {', '.join(self.instruments)}")
        print(f"测试周期: {', '.join(self.periods)}")
        print(f"\n合约信息:")
        for inst in self.instruments:
            info = get_instrument_info(inst)
            if info:
                print(f"  {inst}: {info['name']} ({info['exchange']}) 基准价:{info['base_price']}")
        print("\n按 Ctrl+C 停止\n")

        try:
            while True:
                for inst in self.instruments:
                    tick = self.markets[inst].next()

                    for period_name in self.periods:
                        key = f"{inst}_{period_name}"
                        flow = self.calculators[key].on_tick(tick)

                        if flow:
                            self.aggregators[key].add_tick(flow)

                    self.tick_count += 1

                if self.tick_count % 30 == 0:
                    print(f"\n📊 已处理 {self.tick_count} 个 Tick")
                    for inst in self.instruments:
                        for period_name in self.periods:
                            key = f"{inst}_{period_name}"
                            agg = self.aggregators[key]
                            bar_count = len(agg.bars)
                            if agg.current_bar:
                                bar = agg.current_bar
                                print(f"  {inst} [{period_name}]: {bar_count}根K线 | "
                                      f"当前Δ:{bar['delta']:+d} Buy:{bar['buy_vol']} Sell:{bar['sell_vol']}")

                time.sleep(0.3)

        except KeyboardInterrupt:
            print(f"\n\n✅ 测试完成！共处理 {self.tick_count} 个 Tick")
            for inst in self.instruments:
                for period_name in self.periods:
                    key = f"{inst}_{period_name}"
                    agg = self.aggregators[key]
                    print(f"  {inst} [{period_name}]: {len(agg.bars)} 根完整K线")


def main():
    """主函数 - 可自由修改合约和周期"""

    # ====== 在这里配置测试参数 ======
    instruments = [
        "rb2510",   # 螺纹钢
        # "i2509",    # 铁矿石
        # "cu2509",   # 铜
    ]

    periods = [
        "1m",       # 1分钟
        # "5m",       # 5分钟
    ]
    # =================================

    tester = MultiInstrumentTester(instruments, periods)
    tester.run()


if __name__ == "__main__":
    main()
