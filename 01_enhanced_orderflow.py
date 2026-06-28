#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
01_enhanced_orderflow.py
========================
增强版：同时显示【原始订单流】和【聚合K线】
支持多合约（5-8个品种）同时监控

运行方式：
  python 01_enhanced_orderflow.py
"""

import time
import random
import sys
import os
from collections import deque, defaultdict
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core import OrderFlowCalculator, KlineAggregator
from config import get_instrument_info, PERIODS


# ==================== 1. 原始订单流数据结构 ====================
class RawOrderFlow:
    """
    原始订单流数据 - 每一笔成交的明细
    这是 CTP 推送的最原始数据，未经任何聚合
    """
    def __init__(self):
        self.buffer = deque(maxlen=5000)  # 保留最近5000笔
        self.total_buy_vol = 0
        self.total_sell_vol = 0
        self.total_delta = 0

    def add(self, flow_data):
        """添加一笔订单流数据"""
        self.buffer.append(flow_data)
        self.total_buy_vol += flow_data['buy_vol']
        self.total_sell_vol += flow_data['sell_vol']
        self.total_delta += flow_data['delta']

    def get_recent(self, n=20):
        """获取最近n笔"""
        return list(self.buffer)[-n:]

    def get_stats(self, window=100):
        """获取最近window笔的统计"""
        recent = list(self.buffer)[-window:]
        if not recent:
            return {}
        buy_vol = sum(r['buy_vol'] for r in recent)
        sell_vol = sum(r['sell_vol'] for r in recent)
        delta = sum(r['delta'] for r in recent)
        avg_price = sum(r['price'] * r['total_vol'] for r in recent) / sum(r['total_vol'] for r in recent) if recent else 0
        return {
            'window': len(recent),
            'buy_vol': buy_vol,
            'sell_vol': sell_vol,
            'delta': delta,
            'avg_price': round(avg_price, 2),
            'buy_ratio': round(buy_vol / (buy_vol + sell_vol) * 100, 1) if (buy_vol + sell_vol) > 0 else 50,
        }

    def print_recent(self, n=10):
        """打印最近n笔原始订单流"""
        recent = self.get_recent(n)
        if not recent:
            return
        print(f"\n{'─' * 80}")
        print(f"📋 原始订单流明细 (最近 {len(recent)} 笔)")
        print(f"{'─' * 80}")
        print(f"{'时间':<12} {'价格':>8} {'成交量':>6} {'主动买':>6} {'主动卖':>6} {'Δ':>6} {'方向':>6}")
        print(f"{'─' * 80}")
        for r in reversed(recent):  # 最新的在上面
            direction = "🟢BUY" if r['aggressor'] == 'BUY' else "🔴SELL" if r['aggressor'] == 'SELL' else "⚪MIX"
            print(f"{r['time']:<12} {r['price']:>8.2f} {r['total_vol']:>6} {r['buy_vol']:>6} {r['sell_vol']:>6} {r['delta']:+6d} {direction:>6}")

        # 打印统计
        stats = self.get_stats(100)
        if stats:
            print(f"{'─' * 80}")
            print(f"📊 最近100笔统计: 主动买 {stats['buy_vol']} | 主动卖 {stats['sell_vol']} | "
                  f"净Δ {stats['delta']:+d} | 买占比 {stats['buy_ratio']}% | 均价 {stats['avg_price']}")

            # 趋势判断
            if stats['buy_ratio'] > 60:
                trend = "🟢 强势买盘主导"
            elif stats['buy_ratio'] < 40:
                trend = "🔴 强势卖盘主导"
            else:
                trend = "⚪ 买卖均衡"
            print(f"📈 趋势判断: {trend}")


# ==================== 2. 多合约监控器 ====================
class MultiInstrumentMonitor:
    """多合约同时监控器"""

    def __init__(self, instruments, periods, show_raw=True, show_kline=True):
        """
        Args:
            instruments: 合约列表，如 ["rb2510", "i2509", "cu2509"]
            periods: 周期列表，如 ["1m", "5m"]
            show_raw: 是否显示原始订单流
            show_kline: 是否显示聚合K线
        """
        self.instruments = instruments
        self.periods = periods
        self.show_raw = show_raw
        self.show_kline = show_kline

        # 每个合约一个模拟市场
        self.markets = {}
        # 每个(合约,周期)一个订单流计算器 + K线聚合器
        self.calculators = {}
        self.aggregators = {}
        # 每个合约一个原始订单流缓存
        self.raw_flows = {}

        for inst in instruments:
            info = get_instrument_info(inst)
            base_price = info.get('base_price', 3456.0) if info else 3456.0
            self.markets[inst] = MockMarket(inst, base_price)
            self.raw_flows[inst] = RawOrderFlow()

            for period_name in periods:
                key = f"{inst}_{period_name}"
                self.calculators[key] = OrderFlowCalculator()
                self.aggregators[key] = KlineAggregator(PERIODS[period_name])

        self.tick_count = 0
        self.last_print_time = 0

    def run(self):
        """主循环"""
        print("=" * 90)
        print("🧪 CTP 订单流系统 - 增强版（原始订单流 + K线聚合 + 多合约）")
        print("=" * 90)
        print(f"\n监控合约: {', '.join(self.instruments)} ({len(self.instruments)}个)")
        print(f"监控周期: {', '.join(self.periods)} ({len(self.periods)}个)")
        print(f"显示模式: 原始订单流={'✅' if self.show_raw else '❌'}  K线聚合={'✅' if self.show_kline else '❌'}")

        print(f"\n合约详情:")
        for inst in self.instruments:
            info = get_instrument_info(inst)
            if info:
                print(f"  {inst:>8} | {info['name']:<6} | {info['exchange']:<6} | 基准价:{info['base_price']:>10.2f}")

        print("\n" + "=" * 90)
        print("按 Ctrl+C 停止\n")

        try:
            while True:
                for inst in self.instruments:
                    tick = self.markets[inst].next()

                    for period_name in self.periods:
                        key = f"{inst}_{period_name}"
                        flow = self.calculators[key].on_tick(tick)

                        if flow:
                            # 保存原始订单流
                            self.raw_flows[inst].add(flow)
                            # K线聚合
                            self.aggregators[key].add_tick(flow)

                    self.tick_count += 1

                # 定期打印（每2秒或每30个tick）
                now = time.time()
                if now - self.last_print_time >= 2.0 or self.tick_count % 30 == 0:
                    self._print_status()
                    self.last_print_time = now

                time.sleep(0.2)  # 加快模拟速度

        except KeyboardInterrupt:
            self._print_final_summary()

    def _print_status(self):
        """打印当前状态"""
        print(f"\n{'='*90}")
        print(f"⏰ {datetime.now().strftime('%H:%M:%S')} | 总Tick: {self.tick_count}")
        print(f"{'='*90}")

        for inst in self.instruments:
            market = self.markets[inst]
            raw = self.raw_flows[inst]

            print(f"\n【{inst}】最新价: {market.LastPrice:.2f} | 总成交量: {market.Volume}")

            # 1. 原始订单流（如果开启）
            if self.show_raw:
                raw.print_recent(8)  # 显示最近8笔

            # 2. K线聚合（如果开启）
            if self.show_kline:
                for period_name in self.periods:
                    key = f"{inst}_{period_name}"
                    agg = self.aggregators[key]
                    bar_count = len(agg.bars)
                    if agg.current_bar:
                        bar = agg.current_bar
                        print(f"  📊 [{period_name}] K线: {bar_count}根完成 | "
                              f"O:{bar['open']:.2f} H:{bar['high']:.2f} L:{bar['low']:.2f} C:{bar['close']:.2f} | "
                              f"Vol:{bar['volume']} Buy:{bar['buy_vol']} Sell:{bar['sell_vol']} Δ:{bar['delta']:+d}")

    def _print_final_summary(self):
        """最终汇总"""
        print(f"\n{'='*90}")
        print(f"✅ 测试完成！共处理 {self.tick_count} 个 Tick")
        print(f"{'='*90}")

        for inst in self.instruments:
            raw = self.raw_flows[inst]
            print(f"\n【{inst}】")
            print(f"  原始订单流: 总买 {raw.total_buy_vol} | 总卖 {raw.total_sell_vol} | 净Δ {raw.total_delta:+d}")

            for period_name in self.periods:
                key = f"{inst}_{period_name}"
                agg = self.aggregators[key]
                print(f"  K线 [{period_name}]: {len(agg.bars)} 根完成")


# ==================== 3. 模拟市场（增强版） ====================
class MockMarket:
    """模拟单个合约的市场行情"""

    def __init__(self, instrument, base_price):
        self.InstrumentID = instrument
        self._base_price = base_price
        self.LastPrice = base_price
        self.Volume = 0
        self.Turnover = 0.0
        self.BidPrice1 = base_price - 0.5
        self.BidVolume1 = 150
        self.AskPrice1 = base_price + 0.5
        self.AskVolume1 = 200
        self.UpdateTime = datetime.now().strftime("%H:%M:%S")
        self.UpdateMillisec = 500
        self.OpenInterest = 500000

        # 每个合约有自己的波动特性
        self.volatility = random.uniform(1.5, 4.0)
        self.trend_bias = random.uniform(-0.3, 0.3)  # 轻微趋势偏向

    def next(self):
        """产生下一个 Tick"""
        price_change = random.gauss(self.trend_bias, self.volatility)
        self.LastPrice = round(self._base_price + price_change, 2)
        self.LastPrice = max(self._base_price * 0.95, min(self._base_price * 1.05, self.LastPrice))

        # 成交量：有时大单，有时没成交
        if random.random() > 0.2:
            vol_increment = random.randint(1, 80)
        else:
            vol_increment = 0
        self.Volume += vol_increment

        spread = random.uniform(0.5, 2.0)
        self.BidPrice1 = round(self.LastPrice - spread/2, 2)
        self.AskPrice1 = round(self.LastPrice + spread/2, 2)
        self.BidVolume1 = random.randint(30, 500)
        self.AskVolume1 = random.randint(30, 500)

        now = datetime.now()
        self.UpdateTime = now.strftime("%H:%M:%S")
        self.UpdateMillisec = now.microsecond // 1000

        return self


def main():
    """主函数 - 在这里配置监控参数"""

    # ==================== 配置区域 ====================
    # 合约列表：可以自由添加/删除，支持5-8个品种同时监控
    instruments = [
        "rb2510",   # 螺纹钢 (黑色系)
        "i2509",    # 铁矿石 (黑色系)
        "cu2509",   # 铜 (有色)
        "au2512",   # 黄金 (贵金属)
        "IF2509",   # 沪深300 (股指)
        # "m2509",    # 豆粕 (农产品) - 取消注释即可添加
        # "TA509",    # PTA (化工)
        # "sc2509",   # 原油 (能源)
    ]

    # 周期列表：可以同时维护多个周期
    periods = [
        "1m",       # 1分钟线
        "5m",       # 5分钟线
        # "15m",      # 15分钟线 - 取消注释即可添加
    ]

    # 显示模式
    show_raw_orderflow = True    # 显示原始订单流明细
    show_kline_aggregation = True  # 显示K线聚合结果
    # =================================================

    monitor = MultiInstrumentMonitor(
        instruments=instruments,
        periods=periods,
        show_raw=show_raw_orderflow,
        show_kline=show_kline_aggregation,
    )
    monitor.run()


if __name__ == "__main__":
    main()
