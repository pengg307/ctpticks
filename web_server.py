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

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import duckdb  # [DuckDB] 嵌入式数据库
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
    "ORDERFLOW_MAXLEN": 2000,    # [FIX-SCROLL] 订单流内存保留条数 500->2000
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

# ============ DuckDB 存储管理器（替换 CSVManager）============
class DuckDBManager:
    """使用 DuckDB 嵌入式数据库存储 Tick 和 K 线数据

    优势:
    - 列式存储+自动压缩，磁盘空间比 CSV 省 70-80%
    - SQL 查询，分析性能比 CSV 快 10-100 倍
    - 单文件，备份只需复制一个 .duckdb 文件
    - 原生支持时间序列函数、窗口函数
    - 一行 SQL 导出 CSV: COPY ... TO 'file.csv'
    """

    def __init__(self, data_dir=DATA_DIR):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self.db_path = os.path.join(data_dir, "ctp_data.duckdb")

        # 连接 DuckDB（单连接，多线程安全）
        import duckdb
        self.conn = duckdb.connect(self.db_path)
        self._init_tables()
        print(f"[DuckDB] 数据库初始化: {os.path.abspath(self.db_path)}")

    def _init_tables(self):
        """创建表结构（如果不存在）"""
        # Tick 原始数据表
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
                total_volume BIGINT
            )
        """)

        # K 线数据表
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
                tick_count INT
            )
        """)

        # 创建索引（加速查询）
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_ticks_inst_time ON ticks(instrument, timestamp)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_klines_inst_period_time ON klines(instrument, period, timestamp)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_ticks_date ON ticks(date_str)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_klines_date ON klines(date_str)")

        # 创建序列用于自增ID
        self.conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_tick_id START 1")
        self.conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_kline_id START 1")

    def save_tick(self, inst, tick):
        """保存单条 Tick（自动事务，无需手动 flush）"""
        try:
            self.conn.execute("""
                INSERT INTO ticks 
                (id, timestamp, date_str, instrument, price, volume, buy_vol, sell_vol, delta, 
                 bid, ask, bid_vol, ask_vol, aggressor, total_volume)
                VALUES (
                    nextval('seq_tick_id'),
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?
                )
            """, [
                self._parse_time(tick.get("time", "")),
                datetime.now().strftime("%Y%m%d"),
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
                tick.get("volume_total", 0)
            ])
        except Exception as e:
            print(f"[DuckDB] Tick 写入错误: {e}")

    def save_kline(self, inst, period, bar):
        """保存单根 K 线"""
        try:
            self.conn.execute("""
                INSERT INTO klines 
                (id, timestamp, date_str, instrument, period, open, high, low, close,
                 volume, buy_vol, sell_vol, delta, tick_count)
                VALUES (
                    nextval('seq_kline_id'),
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?
                )
            """, [
                self._parse_time(bar.get("time", "")),
                datetime.now().strftime("%Y%m%d"),
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
                bar.get("tick_count", 1)
            ])
        except Exception as e:
            print(f"[DuckDB] K线写入错误: {e}")

    def load_klines(self, inst, period, limit=200):
        """加载 K 线历史数据（替代 CSV 加载）"""
        try:
            result = self.conn.execute("""
                SELECT 
                    strftime(timestamp, '%H:%M') as time,
                    epoch_ms(timestamp) as timestamp_ms,
                    open, high, low, close,
                    volume, buy_vol, sell_vol, delta, tick_count
                FROM klines
                WHERE instrument = ? AND period = ?
                ORDER BY timestamp DESC
                LIMIT ?
            """, [inst, period, limit]).fetchall()

            # 转换为字典列表（兼容原有格式）
            klines = []
            for row in reversed(result):  # 反转回时间正序
                klines.append({
                    "time": row[0],
                    "timestamp": row[1],
                    "open": float(row[2]),
                    "high": float(row[3]),
                    "low": float(row[4]),
                    "close": float(row[5]),
                    "volume": int(row[6]),
                    "buy_vol": int(row[7]),
                    "sell_vol": int(row[8]),
                    "delta": int(row[9]),
                    "tick_count": int(row[10])
                })
            return klines
        except Exception as e:
            print(f"[DuckDB] K线加载错误: {e}")
            return []

    def load_ticks(self, inst, limit=500):
        """加载 Tick 历史数据"""
        try:
            result = self.conn.execute("""
                SELECT 
                    strftime(timestamp, '%H:%M:%S') as time,
                    '0' as millisec,
                    instrument,
                    price,
                    volume as total_vol,
                    buy_vol,
                    sell_vol,
                    delta,
                    bid,
                    ask,
                    bid_vol,
                    ask_vol,
                    aggressor
                FROM ticks
                WHERE instrument = ?
                ORDER BY timestamp DESC
                LIMIT ?
            """, [inst, limit]).fetchall()

            ticks = []
            for row in reversed(result):
                ticks.append({
                    "time": row[0],
                    "millisec": row[1],
                    "instrument": row[2],
                    "price": float(row[3]),
                    "total_vol": int(row[4]),
                    "buy_vol": int(row[5]),
                    "sell_vol": int(row[6]),
                    "delta": int(row[7]),
                    "bid": float(row[8]),
                    "ask": float(row[9]),
                    "bid_vol": int(row[10]),
                    "ask_vol": int(row[11]),
                    "aggressor": row[12]
                })
            return ticks
        except Exception as e:
            print(f"[DuckDB] Tick加载错误: {e}")
            return []

    def cleanup_old_data(self, tick_days=3, kline_1m_days=60, kline_other_days=7):
        """清理过期数据（替代 CSV 文件删除）"""
        try:
            now = datetime.now()

            # 清理旧 Tick
            tick_cutoff = (now - timedelta(days=tick_days)).strftime("%Y-%m-%d")
            self.conn.execute("DELETE FROM ticks WHERE date_str < ?", [tick_cutoff.replace("-", "")])
            tick_deleted = self.conn.execute("SELECT changes()").fetchone()[0]

            # 清理旧 1m K线
            kline_1m_cutoff = (now - timedelta(days=kline_1m_days)).strftime("%Y-%m-%d")
            self.conn.execute("DELETE FROM klines WHERE period = '1m' AND date_str < ?", [kline_1m_cutoff.replace("-", "")])

            # 清理旧其他周期 K线
            kline_other_cutoff = (now - timedelta(days=kline_other_days)).strftime("%Y-%m-%d")
            self.conn.execute("DELETE FROM klines WHERE period IN ('5m', '15m', '1h') AND date_str < ?", [kline_other_cutoff.replace("-", "")])

            # 压缩数据库（释放空间）
            self.conn.execute("VACUUM")

            print(f"[DuckDB] 清理完成，删除 {tick_deleted} 条旧Tick，已VACUUM")
            return tick_deleted
        except Exception as e:
            print(f"[DuckDB] 清理错误: {e}")
            return 0

    def export_to_csv(self, table, filepath, where_clause="", params=None):
        """导出数据到 CSV（给外部系统用）

        示例:
            export_to_csv("ticks", "./export/ticks_20250630.csv", 
                         "WHERE date_str = '20250630'")
        """
        try:
            os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
            query = f"COPY (SELECT * FROM {table} {where_clause}) TO '{filepath}' (HEADER, DELIMITER ',')"
            self.conn.execute(query)
            print(f"[DuckDB] 导出完成: {filepath}")
            return True
        except Exception as e:
            print(f"[DuckDB] 导出错误: {e}")
            return False

    def query(self, sql, params=None):
        """执行自定义 SQL 查询（给外部系统读数据用）"""
        try:
            if params:
                return self.conn.execute(sql, params).fetchall()
            else:
                return self.conn.execute(sql).fetchall()
        except Exception as e:
            print(f"[DuckDB] 查询错误: {e}")
            return []

    def get_stats(self):
        """获取数据库统计信息"""
        try:
            tick_count = self.conn.execute("SELECT COUNT(*) FROM ticks").fetchone()[0]
            kline_count = self.conn.execute("SELECT COUNT(*) FROM klines").fetchone()[0]
            db_size = os.path.getsize(self.db_path)
            return {
                "tick_count": tick_count,
                "kline_count": kline_count,
                "db_size_mb": round(db_size / 1024 / 1024, 2),
                "db_path": self.db_path
            }
        except Exception as e:
            print(f"[DuckDB] 统计错误: {e}")
            return {}

    def force_flush(self):
        """强制刷盘（DuckDB 自动事务，无需手动 flush）"""
        try:
            self.conn.execute("CHECKPOINT")
            print("[DuckDB] 强制刷盘完成")
        except Exception as e:
            print(f"[DuckDB] 刷盘错误: {e}")

    def close(self):
        """关闭连接"""
        try:
            self.conn.execute("CHECKPOINT")
            self.conn.close()
            print("[DuckDB] 连接已关闭")
        except Exception as e:
            print(f"[DuckDB] 关闭错误: {e}")

    def _parse_time(self, time_str):
        """解析时间字符串为 TIMESTAMP"""
        try:
            if len(time_str) == 5:  # HH:MM
                today = datetime.now().strftime("%Y-%m-%d")
                return f"{today} {time_str}:00"
            elif len(time_str) == 8:  # HH:MM:SS
                today = datetime.now().strftime("%Y-%m-%d")
                return f"{today} {time_str}"
            else:
                return datetime.now()
        except:
            return datetime.now()


# ============ 外部系统读数据接口 ============
class DataReader:
    """给外部系统用的只读接口

    使用方式:
        reader = DataReader("./data/ctp_data.duckdb")

        # 查询最近100条Tick
        ticks = reader.get_recent_ticks("rb2510", 100)

        # 查询某时间段K线
        klines = reader.get_klines_range("rb2510", "1m", "2025-06-30 09:00:00", "2025-06-30 15:00:00")

        # 自定义SQL查询
        result = reader.query("SELECT instrument, SUM(volume) FROM ticks GROUP BY instrument")
    """

    def __init__(self, db_path="./data/ctp_data.duckdb"):
        import duckdb
        self.conn = duckdb.connect(db_path, read_only=True)

    def get_recent_ticks(self, instrument, limit=100):
        """获取最近N条Tick"""
        return self.conn.execute("""
            SELECT * FROM ticks 
            WHERE instrument = ? 
            ORDER BY timestamp DESC 
            LIMIT ?
        """, [instrument, limit]).fetchdf()

    def get_klines_range(self, instrument, period, start_time, end_time):
        """获取某时间段K线"""
        return self.conn.execute("""
            SELECT * FROM klines 
            WHERE instrument = ? AND period = ? 
            AND timestamp BETWEEN ? AND ?
            ORDER BY timestamp
        """, [instrument, period, start_time, end_time]).fetchdf()

    def get_daily_summary(self, date_str=None):
        """获取每日汇总统计"""
        if date_str is None:
            date_str = datetime.now().strftime("%Y%m%d")
        return self.conn.execute("""
            SELECT 
                instrument,
                COUNT(*) as tick_count,
                MIN(price) as min_price,
                MAX(price) as max_price,
                AVG(price) as avg_price,
                SUM(volume) as total_volume,
                SUM(delta) as net_delta
            FROM ticks 
            WHERE date_str = ?
            GROUP BY instrument
            ORDER BY tick_count DESC
        """, [date_str]).fetchdf()

    def query(self, sql, params=None):
        """执行自定义SQL查询"""
        if params:
            return self.conn.execute(sql, params).fetchdf()
        else:
            return self.conn.execute(sql).fetchdf()

    def close(self):
        self.conn.close()


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


# ============ 交易时间工具（按品种区分夜盘结束时间）============
# 2026年最新交易时间（上期所/能源中心/大商所/郑商所）
# 参考: 上海期货交易所官网 www.shfe.com.cn/services/calenderandholidays/tradinghours/
#       大连商品交易所 www.dce.com.cn
#       郑州商品交易所 www.czce.com.cn

# 日盘统一时间段
DAY_SESSIONS = [("09:00", "10:15"), ("10:30", "11:30"), ("13:30", "15:00")]

# 品种夜盘时间段映射（None 表示无夜盘）
# 格式: "品种代码前缀": ("夜盘开始", "夜盘结束")
# 结束时间跨凌晨的用 "HH:MM" 表示，_time_in_range 会自动处理跨天
INSTRUMENT_NIGHT_SESSIONS = {
    # 上期所 - 有色金属: 21:00-次日01:00
    "cu": ("21:00", "01:00"),   # 铜
    "al": ("21:00", "01:00"),   # 铝
    "zn": ("21:00", "01:00"),   # 锌
    "pb": ("21:00", "01:00"),   # 铅
    "ni": ("21:00", "01:00"),   # 镍
    "sn": ("21:00", "01:00"),   # 锡
    "ss": ("21:00", "01:00"),   # 不锈钢
    "ao": ("21:00", "01:00"),   # 氧化铝
    "bc": ("21:00", "01:00"),   # 国际铜 (能源中心)

    # 上期所 - 贵金属: 21:00-次日02:30
    "au": ("21:00", "02:30"),   # 黄金
    "ag": ("21:00", "02:30"),   # 白银

    # 能源中心 - 原油: 21:00-次日02:30
    "sc": ("21:00", "02:30"),   # 原油

    # 上期所 - 能源化工: 21:00-23:00
    "rb": ("21:00", "23:00"),   # 螺纹钢
    "hc": ("21:00", "23:00"),   # 热轧卷板
    "fu": ("21:00", "23:00"),   # 燃料油
    "bu": ("21:00", "23:00"),   # 沥青
    "ru": ("21:00", "23:00"),   # 天然橡胶
    "sp": ("21:00", "23:00"),   # 纸浆
    "br": ("21:00", "23:00"),   # 丁二烯橡胶
    "nr": ("21:00", "23:00"),   # 20号胶

    # 能源中心 - 其他: 21:00-23:00
    "lu": ("21:00", "23:00"),   # 低硫燃料油

    # 大商所 - 工业品/农产品: 21:00-23:00
    "i":  ("21:00", "23:00"),   # 铁矿石
    "j":  ("21:00", "23:00"),   # 焦炭
    "jm": ("21:00", "23:00"),   # 焦煤
    "p":  ("21:00", "23:00"),   # 棕榈油
    "y":  ("21:00", "23:00"),   # 豆油
    "m":  ("21:00", "23:00"),   # 豆粕
    "rm": ("21:00", "23:00"),   # 菜粕
    "a":  ("21:00", "23:00"),   # 黄大豆1号
    "b":  ("21:00", "23:00"),   # 黄大豆2号
    "c":  ("21:00", "23:00"),   # 玉米
    "cs": ("21:00", "23:00"),   # 玉米淀粉
    "eb": ("21:00", "23:00"),   # 苯乙烯
    "eg": ("21:00", "23:00"),   # 乙二醇
    "l":  ("21:00", "23:00"),   # 聚乙烯
    "pp": ("21:00", "23:00"),   # 聚丙烯
    "pvc":("21:00", "23:00"),   # 聚氯乙烯
    "pg": ("21:00", "23:00"),   # 液化石油气

    # 郑商所 - 夜盘品种: 21:00-23:00
    "cf": ("21:00", "23:00"),   # 棉花
    "sr": ("21:00", "23:00"),   # 白糖
    "ta": ("21:00", "23:00"),   # PTA
    "ma": ("21:00", "23:00"),   # 甲醇
    "fg": ("21:00", "23:00"),   # 玻璃
    "oi": ("21:00", "23:00"),   # 菜籽油
    "sa": ("21:00", "23:00"),   # 纯碱
    "sf": ("21:00", "23:00"),   # 硅铁
    "sm": ("21:00", "23:00"),   # 锰硅
    "ur": ("21:00", "23:00"),   # 尿素
    "cj": ("21:00", "23:00"),   # 红枣
    "ap": ("21:00", "23:00"),   # 苹果
    "pk": ("21:00", "23:00"),   # 花生

    # 无夜盘品种
    "lh": None,                  # 生猪
    "jd": None,                  # 鸡蛋
    "ri": None,                  # 早籼稻
    "lr": None,                  # 晚籼稻
    "jr": None,                  # 粳稻
    "rs": None,                  # 油菜籽
    "pm": None,                  # 普麦
    "wh": None,                  # 强麦
    "cy": None,                  # 棉纱
}


def _time_in_range(time_str, start_str, end_str):
    """判断时间是否在区间内，支持跨凌晨（如 21:00-01:00）"""
    def parse(t):
        h, m = map(int, t.split(":"))
        return h * 60 + m

    t = parse(time_str)
    s = parse(start_str)
    e = parse(end_str)

    if s <= e:  # 不跨天，如 09:00-10:15
        return s <= t <= e
    else:  # 跨凌晨，如 21:00-01:00
        return t >= s or t <= e


def get_instrument_night_session(inst):
    """获取品种的夜盘时间段，返回 (start, end) 或 None"""
    if not inst:
        return None
    # 提取品种代码前缀（去掉年份数字）
    prefix = ""
    for ch in inst.lower():
        if ch.isalpha():
            prefix += ch
        else:
            break

    # 先查精确匹配
    if prefix in INSTRUMENT_NIGHT_SESSIONS:
        return INSTRUMENT_NIGHT_SESSIONS[prefix]

    # 尝试2字母前缀
    if len(prefix) >= 2 and prefix[:2] in INSTRUMENT_NIGHT_SESSIONS:
        return INSTRUMENT_NIGHT_SESSIONS[prefix[:2]]

    # 尝试1字母前缀
    if len(prefix) >= 1 and prefix[0] in INSTRUMENT_NIGHT_SESSIONS:
        return INSTRUMENT_NIGHT_SESSIONS[prefix[0]]

    # 默认无夜盘（安全策略）
    return None


def is_trading_time(inst=None, now=None):
    """
    判断指定品种当前是否为交易时间
    Args:
        inst: 品种代码，如 "au2512", "rb2510"
        now: 可选，指定时间，默认当前时间
    Returns:
        bool: 是否为交易时间
    """
    if now is None:
        now = datetime.now()

    time_str = now.strftime("%H:%M")
    weekday = now.weekday()

    # 周末休市
    if weekday >= 5:
        return False

    # 检查日盘
    for start, end in DAY_SESSIONS:
        if start <= time_str <= end:
            return True

    # 检查夜盘（按品种）
    night_session = get_instrument_night_session(inst)
    if night_session is None:
        return False  # 无夜盘的品种

    night_start, night_end = night_session
    return _time_in_range(time_str, night_start, night_end)


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

        self.db_manager = DuckDBManager()
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

        klines_1m = self.db_manager.load_klines(inst, "1m", limit=200)
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
                self.db_manager.save_kline(inst, "1m", bar)

            with self._lock:
                self.klines[inst]["1m"] = bars
        else:
            print(f"  [{inst}] CTP模式，等待实时推送...")

        for period_name in ["5m", "15m", "1h"]:
            self._aggregate_from_1m(inst, period_name)

        self._calc_macd(inst, "1m")

        hist_ticks = self.db_manager.load_ticks(inst, limit=100)
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
        self.db_manager.cleanup_old_data()

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

        # MACD needs 26 bars for EMA26 + 9 bars for DEA = 35 bars warmup
        # First 34 values are unreliable, pad with None
        warmup = 34
        n = len(bars)
        dif_padded = [None] * max(0, n - len(dif)) + [round(x, 4) for x in dif]
        dea_padded = [None] * max(0, n - len(dea)) + [round(x, 4) for x in dea]
        macd_padded = [None] * max(0, n - len(macd_hist)) + [round(x, 4) for x in macd_hist]

        with self._lock:
            self.macd[inst][period] = {
                "dif": dif_padded,
                "dea": dea_padded,
                "macd": macd_padded,
            }

    def get_live_macd(self, inst, period):
        """计算包含当前未完成bar的实时MACD值"""
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
            "dif": round(dif[-1], 4),
            "dea": round(dea[-1], 4),
            "macd": round(macd_hist[-1], 4),
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

        # [FIX-DATA-1] 非交易时间的数据不存入数据库（但保留在内存供显示）
        is_trading = is_trading_time(inst)

        calc = self.calculators.get(inst)
        if not calc:
            calc = OrderFlowCalculator()
            self.calculators[inst] = calc

        flow = calc.on_tick(tick_data)
        if not flow:
            return

        # [FIX-DB-THREAD] 把 DuckDB 操作放到 lock 内，避免多线程并发写入
        with self._lock:
            self.last_ticks[inst] = flow
            # [FIX-MOCK-ORDERFLOW] Mock 模式下始终保存 orderflow，不检查交易时间
            # CTP 模式下只在交易时间保存，避免非交易时间数据污染
            if self.data_source == "mock" or is_trading:
                self.orderflow[inst].append(flow)
            self.tick_count += 1
            self._access_time[inst] = time.time()
            self._access_time.move_to_end(inst)

            # [FIX-DATA-3] 只有CTP模式且交易时间才写入DuckDB
            # Mock 模式数据不保存到数据库
            if is_trading and self.data_source == "ctp":
                self.db_manager.save_tick(inst, flow)

            # [FIX-DATA-4] Mock 模式下始终更新 K 线，CTP 模式只在交易时间更新
            if self.data_source == "mock" or is_trading:
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
                    # [FIX-DATA-3] 只有CTP模式才保存K线到DuckDB
                    if period_name == "1m" and self.data_source == "ctp":
                        self.db_manager.save_kline(inst, period_name, current)
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
    def get_orderflow(self, inst, n=150):
        with self._lock:
            if inst not in self.orderflow:
                return []
            return list(self.orderflow[inst])[-n:]

    # [FIX] 防御性处理：如果MACD未计算，返回空结构
    def get_macd(self, inst, period):
        with self._lock:
            if inst not in self.macd or period not in self.macd[inst]:
                return {"dif": [], "dea": [], "macd": []}
            macd_data = self.macd[inst][period]
            has_current = bool(self.current_bar.get(inst, {}).get(period))
            bars_count = len(self.klines[inst][period]) if inst in self.klines and period in self.klines[inst] else 0

        # [FIX-MACD-ALIGN] 返回与get_klines完全对齐的MACD
        # get_klines返回: bars[-100:] + current_bar（如果有）
        # MACD只针对已收盘bar计算，当前bar用null占位
        if has_current and bars_count >= 100:
            # get_klines返回99个已收盘 + 1个当前bar
            # MACD: 99个已收盘bar的值 + 1个null（当前bar占位）
            dif = (macd_data["dif"][-99:] + [None]) if len(macd_data["dif"]) >= 99 else macd_data["dif"]
            dea = (macd_data["dea"][-99:] + [None]) if len(macd_data["dea"]) >= 99 else macd_data["dea"]
            macd = (macd_data["macd"][-99:] + [None]) if len(macd_data["macd"]) >= 99 else macd_data["macd"]
        else:
            # 无当前bar或不足100个，直接取最后100个
            dif = macd_data["dif"][-100:] if macd_data["dif"] else []
            dea = macd_data["dea"][-100:] if macd_data["dea"] else []
            macd = macd_data["macd"][-100:] if macd_data["macd"] else []

        return {"dif": dif, "dea": dea, "macd": macd}

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

    def generate_tick(self, inst):
        # [FIX-MOCK-1] Mock模式下始终生成数据，不受交易时间限制
        # 这样非交易时间也能测试前端功能
        # 数据库存储仍受 is_trading_time 控制（在 update_tick 中）

        # [FIX-MOCK-2] 使用last_price作为基准（如果有CTP真实价格），否则用base_price
        last_real_price = store.last_ticks.get(inst, {}).get("price")
        if last_real_price and last_real_price > 0:
            base = last_real_price
        else:
            base = self.prices.get(inst, BASE_PRICES.get(inst, 3000.0))

        # [FIX-MOCK-3] 价格波动
        change = random.gauss(0, base * 0.0005)
        price = round(base + change, 2)
        # [FIX-MOCK-4] 限制价格在真实价格±1%范围内
        price = max(base * 0.99, min(base * 1.01, price))
        self.prices[inst] = price

        # [FIX-MOCK-ORDERFLOW] 生成真实的订单流数据
        # 模拟真实市场：价格有时触及 bid（主动卖）或 ask（主动买）
        spread = max(base * 0.0002, 0.01)  # 最小 spread 0.01
        bid = round(price - spread, 2)
        ask = round(price + spread, 2)

        # 生成成交量（本次 tick 的成交量）
        vol = random.randint(1, 30) if random.random() > 0.2 else 0
        self.volumes[inst] += vol

        # 模拟主动买卖方向
        # 30% 概率主动买（价格 >= ask），30% 主动卖（价格 <= bid），40% MIX
        r = random.random()
        if r < 0.3 and vol > 0:
            # 主动买：价格向上触及 ask
            price = ask
            buy_vol = vol
            sell_vol = 0
            aggressor = "BUY"
        elif r < 0.6 and vol > 0:
            # 主动卖：价格向下触及 bid
            price = bid
            buy_vol = 0
            sell_vol = vol
            aggressor = "SELL"
        else:
            # 混合成交
            buy_vol = int(vol * random.uniform(0.3, 0.7))
            sell_vol = vol - buy_vol
            aggressor = "MIX" if vol > 0 else "MIX"

        # total_vol 应该是累计成交量（模拟 CTP 的 Volume 字段）
        # OrderFlowCalculator 会计算 vol_delta = volume - last_volume
        return {
            "time": datetime.now().strftime("%H:%M:%S"),
            "millisec": datetime.now().microsecond // 1000,
            "instrument": inst,
            "price": price,
            "total_vol": self.volumes[inst],  # 累计成交量（模拟 CTP Volume）
            "buy_vol": buy_vol,       # 本次主动买量
            "sell_vol": sell_vol,     # 本次主动卖量
            "delta": buy_vol - sell_vol,
            "bid": bid,
            "ask": ask,
            "bid_vol": random.randint(50, 500),
            "ask_vol": random.randint(50, 500),
            "aggressor": aggressor,
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
                    # [FIX-MOCK-5] 过滤非交易时间返回的None
                    if tick is not None:
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
                    orderflow = list(store.orderflow.get(main_inst, deque()))[-500:]

                data_source = dsm.current

            # [FIX-OF-4] 计算订单流统计信息
            of_count = len(orderflow)
            of_time_range = ""
            if orderflow and len(orderflow) > 0:
                first_time = orderflow[0].get("time", "--")
                last_time = orderflow[-1].get("time", "--")
                of_time_range = first_time + " ~ " + last_time

            # [FIX-KLINE] 获取所有窗口品种的实时 K 线数据 + 实时MACD
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
                            # [FIX-MACD] 追加实时MACD值
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
                # [FIX-OF-5] 新增订单流统计信息
                "orderflow_stats": {
                    "count": of_count,
                    "time_range": of_time_range,
                    "max_display": 150
                },
                # [FIX-KLINE] 推送实时 K 线更新
                "kline_update": kline_update,
            }

            for inst in active_insts:
                tick = last_ticks_copy.get(inst)
                if tick:
                    data["instruments"][inst] = {
                        "last_price": tick["price"],
                        "volume": tick.get("volume_total", 0),
                        "delta": tick["delta"],
                    }

            # [FIX-OF-6] orderflow 已在上面赋值，无需重复
            # if main_inst and main_inst in active_insts:
            #     data["selected_orderflow"] = orderflow

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

# ============ 外部读取 API（只读，不修改现有功能）============
# 这些接口供外部进程通过 HTTP 读取 DuckDB 数据
# 使用 store.db_manager 的现有连接，无需额外配置

@app.get("/api/read/last")
async def api_read_last(
    table: str = "ticks",
    instrument: str = Query(default="", description="品种代码，如 rb2510"),
    n: int = Query(default=10, ge=1, le=1000, description="读取最后N条"),
    period: str = Query(default="", description="K线周期，如 1m/5m/15m/1h"),
    order_by: str = Query(default="timestamp DESC", description="排序字段"),
):
    """
    读取最后N条记录（通用查询接口）
    示例:
        /api/read/last?table=ticks&instrument=rb2510&n=10
        /api/read/last?table=klines&instrument=rb2510&period=1m&n=20
    """
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
    instrument: str = Query(..., description="品种代码，如 rb2510"),
    period: str = Query(default="1m", description="K线周期: 1m/5m/15m/1h"),
    limit: int = Query(default=200, ge=1, le=5000, description="返回条数"),
    start_time: str = Query(default="", description="开始时间，如 2025-07-01 09:00:00"),
    end_time: str = Query(default="", description="结束时间，如 2025-07-01 15:00:00"),
):
    """
    读取K线数据（专用接口，返回标准格式）
    示例:
        /api/read/klines?instrument=rb2510&period=1m&limit=100
        /api/read/klines?instrument=rb2510&period=5m&start_time=2025-07-01 09:00:00
    """
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
                   open, high, low, close, volume, buy_vol, sell_vol, delta, tick_count
            FROM klines {where_clause} ORDER BY timestamp DESC LIMIT {limit}
        """
        result = store.db_manager.conn.execute(sql, params).fetchall()
        klines = []
        for row in reversed(result):
            klines.append({"time": row[0], "timestamp": row[1], "open": float(row[2]), "high": float(row[3]),
                           "low": float(row[4]), "close": float(row[5]), "volume": int(row[6]),
                           "buy_vol": int(row[7]), "sell_vol": int(row[8]), "delta": int(row[9]), "tick_count": int(row[10])})
        return {"success": True, "instrument": instrument, "period": period, "count": len(klines), "klines": klines}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/read/ticks")
async def api_read_ticks(
    instrument: str = Query(..., description="品种代码，如 rb2510"),
    limit: int = Query(default=100, ge=1, le=5000, description="返回条数"),
    start_time: str = Query(default="", description="开始时间"),
    end_time: str = Query(default="", description="结束时间"),
):
    """
    读取Tick明细
    示例:
        /api/read/ticks?instrument=rb2510&limit=50
    """
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
                   buy_vol, sell_vol, delta, bid, ask, aggressor
            FROM ticks {where_clause} ORDER BY timestamp DESC LIMIT {limit}
        """
        result = store.db_manager.conn.execute(sql, params).fetchall()
        ticks = []
        for row in reversed(result):
            ticks.append({"time": row[0], "instrument": row[1], "price": float(row[2]), "volume": int(row[3]),
                          "buy_vol": int(row[4]), "sell_vol": int(row[5]), "delta": int(row[6]),
                          "bid": float(row[7]), "ask": float(row[8]), "aggressor": row[9]})
        return {"success": True, "instrument": instrument, "count": len(ticks), "ticks": ticks}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/read/summary")
async def api_read_summary(
    date_str: str = Query(default="", description="日期，如 20250701，默认今天"),
):
    """
    读取每日汇总统计
    示例:
        /api/read/summary
        /api/read/summary?date_str=20250701
    """
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
        return {"success": True, "date": date_str, "tick_summary": tick_stats.to_dict(orient='records'),
                "kline_summary": kline_stats.to_dict(orient='records')}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/read/query")
async def api_read_query(
    sql: str = Query(..., description="SQL 查询语句（只读，仅 SELECT）"),
    limit: int = Query(default=1000, ge=1, le=10000, description="最大返回条数"),
):
    """
    执行自定义 SQL 查询（只读，限制为 SELECT）
    示例:
        /api/read/query?sql=SELECT * FROM ticks WHERE instrument='rb2510' LIMIT 10
        /api/read/query?sql=SELECT instrument, COUNT(*) FROM ticks GROUP BY instrument
    """
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
    """
    获取数据库信息（表结构、记录数等）
    示例:
        /api/read/dbinfo
    """
    try:
        tables = store.db_manager.conn.execute("SHOW TABLES").fetchdf()
        tick_count = store.db_manager.conn.execute("SELECT COUNT(*) FROM ticks").fetchone()[0]
        kline_count = store.db_manager.conn.execute("SELECT COUNT(*) FROM klines").fetchone()[0]
        db_size = os.path.getsize(store.db_manager.db_path)
        latest_tick = store.db_manager.conn.execute("SELECT MAX(timestamp) FROM ticks").fetchone()[0]
        return {"success": True, "db_path": store.db_manager.db_path, "db_size_mb": round(db_size / 1024 / 1024, 2),
                "tables": tables.to_dict(orient='records'), "tick_count": tick_count, "kline_count": kline_count,
                "latest_tick_time": str(latest_tick) if latest_tick else None,
                "active_instruments": list(store.active_instruments), "data_source": dsm.current}
    except Exception as e:
        return {"success": False, "error": str(e)}

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
    store.db_manager.force_flush()
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
