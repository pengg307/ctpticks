#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
test_server.py
==============
极简测试服务器 - 验证数据流

运行: python test_server.py
打开: http://localhost:8081
"""

import json
import time
import random
import threading
from datetime import datetime
from collections import deque

from flask import Flask
from flask_sock import Sock

app = Flask(__name__)
sock = Sock(app)

# 简单数据
class SimpleStore:
    def __init__(self):
        self.ticks = []
        self.lock = threading.Lock()

    def add(self, tick):
        with self.lock:
            self.ticks.append(tick)
            if len(self.ticks) > 100:
                self.ticks.pop(0)

    def get_recent(self, n=10):
        with self.lock:
            return list(self.ticks)[-n:]

store = SimpleStore()

# 模拟产生数据
def generate_data():
    count = 0
    while True:
        time.sleep(1)
        count += 1
        tick = {
            "time": datetime.now().strftime("%H:%M:%S"),
            "price": round(3456 + random.gauss(0, 5), 2),
            "volume": random.randint(10, 100),
            "count": count,
        }
        store.add(tick)
        print(f"产生数据: {tick}")

threading.Thread(target=generate_data, daemon=True).start()

# 客户端管理
clients = []
clients_lock = threading.Lock()

def broadcast():
    while True:
        time.sleep(2)
        data = {
            "type": "update",
            "ticks": store.get_recent(5),
            "total": len(store.ticks),
        }
        msg = json.dumps(data)
        with clients_lock:
            dead = []
            for ws in clients:
                try:
                    ws.send(msg)
                except:
                    dead.append(ws)
            for ws in dead:
                clients.remove(ws)

threading.Thread(target=broadcast, daemon=True).start()

@app.route('/')
def index():
    return """
    <!DOCTYPE html>
    <html>
    <head><title>测试</title></head>
    <body>
        <h1>数据测试</h1>
        <div id="status">连接中...</div>
        <div id="data"></div>
        <script>
            const ws = new WebSocket("ws://" + location.host + "/ws");
            ws.onopen = () => { document.getElementById("status").innerText = "已连接"; };
            ws.onmessage = (e) => {
                const data = JSON.parse(e.data);
                document.getElementById("data").innerHTML = "<pre>" + JSON.stringify(data, null, 2) + "</pre>";
            };
            ws.onclose = () => { document.getElementById("status").innerText = "断开"; };
        </script>
    </body>
    </html>
    """

@sock.route('/ws')
def websocket(ws):
    print("客户端连接")
    with clients_lock:
        clients.append(ws)
    try:
        while True:
            ws.receive()
    except:
        pass
    finally:
        with clients_lock:
            if ws in clients:
                clients.remove(ws)
        print("客户端断开")

if __name__ == '__main__':
    print("启动测试服务器: http://localhost:8081")
    app.run(host='0.0.0.0', port=8081, debug=False)
