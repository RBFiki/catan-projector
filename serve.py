#!/usr/bin/env python3
"""
Catán Projector — servidor local
  HTTP  :8080  → sirve index.html a los celulares
  WS    :8765  → relay en tiempo real entre tablero y controles remotos

Uso:
  python3 serve.py

Luego abre en el navegador del proyector:
  http://[IP-de-esta-PC]:8080/index.html

Los celulares escanean el QR y se conectan automáticamente.
"""
import asyncio
import json
import os
import socket
import http.server
import socketserver
import threading
from collections import defaultdict

import websockets

ROOT = os.path.dirname(os.path.abspath(__file__))
HTTP_PORT = 9080
WS_PORT   = 9765

rooms: dict = defaultdict(set)  # code → set of websocket connections


def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=ROOT, **kwargs)

    def log_message(self, fmt, *args):
        pass  # silencioso


def run_http():
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("", HTTP_PORT), Handler) as httpd:
        httpd.serve_forever()


async def relay(ws):
    room_code = None
    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except Exception:
                continue

            if msg.get("type") == "join":
                room_code = str(msg.get("code", ""))
                rooms[room_code].add(ws)
                n = len(rooms[room_code])
                print(f"  join  room={room_code}  peers={n}")
                continue

            if room_code is None:
                continue

            peers = [p for p in rooms[room_code] if p is not ws]
            if peers:
                await asyncio.gather(
                    *[p.send(raw) for p in peers],
                    return_exceptions=True
                )
    except Exception:
        pass
    finally:
        if room_code and room_code in rooms:
            rooms[room_code].discard(ws)
            if not rooms[room_code]:
                del rooms[room_code]


async def main():
    ip = get_local_ip()

    threading.Thread(target=run_http, daemon=True).start()

    async with websockets.serve(relay, "0.0.0.0", WS_PORT, ping_interval=20, ping_timeout=60):
        print("=" * 50)
        print("  Catán Projector — servidor local")
        print("=" * 50)
        print(f"  Abre en el proyector/PC:")
        print(f"    http://{ip}:{HTTP_PORT}/index.html")
        print()
        print(f"  Los celulares escanean el QR automáticamente.")
        print(f"  (Deben estar en la misma red wifi)")
        print("=" * 50)
        print()

        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
