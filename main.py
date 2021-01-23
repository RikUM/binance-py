import asyncio
import websockets

async def hello():
    url = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    async with websockets.connect(url) as websocket:
        while True:
            data = await websocket.recv()
            print(data)

asyncio.get_event_loop().run_until_complete(hello())