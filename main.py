import asyncio
import websockets
import json
from datetime import datetime
from decimal import Decimal
import pprint


async def handle_data(websocket):
    data = await websocket.recv()
    data = await parse_data(data)
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(data)

async def parse_data(data):
    """ Abbreviations:
    "e": "trade",     // Event type
    "E": 123456789,   // Event time
    "s": "BNBBTC",    // Symbol
    "t": 12345,       // Trade ID
    "p": "0.001",     // Price
    "q": "100",       // Quantity
    "b": 88,          // Buyer order ID
    "a": 50,          // Seller order ID
    "T": 123456785,   // Trade time
    "m": true,        // Is the buyer the market maker?
    "M": true         // Ignore
    """
    data = json.loads(data)
    data['E'] = datetime.fromtimestamp(data['E']/1000) # event time
    data['T'] = datetime.fromtimestamp(data['T']/1000) # trade time
    data['p'] = Decimal(data['p']) # price
    data['q'] = Decimal(data['q']) # quantity
    return data



async def hello():
    url = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    async with websockets.connect(url) as websocket:
        while True:
            await asyncio.create_task(handle_data(websocket))

asyncio.get_event_loop().run_until_complete(hello())