import asyncio
import websockets
import json
from datetime import datetime
from decimal import Decimal
import pprint
import psycopg2
from psycopg2 import Error

""" Abbreviations for trade data:
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


async def handle_data(websocket):
    data = await websocket.recv()
    data = await parse_data(data)
    pp = pprint.PrettyPrinter(indent=4)
    await insert_data(data)
    pp.pprint(data)

async def parse_data(data):
    data = json.loads(data)
    data['E'] = datetime.fromtimestamp(data['E']/1000) # event time
    data['T'] = datetime.fromtimestamp(data['T']/1000) # trade time
    data['p'] = Decimal(data['p']) # price
    data['q'] = Decimal(data['q']) # quantity
    return data

async def insert_data(data):
    # Connect to an existing database
    try:
        connection = psycopg2.connect(user="postgres",
                                password="",
                                host="localhost",
                                port="5432",
                                database="binance")
        cursor = connection.cursor()

        postgres_insert_query = """ INSERT INTO trades 
        (symbol, price, quantity, datetime) 
        VALUES (%s,%s,%s,%s);"""

        record_to_insert = (data['s'], data['p'],data['q'],data['T']) 
        cursor.execute(postgres_insert_query, record_to_insert)
        connection.commit()

    except (Exception, psycopg2.Error) as error :
        if(connection):
            print("Failed to insert record into mobile table", error)

    finally:
        #closing database connection.
        if (connection):
            cursor.close()
            connection.close()                       


async def hello():
    url = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    async with websockets.connect(url) as websocket:
        while True:
            await asyncio.create_task(handle_data(websocket))

asyncio.get_event_loop().run_until_complete(hello())