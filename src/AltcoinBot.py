import os, time, websockets, asyncio, json, mysql.connector
import datetime
from decimal import Decimal

import nest_asyncio
from pytz import timezone
import pandas as pd
from binance import BinanceSocketManager, Client, AsyncClient
from dotenv import load_dotenv
from sqlalchemy import create_engine

pd.set_option('precision', 10)

load_dotenv()

user, password, API_KEY, API_SECRET = os.getenv('USERNAME'), os.getenv('PASSWORD'), os.getenv('API_KEY'), os.getenv(
    'API_SECRET')

conn = mysql.connector.connect(user=user, password=password,
                               host='127.0.0.1', database='crypto_db')
cursor = conn.cursor()
cursor.execute("CREATE DATABASE IF NOT EXISTS altcoin_db")
engine = create_engine('mysql+pymysql://{}:{}@localhost/altcoin_db'.format(user, password))
client: Client = Client(API_KEY, API_SECRET, tld='us')


TIMEZONE = timezone('America/New_York')


def loadTransactions(order, side):
    price = float(order['fills'][0]['price'])
    curr_time = datetime.datetime.now(TIMEZONE)
    quantity = float(order['origQty'])
    dollar_AMT = quantity * price
    order_data = {'Symbol': [order['symbol']],'Time': [curr_time],
                  'Type': [side], 'Price': [price],
                  'Quantity': [quantity], 'DollarAMT': [dollar_AMT]}
    buy_df2 = pd.DataFrame.from_dict(order_data)
    print(buy_df2)
    buy_df2.to_sql('AltCoinTransactions', engine, if_exists='append', index=False)


def get_top_symbol():
    all_pairs = pd.DataFrame(client.get_ticker())
    all_pairs['priceChangePercent'] = all_pairs['priceChangePercent'].astype(float)
    usd_coins = all_pairs[all_pairs.symbol.str.endswith('USDT')]
    non_lev = usd_coins[~((usd_coins.symbol.str.contains('UP')) | (usd_coins.symbol.str.contains('DOWN')))]
    top_symbol = non_lev[non_lev.priceChangePercent == non_lev.priceChangePercent.max()]
    top_symbol = top_symbol.symbol.values[0]
    return top_symbol


def getminutedata(symbol, interval, lookback):
    frame = pd.DataFrame(client.get_historical_klines(symbol,
                                                      interval,
                                                      lookback + 'min ago UTC'))
    frame = frame.iloc[:, :6]
    frame.columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume']
    frame = frame.set_index('Time')
    frame.index = pd.to_datetime(frame.index, unit='ms')
    frame = frame.astype('float64')
    return frame


def createframe(msg):

    df = pd.DataFrame([msg])
    df = df.loc[:, ['s', 'E', 'p']]
    df.columns = ['symbol', 'Time', 'Price']
    df.Price = df.Price.astype('float64')
    df.Time = pd.to_datetime(df.Time, unit='ms')
    return df


def new_order(side, quantity, symbol):
        order = client.create_order(symbol=symbol, side=side, type='MARKET', quantity=quantity)
        loadTransactions(order, side)
        print('sending order')
        print(order)
        return order


async def strategy(client2, bsm, SL=Decimal(0.985), Target=Decimal(1.02), open_position=False):
    try:
        asset = get_top_symbol()

    except:
        time.sleep(61)
        asset = get_top_symbol()
   # buy_amt = float(input('How much {} do you want to buy?: '.format(get_top_symbol())))
    buy_amt = 40.00
    socket = bsm.trade_socket(asset)
    df = getminutedata(asset, '1m', '120')
    qty = float(round((Decimal(buy_amt) / Decimal(str(df.Close.iloc[-1]))),0))
    print('Last Price', Decimal(str(df.Close.iloc[-1])))
    print('Quantity', qty)

    if ((df.Close.pct_change() + 1).cumprod()).iloc[-1] > 1:
        order = new_order('BUY', qty, asset)
        buyprice = Decimal((order['fills'][0]['price']))
        print('Buying {} at'.format(asset), buyprice)
        open_position = True
        async with socket:
            while open_position:
                # await socket.__aenter__()
                msg = await socket.recv()
                if msg:
                    df = createframe(msg)
                    print(df)
                print('-------------------')
                print('Buy Price: ' + str(buyprice))
                print('Target Price: ' + str(round((buyprice * Decimal(Target)),8)))
                print('Stop Loss Price: ' + str(round(buyprice * Decimal(SL), 8)))
                print('-------------------')

                if df.Price.values <= buyprice * SL:
                    order = new_order('SELL', qty, asset)
                    print('-------------------')
                    print('STOP LOSS HIT! Selling now...')
                    print('-------------------')
                    print(order)
                    break
                if df.Price.values >= buyprice * Target:
                    order=new_order('SELL', qty, asset)
                    print('-------------------')
                    print('TARGET PRICE HIT! Selling now...')
                    print('-------------------')
                    print(order)
                    break
        await client2.close_connection()


async def main():
    nest_asyncio.apply()
    asyncclient = await AsyncClient.create()
    bsm = BinanceSocketManager(asyncclient, user_timeout =20)
    while True:
        try:
            await strategy(asyncclient, bsm)
        except KeyboardInterrupt:
            await asyncclient.close_connection()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

