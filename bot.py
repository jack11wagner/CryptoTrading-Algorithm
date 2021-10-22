import pandas as pd
import websocket, json, pprint, talib, numpy, pandas, pymysql, os
from dotenv import load_dotenv
import mysql.connector
from sqlalchemy import create_engine
from binance.client import Client
from binance.enums import *

load_dotenv()

user, password, API_KEY, API_SECRET = os.getenv('USERNAME'), os.getenv('PASSWORD'), os.getenv('API_KEY'), os.getenv(
    'API_SECRET')

conn = mysql.connector.connect(user='jacksonw', password='3083Creek',
                               host='127.0.0.1', database='crypto_db')
cursor = conn.cursor()
engine = create_engine('mysql+pymysql://{}:{}@localhost/crypto_db'.format('jacksonw', '3083Creek'))

client = Client(API_KEY, API_SECRET, tld='us')

RSI_PERIOD = 14
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30
TRADE_SYMBOL = 'BTCUSD'
TRADE_QUANTITY = 0.000384


def order(side, quantity, symbol, order_type=ORDER_TYPE_MARKET):
    try:
        order = client.create_order(symbol=symbol, side=side, type=ORDER_TYPE_MARKET, quantity=quantity)
        order_data = {'Time': [pd.to_datetime(order['transactTime'], unit='ms')],
                      'Type': [side], 'Price': [float(order['fills'][0]['price'])],
                      'Quantity': [float(order['origQty'])]}
        buy_df2 = pd.DataFrame.from_dict(order_data)
        buy_df2.to_sql('Transactions', engine, if_exists='append', index=False)
        print('sending order')
        print(order)

    except Exception as e:
        return False

    return True


def createFrame(symbol, Time, openprice, closeprice, change):
    df = pd.DataFrame({'symbol': [symbol], 'Time': [Time], 'Open_Price': [openprice], 'Close_Price': [closeprice],
                       'Change_in_Price': change})

    df.Time = pd.to_datetime(df.Time, unit='ms')
    return df


def RSI_Frame_To_SQL(Symbol, Time, RSI):
    df = pd.DataFrame({'symbol': [Symbol], 'Time': [pd.to_datetime(Time, unit='ms')], 'LastRSI': [RSI]})
    df.to_sql('RSI', engine, if_exists='append', index=False)


SOCKET = 'wss://stream.binance.com:9443/ws/btcusdt@kline_1m'


def on_open(ws):
    print('opened connection')


def on_close(ws):
    print('closed connection')


close_data = pd.read_sql('CandleCloses', engine)
closes = list(close_data['Close_Price'])
print(closes)
last_position = pd.read_sql('Transactions', engine)

if last_position['Type'].iloc[-1] == 'BUY':
    in_position = True
    print('In current position at $', last_position['Price'].iloc[-1], ' with quantity of ', last_position['Quantity'].iloc[-1])
else:
    in_position = False
    print('Not currently in a position')




def on_message(ws, message):
    global closes
    global in_position
    # print('received message')
    json_message = json.loads(message)
    # pprint.pprint(json_message)
    candle = json_message['k']
    # If True this means the candle is the last in the series aka last in minute interval
    is_candle_closed = candle['x']

    symbol, time, open, close = json_message['s'], candle['T'], candle['o'], candle['c']
    change = float(close) - float(open)

    if is_candle_closed:
        print('candle closed at {}'.format(close))

        closes.append(float(close))
        frame = createFrame(symbol, time, float(open), float(close), float(change))
        print(frame)
        frame.to_sql('CandleCloses', engine, if_exists='append', index=False)
        print('closes')
        print(closes)

        # makes sure number of closes recorded is greater than 14
        if len(closes) > RSI_PERIOD:
            np_closes = numpy.array(closes)
            rsi = talib.RSI(np_closes, RSI_PERIOD)
            print("all rsis calculated so far")
            print(rsi)
            last_rsi = rsi[-1]
            RSI_Frame_To_SQL(symbol, time, last_rsi)

            print('the current rsi is {}'.format(last_rsi))

            if last_rsi > RSI_OVERBOUGHT:
                if in_position:
                    print('Overbought! SELL! SELL! SELL')
                    order_succeeded = order(SIDE_SELL, TRADE_QUANTITY, TRADE_SYMBOL)
                    if order_succeeded:
                        in_position = False
                else:
                    print('It is overbought but we dont own any. Nothing to do.')

            if last_rsi < RSI_OVERSOLD:
                if in_position:
                    print('It is oversold, but you own it, Nothing to do.')

                else:
                    print("Oversold! BUY! BUY BUY!")
                    order_succeeded = order(SIDE_BUY, TRADE_QUANTITY, TRADE_SYMBOL)
                    if order_succeeded:
                        in_position = True


ws = websocket.WebSocketApp(SOCKET, on_open=on_open, on_close=on_close, on_message=on_message)
ws.run_forever()
