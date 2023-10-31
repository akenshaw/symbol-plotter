import websocket, requests, json, asyncio, aiohttp
import datetime, os, glob, zipfile
import pandas as pd
import numpy as np
# %%
fr_dict = {}
def get_fr_from_ws():
    ws = websocket.create_connection("wss://fstream.binance.com/stream?streams=!markPrice@arr")

    # ws.send("your_message")
    message = ws.recv()
    ws.close()
    
    result = json.loads(message)
    if result['stream'] == '!markPrice@arr':
        for i in result['data']:
            symbol = i['s']
            funding_rate = float(i['r']) * 100
            mark_price = i['p']

            fr_dict[symbol] = {
                'funding_rate': round(funding_rate, 4),
                'mark_price': mark_price
            }
    return fr_dict
# %%
async def get_the_last_oi(symbol):
    url = 'https://fapi.binance.com/fapi/v1/openInterest'
    params = {'symbol': symbol}

    data = {}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status == 400:
                    data = await response.json()
                    return data
                response.raise_for_status()
                data = await response.json()
    except aiohttp.ClientError as e:
        print(f"Error: API request failed - {e}")
    except json.JSONDecodeError:
        print("Error: JSON data is not valid or empty")
    return data

async def oi_fr_wrap(symbol, fr_dict):
    oi = await get_the_last_oi(symbol)
    if 'openInterest' not in oi:
        if 'msg' in oi:
            print(f"{symbol}, {oi['msg']}")
        else: 
            print(f"Skipping {symbol}, {oi}")
        return None

    return {
        'symbol': symbol,
        'funding_rate': fr_dict[symbol]['funding_rate'],
        'mark_price': fr_dict[symbol]['mark_price'],
        'open_interest': oi['openInterest']
    }

async def get_the_oi_hist(symbol, timeframe):
    url = 'https://fapi.binance.com/futures/data/openInterestHist'
    end_time = None
    results = []

    now = int(datetime.datetime.now().timestamp() * 1000)
    # Subtract 3 days in milliseconds
    min_time = now - (3 * 24 * 60 * 60 * 1000)
    async with aiohttp.ClientSession() as session:
        while True:
            params = {
                'symbol': symbol,
                'period': timeframe,
                'limit': 500
            }
            if end_time is not None:
                params['endTime'] = end_time
            async with session.get(url, params=params) as response:
                data = await response.json()
            if not data:
                break
            results.append(data)
            
            min_timestamp = min([d['timestamp'] for d in data])
            end_time = min_timestamp - 1
            if not data or min_timestamp < min_time:
                break
          
    df = pd.concat([pd.DataFrame(r, columns=['timestamp', 'symbol', 'sumOpenInterest', 'sumOpenInterestValue']) for r in results])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.rename(columns={"timestamp": "Timestamp"}, inplace=True)
    
    df = df.sort_values(by='Timestamp')
    df[['sumOpenInterestValue']] = df[['sumOpenInterestValue']].astype(float)
    df['sumOpenInterestValue'] = df['sumOpenInterestValue'].round(2)

    df = df.drop(['symbol', 'sumOpenInterest'], axis=1)
    df = df.copy()
    return df

async def get_the_klines(symbol, timeframe):
    url = 'https://fapi.binance.com/fapi/v1/klines'
    params = {
        'symbol': symbol,
        'interval': timeframe, 
        'limit': 1000
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            data = await response.json()

    df = pd.DataFrame(data, columns=['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close Time', 'Quote asset volume', 'Number of trades', 'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore'])
    df[['Quote asset volume', 'Taker buy quote asset volume']] = df[['Quote asset volume', 'Taker buy quote asset volume']].astype(float)

    df = df.assign(taker_buy_volume = df['Taker buy quote asset volume'], taker_sell_volume = df['Quote asset volume'] - df['Taker buy quote asset volume'])
    df = df.drop(['Volume', 'Close Time', 'Taker buy base asset volume', 'Quote asset volume', 'Taker buy quote asset volume', 'Ignore'], axis=1)

    df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='ms')
    df[['Open', 'High', 'Low', 'Close']] = df[['Open', 'High', 'Low', 'Close']].astype(float)

    df = df.assign(cumulative_volume_delta = df['taker_buy_volume'] - df['taker_sell_volume'])
    df['cumulative_volume_delta'] = df['cumulative_volume_delta'].cumsum()
    df = df.sort_values(by='Timestamp')
    
    df = df.copy()
    oi_df = await get_the_oi_hist(symbol, timeframe)
    df = pd.merge_asof(df, oi_df, on='Timestamp')
    return df  

async def get_hist_trades(symbol):
    folder = f'/Users/berkes/new_save/binance-public-data-master/python/data/futures/um/daily/trades/{symbol}'

    start_date = datetime.date.today() - datetime.timedelta(days=3)
    end_date = datetime.date.today() - datetime.timedelta(days=1)
    date_range = pd.date_range(start=start_date, end=end_date)

    # Download the csv files if they don't exist
    for date in date_range:
        date_str = date.strftime("%Y-%m-%d")
        csv_filename = f"{symbol}-trades-{date_str}.csv"
        if not os.path.exists(os.path.join(folder, csv_filename)):
            os.system(f"python3.11 /Users/berkes/new_save/binance-public-data-master/python/download-trade.py -t um -s {symbol} -skip-monthly 1 -startDate {date_str}")
        else:
            print(f"{csv_filename} already exists. Skipping...")
    
    # Unzip the zip files and delete them
    zip_files = glob.glob(os.path.join(folder, '*.zip'))
    for zip_file in zip_files:
        csv_file = zip_file.replace('.zip', '.csv')
        if csv_file not in os.listdir(folder):
            with zipfile.ZipFile(zip_file, 'r') as z:
                z.extractall(folder)
                os.remove(zip_file)
        else:
            print(f"{csv_file} already exists, skipping {zip_file}")

    date_strings = [date.strftime("%Y-%m-%d") for date in date_range]
    csv_files = []
    for date_string in date_strings:
        pattern = os.path.join(folder, f"{symbol}-trades-{date_string}.csv")
        csv_files.extend(glob.glob(pattern))

    df_hist_list = [pd.read_csv(f) for f in csv_files]
    df_hist = pd.concat(df_hist_list)

    df_hist.drop('quote_qty', axis=1, inplace=True)
    df_hist = df_hist.rename(columns={'id': 'tradeId', 'is_buyer_maker': 'side', 'qty': 'quantity'})
    
    df_hist[['price', 'quantity']] = df_hist[['price', 'quantity']].astype(float)
    df_hist['side'] = np.where(df_hist['side'], 'Sell', 'Buy')
    df_hist['time'] = pd.to_datetime(df_hist['time'], unit='ms')

    df_hist = df_hist.sort_values(by='tradeId').reset_index(drop=True)
    return df_hist

async def get_hist_aggtrades(symbol):
    folder = f'/Users/berkes/new_save/binance-public-data-master/python/data/futures/um/daily/aggTrades/{symbol}'
    start_date = datetime.date.today() - datetime.timedelta(days=3)
    end_date = datetime.date.today() - datetime.timedelta(days=1)
    date_range = pd.date_range(start=start_date, end=end_date)

    # Download the csv files if they don't exist
    for date in date_range:
        date_str = date.strftime("%Y-%m-%d")
        csv_filename = f"{symbol}-aggTrades-{date_str}.csv"
        if not os.path.exists(os.path.join(folder, csv_filename)):
            os.system(f"python3.11 /Users/berkes/new_save/binance-public-data-master/python/download-aggTrade.py -t um -s {symbol} -skip-monthly 1 -startDate {date_str}")
        else:
            print(f"{csv_filename} already exists. Skipping...")
    
    # Unzip the zip files and delete them
    zip_files = glob.glob(os.path.join(folder, '*.zip'))
    for zip_file in zip_files:
        csv_file = zip_file.replace('.zip', '.csv')
        if csv_file not in os.listdir(folder):
            with zipfile.ZipFile(zip_file, 'r') as z:
                z.extractall(folder)
                os.remove(zip_file)
        else:
            print(f"{csv_file} already exists, skipping {zip_file}")

    date_strings = [date.strftime("%Y-%m-%d") for date in date_range]
    csv_files = []
    for date_string in date_strings:
        pattern = os.path.join(folder, f"{symbol}-aggTrades-{date_string}.csv")
        csv_files.extend(glob.glob(pattern))

    df_hist_list = [pd.read_csv(f) for f in csv_files]
    df_hist = pd.concat(df_hist_list)

    df_hist = df_hist.rename(columns={'agg_trade_id': 'tradeId', 'is_buyer_maker': 'side', 'transact_time': 'time'})
    
    df_hist[['price', 'quantity']] = df_hist[['price', 'quantity']].astype(float)
    df_hist['side'] = np.where(df_hist['side'], 'Sell', 'Buy')
    df_hist['time'] = pd.to_datetime(df_hist['time'], unit='ms')

    df_hist = df_hist.sort_values(by='tradeId').reset_index(drop=True)
    return df_hist