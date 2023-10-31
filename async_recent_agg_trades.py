from datetime import datetime, timedelta, timezone
import time, os, glob, json
import traceback, sys, logging, aiohttp, asyncio, pytz
from pathlib import Path
import requests, zipfile

import pandas as pd
import numpy as np
from json import JSONDecodeError
from concurrent.futures import ThreadPoolExecutor, as_completed

from df_aggtrades_integrity import main as integrity

BASE_URL = "https://fapi.binance.com"
ENDPOINT = "/fapi/v1/aggTrades"
api_key = ""

logger = logging.getLogger("aggtrades")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

async def get_used_weight_and_latency():
    BASE_URL = "https://api.binance.com"
    ENDPOINT = "/api/v3/ping"
    
    url = f"{BASE_URL}{ENDPOINT}"
    headers = {"X-MBX-APIKEY": api_key}

    async with aiohttp.ClientSession() as session:
        start_time = time.perf_counter()
        async with session.get(url, headers=headers) as response:
            end_time = time.perf_counter()
            latency = (end_time - start_time) * 1000
            if response.status == 200:
                used_weight = int(response.headers.get("X-MBX-USED-WEIGHT-1M", 0))
                return used_weight, latency
            else:
                raise Exception(f"Failed to fetch data: {response.status}")

async def cooldown(used_weight, task_name):
    if used_weight > 2300:
        logger.warning(f"\ncool down triggered with {used_weight}, {task_name}, sleeping for 15 seconds...")
        await asyncio.sleep(15)
    elif used_weight > 2100:
        logger.warning(f"\ncool down triggered with {used_weight}, {task_name}, sleeping for 8 seconds...")
        await asyncio.sleep(8)
    elif used_weight > 1700:
        logger.info(f"\ncool down triggered with {used_weight}, {task_name}, sleeping for 3 seconds...")
        await asyncio.sleep(3)
    pass

async def get_batch_trades(symbol, limit, fromId=None):    
    url = f"{BASE_URL}{ENDPOINT}"
    headers = {"X-MBX-APIKEY": api_key}
    params = {"symbol": symbol, "limit": limit}
    if fromId is not None:
        params["fromId"] = fromId

    original_limit = limit
    backoff_factor = 1
    successful_responses = 0
    
    async with aiohttp.ClientSession() as session:
        while True:
            async with session.get(url, params=params, headers=headers) as response:               
                
                if response.status == 200:
                    task_name = asyncio.current_task().get_name() 
                    try:             
                        trades = await response.json()                    
                        first_batch_tid = trades[0]["a"]
                        last_batch_tid = trades[-1]["a"]

                        used_weight = int(response.headers.get("X-MBX-USED-WEIGHT-1M", 0))    
                        logger.info(f"{len(trades)} trades, {first_batch_tid}-{last_batch_tid}, {used_weight}, {task_name}")
                    except (json.JSONDecodeError, IndexError) as e:
                        logger.error(f"Error: Failed to process trades: {e}, {trades}, {fromId}")                     
                    await cooldown(used_weight, task_name)
                    
                    backoff_factor = 1
                    successful_responses += 1
                    if successful_responses >= 10:
                        if params["limit"] < original_limit:
                            increased_limit = int(params["limit"] * 1.1)
                            params["limit"] = min(increased_limit, original_limit)
                        successful_responses = 0

                elif response.status == 429:
                    retry_after = int(response.headers['Retry-After'])
                    logger.error(f"\n***Received 429 error, backing off for {retry_after} seconds...***\n")    
                    await asyncio.sleep(retry_after)
                    continue                
                elif response.status == 400:
                    backoff_factor *= 2
                    reduced_limit = int(params["limit"] * (1 - 0.2 * backoff_factor))
                    logger.warning("API call failed with status code 400, retrying with reduced limit of {reduced_limit}")
                    if reduced_limit < 1:
                        logger.info("Limit reduced to less than 1, cannot reduce further")
                        response_text = await response.text()
                        logger.info(response_text)
                        break
                    params["limit"] = reduced_limit
                    continue
                        
                else:
                    response_text = await response.text()
                    logger.error(f"Error request URL: {response.url}")
                    logger.error(f"Error request parameters: {params}")
                    raise Exception(f"Failed to fetch data: {response.status}, {response}, {response_text}")
            return trades, used_weight, params["limit"]

async def get_trades_in_range(symbol, start_from_id, num_trades):
    trades = []
    limit = 900
    remaining_trades = num_trades
    current_from_id = start_from_id
    success_count = 0 

    while remaining_trades > 0:
        batch_size = min(limit, remaining_trades)
        batch_trades, used_weight, limit = await get_batch_trades(symbol, batch_size, current_from_id)
        
        if batch_trades:
            success_count += 1 
            if success_count >= 10 and limit < 1000: 
                limit = min(1000, limit * 2)
                print(f"\nincreasing limit to {limit}")
                success_count = 0 
        else:
            success_count = 0 

        trades.extend(batch_trades)
        remaining_trades -= len(batch_trades)
        current_from_id = batch_trades[0]["a"] - len(batch_trades)
        last_trade_time = batch_trades[-1]["T"]
        
        task_name = asyncio.current_task().get_name() 
        print(f"{task_name}: retrieved {len(batch_trades)} trades from {last_trade_time}, {remaining_trades} remaining trades, with a used weight of {used_weight}")
    return trades

async def fetch_trades(symbol, date, recent_df_exists, start_trade_id):
    tasks = []
    trades, _, _ = await get_batch_trades(symbol, 1)
    most_recent_trade_id = trades[0]["a"]
    
    if not recent_df_exists:
        earliest_trade_id_in_date_range = await find_trade_id_by_date(symbol, date, most_recent_trade_id)
    else:
        earliest_trade_id_in_date_range = start_trade_id

    num_parallel_requests = 4
    num_trades_per_task = (most_recent_trade_id - earliest_trade_id_in_date_range) // num_parallel_requests

    _, latency = await get_used_weight_and_latency()
    print(f"Latency: {latency:.2f} ms, total trades to gather until {date}: {num_trades_per_task * num_parallel_requests}\n")
    
    for i in range(num_parallel_requests):
        fromId = most_recent_trade_id - (i * num_trades_per_task)
        task_name = f"Task {i+1}"
        task = asyncio.ensure_future(get_trades_in_range(symbol, fromId, num_trades_per_task))
        task.set_name(task_name)
        tasks.append(task)

    results = await asyncio.gather(*tasks)
    data = []
    for result in results:
        data.extend(result)

    print(data[0]["T"], data[-1]["T"], len(data), "\n")
    
    #timestamp = date / 1000 
    #date_obj = datetime.fromtimestamp(timestamp)
    #formatted_date = date_obj.strftime("%Y_%m_%d")

    df = pd.DataFrame(data, columns=['a', 'p', 'q', 'f', 'l', 'T', 'm'])
    df = df.rename(columns={'a': 'tradeId', 'p': 'price', 'q': 'quantity', 'T': 'time', 'm': 'side'})
    df[['price', 'quantity']] = df[['price', 'quantity']].astype(float)
    df['side'] = np.where(df['side'], 'Sell', 'Buy')
    df['time'] = pd.to_datetime(df['time'], unit='ms')

    df.drop_duplicates(subset='tradeId', keep='last', inplace=True)
    df_checked = await integrity(symbol, df)
    #df_checked.to_csv(f"/Users/berkes/new_save/recent_trades_df/{symbol}_recent_trades_{formatted_date}.csv", index=False)
    return df_checked

async def get_single_trade(symbol, trade_id):
    trades, _, _ = await get_batch_trades(symbol, 1, trade_id)
    return trades[0]

async def find_trade_id_by_date(symbol, date, most_recent_trade_id):
    lower_bound_id = 0
    upper_bound_id = most_recent_trade_id

    while lower_bound_id <= upper_bound_id:
        mid_id = (lower_bound_id + upper_bound_id) // 2
        current_trade = await get_single_trade(symbol, mid_id)
        current_trade_time = current_trade["T"]

        if current_trade_time > date:
            upper_bound_id = mid_id - 1
        elif current_trade_time < date:
            lower_bound_id = mid_id + 1
        else:
            return current_trade["a"]

    closest_trade = current_trade
    print(f"\nFound trade id {closest_trade['a']} with time {closest_trade['T']}")
    return closest_trade["a"]

async def start_of_day_unix_ms():
    local_tz = timezone(timedelta(hours=3))
    current_time = datetime.now(local_tz)
    
    utc_time = current_time.astimezone(timezone.utc)
    start_of_day = utc_time.replace(hour=0, minute=0, second=0, microsecond=0)

    unix_time_in_s = start_of_day.timestamp()
    return int(unix_time_in_s * 1000)

async def main(symbol=None, from_date=None, recent_df_exists=False, start_trade_id=None):
    if from_date is None:
        from_date = await start_of_day_unix_ms()
    if symbol is None:
        symbol = "BCHUSDT"

    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)

    file_handler = logging.FileHandler(f"/Users/berkes/new_save/logger/{symbol}_recent_aggtrades_{from_date}.log")
    logger.addHandler(file_handler)
    file_handler.setFormatter(formatter)
    
    print(f"Starting recent aggregated trades fetch... -> from {from_date}, with a fromId of {start_trade_id}")
    tasks = [
        asyncio.ensure_future(fetch_trades(symbol, from_date, recent_df_exists, start_trade_id)),
    ]
    results = await asyncio.gather(*tasks)
    return results[0] 

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())