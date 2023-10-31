import pandas as pd
import numpy as np
import requests, time, traceback, asyncio, aiohttp
# %%
api_key = ""
# %%
async def missing_integrity_check(df):
    df = df.drop_duplicates(subset='tradeId', keep='last')
    #df['time'] = pd.to_datetime(df['time'])    

    expected = np.arange(df['tradeId'].min(), df['tradeId'].max() + 1)
    actual = df['tradeId'].to_numpy()
    missing = np.setdiff1d(expected, actual)

    if len(missing) == 0: 
        print("There are no missing tradeIds")
    else:
        print(len(missing), "missing tradeId")

        gaps = np.split(missing, np.where(np.diff(missing) != 1)[0] + 1)
        gap_lengths = [len(gap) for gap in gaps]
        for gap in gaps:
            if len(gap) > 1:
                try:
                    first = gap[0]
                    last = gap[-1]
                except IndexError:
                    continue

                #prev = df[df['tradeId'] < first]['tradeId'].max()
                #next = df[df['tradeId'] > last]['tradeId'].min()

                #print(f"The missing tradeIds are between {first} and {last}.")
                #print(f"Their time window is between {df[df['tradeId'] == prev]['time'].iloc[0]} and {df[df['tradeId'] == next]['time'].iloc[0]}.")          
                #print(df[df['tradeId'].isin([prev, next])])
        return gaps, gap_lengths
# %%
async def trades_gaps_filler(symbol, prev_trade_id, next_trade_id, original_df):
    headers = {"X-MBX-APIKEY": api_key}
    BASE_URL = "https://fapi.binance.com"
    ENDPOINT = "/fapi/v1/aggTrades"
    url = f"{BASE_URL}{ENDPOINT}"

    max_limit = 1000
    last_trade_id = prev_trade_id
    data = []
    async with aiohttp.ClientSession() as session:
        while True:
            trade_diff = next_trade_id - last_trade_id
            if trade_diff <= 0:
                break
            limit = min(trade_diff, max_limit)
            params = {
                "symbol": symbol,
                "limit": int(limit),
                "fromId": int(last_trade_id) 
            }
            try:
                async with session.get(url, params=params, headers=headers) as response:
                    #print(f"\n{prev_trade_id}, {next_trade_id}, {limit}")
                    print(f"Requesting {limit} trades from {last_trade_id}...")
                    if response.status == 200:
                        trades = await response.json()
                        print(f"Retrieved {len(trades)} trades.")

                        if len(trades) == 0:
                            print(f"While retrieving trade id {last_trade_id}, requesting {limit} trades, got empty trades list with {len(trades)} items")
                            break
                        if trades[0]['a'] >= next_trade_id:
                            print(f"Before filtering: {trades}")
                            trades = [trade for trade in trades if trade['a'] < next_trade_id]
                            print(f"After filtering: {trades}")
                            data.extend(trades)
                            print(f"Condition met, breaking the loop after retrieving {len(trades)} trades.")
                            break

                        data.extend(trades)
                        last_trade_id = trades[0]['a'] + len(trades)
                        await asyncio.sleep(0.1)
                        if int(response.headers["X-MBX-USED-WEIGHT-1M"]) > 1000:
                            await asyncio.sleep(2)
                    else:
                        print(f"Requesting {limit} trades from {last_trade_id} failed with status code {response.status}")
                        print(await response.text())
                        break
            except Exception as e:
                error_message = traceback.format_exc()
                print(f"Exception occurred: {e}\n{error_message}")
                break

    df = pd.DataFrame(data, columns=['a', 'p', 'q', 'f', 'l', 'T', 'm'])
    df = df.rename(columns={'a': 'tradeId', 'p': 'price', 'q': 'quantity', 'T': 'time', 'm': 'side'})
    df[['price', 'quantity']] = df[['price', 'quantity']].astype(float)
    df['side'] = np.where(df['side'], 'Sell', 'Buy')
    df['time'] = pd.to_datetime(df['time'], unit='ms')

    combined_df = pd.concat([original_df, df]).sort_values(by='tradeId').reset_index(drop=True)
    combined_df.drop_duplicates(subset='tradeId', keep='last', inplace=True)
    print(f"Combined df has now {len(combined_df)} trades.\n")
    return combined_df
# %%
async def main(symbol, df):
    print(f"Total aggregated trades before the integrity check is {len(df)}. Continuing for missing aggregated trades in {symbol}...")
    while True:
        try:
            gaps, gap_lengths = await missing_integrity_check(df)
            filled_trades = pd.DataFrame() 
            if not any(gap_length > 1 for gap_length in gap_lengths):
                break

            for i, gap in enumerate(gaps):
                if gap_lengths[i] > 1:
                    prev_trade_id = gap[0] - 1
                    next_trade_id = gap[-1] + 1

                    print(f"**Gap {i+1} is between {prev_trade_id} and {next_trade_id}**")
                    filled_trades = await trades_gaps_filler(symbol, prev_trade_id, next_trade_id, df)
            df = filled_trades 
        except Exception as e:
            print(f"An error occurred while filling gaps: {e}")
            break
    await missing_integrity_check(df)
    
    df.drop_duplicates(subset='tradeId', keep='last', inplace=True)
    df = df.sort_values(by='tradeId').reset_index(drop=True)

    print(f"Total aggregated trades after the integrity check is {len(df)}\n")
    return df
# %%
if __name__ == "__main__":
    symbol = "XRPUSDT"
    date = "06-17"
    df = pd.read_csv(f"/Users/berkes/new_save/recent_trades_df/{symbol}_recent_trades_{date}.csv")
    
    new_df = asyncio.run(main(symbol, df)) 
    new_df.to_csv(f"/Users/berkes/new_save/recent_trades_df/{symbol}_recent_trades_checked_{date}.csv", index=False)
# %%