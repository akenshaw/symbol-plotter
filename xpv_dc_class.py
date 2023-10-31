# %%
import os, datetime, requests, time, ccxt, glob, math, asyncio
import plotly.graph_objects as go

import pandas as pd
import numpy as np 
from decimal import Decimal
from json import JSONDecodeError
from multiprocessing import Pool
from functools import partial

import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import seaborn as sns
from plotly.subplots import make_subplots 

from async_recent_trades import main as fetch_trades_main
from async_recent_agg_trades import main as fetch_aggtrades_main
from df_aggtrades_integrity import main as aggtrades_integrity
from df_trades_integrity import main as trades_integrity
import nance_utils
# %%
now = datetime.datetime.now()
start_time = datetime.datetime(now.year, now.month, now.day)
formatted_date = start_time.strftime('%m-%d')

sns.set_palette("colorblind") 
plt.style.use("seaborn-white")
# %% 
class SymbolHandler:
    def __init__(self, symbol, formatted_date, funding_rate):
        self.symbol = symbol
        self.formatted_date = formatted_date
        self.funding_rate = funding_rate
        self.kline_df = None
        self.trades_df = None
        self.recent_trades_df = None
        self.v_trades_df = None

    async def update_recent_trades(self, fetch_trades_main):
        csv_filename = f"/Users/berkes/new_save/recent_trades_df/{self.symbol}_recent_trades_{self.formatted_date}.csv"
        recent_df_exists = False
        
        if os.path.isfile(csv_filename):
            print(f"\nUpdating {csv_filename}!\n")
            recent_df_exists = True

            df_prev = pd.read_csv(csv_filename)
            start_trade_id = int(df_prev['tradeId'].max())
            try:
                df_recent = await fetch_trades_main(self.symbol, None, recent_df_exists, start_trade_id)              
            except Exception as e:
                print(f"Error while processing symbol {self.symbol}: {e}")
                return
            
            df_updated = pd.concat([df_prev, df_recent], ignore_index=True)
            df_updated = df_updated.drop_duplicates(subset ='tradeId', keep='last')

            df_updated.to_csv(csv_filename, index=False)
            self.recent_trades_df = df_updated
            
            #for file in glob.glob(os.path.join("/Users/berkes/new_save/recent_trades_df/", '*.csv')):
            #    file_date = file.split('_')[-1].replace('.csv', '')
            #    if file_date != formatted_date:
            #        os.remove(file)
            #        print(f"Removed {file}")
        else:
            print(f"\nCSV file doesn't exist. Creating new csv for {self.symbol}...\n")
            start_trade_id = None
            try:
                self.recent_trades_df = await fetch_trades_main(self.symbol, None, recent_df_exists, start_trade_id)
                self.recent_trades_df.to_csv(csv_filename, index=False)
            except Exception as e:
                print(f"Error while processing symbol {self.symbol}, {e}")         
                return
    
    async def update_recent_aggtrades(self, fetch_aggtrades_main):
        csv_filename = f"/Users/berkes/new_save/recent_aggtrades_df/{self.symbol}_recent_aggtrades_{self.formatted_date}.csv"
        recent_df_exists = False
        
        if os.path.isfile(csv_filename):
            print(f"\nUpdating {csv_filename}!\n")
            recent_df_exists = True

            df_prev = pd.read_csv(csv_filename)
            start_trade_id = int(df_prev['tradeId'].max())
            try:
                df_recent = await fetch_aggtrades_main(self.symbol, None, recent_df_exists, start_trade_id)              
            except Exception as e:
                print(f"Error while processing symbol {self.symbol}: {e}")
                return
            
            df_updated = pd.concat([df_prev, df_recent], ignore_index=True)
            df_updated = df_updated.drop_duplicates(subset ='tradeId', keep='last')

            df_updated.to_csv(csv_filename, index=False)
            self.recent_trades_df = df_updated
            
            #for file in glob.glob(os.path.join("/Users/berkes/new_save/recent_aggtrades_df/", '*.csv')):
            #    file_date = file.split('_')[-1].replace('.csv', '')
            #    if file_date != formatted_date:
            #        os.remove(file)
            #        print(f"Removed {file}")
        else:
            print(f"\nCSV file doesn't exist. Creating new csv for {self.symbol}...\n")
            start_trade_id = None
            try:
                self.recent_trades_df = await fetch_aggtrades_main(self.symbol, None, recent_df_exists, start_trade_id)
                self.recent_trades_df.to_csv(csv_filename, index=False)
            except Exception as e:
                print(f"Error while processing symbol {self.symbol}, {e}")         
                return

    async def integrity_check(self, is_aggregated):
        print(f"\n***Merging dataframes... before df lengths: {len(self.recent_trades_df)},  {len(self.trades_df)}")
        df = pd.concat([self.recent_trades_df, self.trades_df], ignore_index=True).sort_values(by='tradeId').reset_index(drop=True)
        df = df.drop_duplicates(subset ='tradeId', keep='last')
        
        print(f"\n***Merged dataframe with a total length of {len(df)}. Checking integrity now...")
        if is_aggregated:
            self.v_trades_df = await aggtrades_integrity(self.symbol, df)
        else:
            self.v_trades_df = await trades_integrity(self.symbol, df)
        print(f"\n***Integrity check finished. Total length of dataframe now: {len(self.v_trades_df)}\n continuing for plotting...")  
     
class SymbolPlotter:
    def __init__(self, symbol_handler):
        self.symbol_handler = symbol_handler
    
    def aggregate_levels(self, df, agg_level):
        df = df.copy()
        df['price'] = (df['price'] / agg_level).round() * agg_level
        df_agg = df.groupby(['price', 'side'])['quantity'].sum().reset_index()
        df_agg = df_agg.pivot(index='price', columns='side', values='quantity')

        df_agg = df_agg.rename(columns={'Buy': 'buy_quantity', 'Sell': 'sell_quantity'})
        df_agg = df_agg.reset_index()
        return df_agg

    def chart_it(self):
        kline_df = self.symbol_handler.kline_df.copy()

        fig = make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.01, row_heights=[0.7, 0.25, 0.25], specs=[[{}], [{}], [{'secondary_y': True}]])
        fig.add_trace(go.Candlestick(x=kline_df['Timestamp'], open=kline_df['Open'], high=kline_df['High'], low=kline_df['Low'], close=kline_df['Close'], showlegend=False), row=1, col=1)

        fig.add_trace(go.Bar(x=kline_df['Timestamp'], y=kline_df['taker_sell_volume'], base=0, marker_color='red', showlegend=False, offsetgroup=1, opacity=0.5), row=2, col=1) 
        fig.add_trace(go.Bar(x=kline_df['Timestamp'], y=kline_df['taker_buy_volume'], base=0, marker_color='green', showlegend=False, offsetgroup=1, opacity=0.5), row=2, col=1)

        fig.add_trace(go.Scatter(x=kline_df['Timestamp'], y=kline_df['cumulative_volume_delta'], line=dict(color='red', width=2, dash='dot'), name='CVD'), row=3, col=1, secondary_y=True)    
        fig.add_trace(go.Scatter(x=kline_df['Timestamp'], y=kline_df['sumOpenInterestValue'], line=dict(color='black', width=2), name='OI'), row=3, col=1)
        
        fig.update_layout(
            legend=dict(
                x=0.5,
                xanchor='center',
                yanchor='top',
                orientation='h',
                traceorder="normal",
                font=dict(family="sans-serif", size=12, color="black"),
                bgcolor="White",
                bordercolor="Black",
                borderwidth=1
            )
        )
        fig.update_layout(barmode='stack')
        fig.update_layout(xaxis_rangeslider_visible=False)
        fig.update_layout(title=f"{self.symbol_handler.symbol}, {self.symbol_handler.funding_rate}")

        fig.update_xaxes(showgrid=False)
        fig.update_yaxes(showgrid=False)
        fig.update_layout(autosize=True, width=2560, height=1440)                                                                                                                                                                                                      
        #fig.show()
        png_filename = f"/Users/berkes/new_save/plots/{self.symbol_handler.symbol}_{self.symbol_handler.formatted_date}_klines.png"
        fig.write_image(png_filename)
        return png_filename

    def trades_plot(self, is_aggregated=False): 
        ### Prepare data for plotting ###
        #df = pd.concat([self.symbol_handler.trades_df, self.symbol_handler.recent_trades_df], axis=0)
        df = self.symbol_handler.v_trades_df

        #df = df.drop_duplicates(subset='tradeId', keep='last')
        df['time'] = pd.to_datetime(df['time'])

        median_price = df['price'].median()
        tick_ranges = {(0.0001, 0.001): (0.001, 7),
                    (0.001, 0.01): (0.001, 6),
                    (0.01, 0.1): (0.001, 5),
                    (0.1, 1): (0.001, 4),
                    (1, 10): (0.001, 3),
                    (10, 100): (0.001, 2),
                    (100, 1000): (0.001, 1),
                    (1000, 10000): (0.001, 0),
                    (10000, float('inf')): (0.001, -1)}
        for r in tick_ranges:
            if r[0] < median_price <= r[1]:
                tick = round(median_price * tick_ranges[r][0], tick_ranges[r][1])
                break

        max_quantity = df['quantity'].max()   
        filtered_df = df[df['quantity'] > (max_quantity/10)]  
        try:
            while len(filtered_df) < 95 or len(filtered_df) > 205:
                if len(filtered_df) < 95:
                    max_quantity *= 0.9 # lower the threshold by 10%
                else:
                    max_quantity *= 1.1 # raise the threshold by 10%
                filtered_df = df[df['quantity'] > (max_quantity/10)]
        except Exception as e:
            print("An error occurred:", e, "using default threshold.")

        decimal_places = round(-math.log10(tick))
        df_agg = self.aggregate_levels(df.copy(), tick)

        df_agg['price'] = df_agg['price'].apply(Decimal)
        df_agg['price'] = df_agg['price'].apply(lambda x: x.quantize(Decimal(f'0.{"0" * decimal_places}')))
        df_agg['price'] = df_agg['price'].astype(float)

        ### Plotting ###
        dpi = 100  
        width = 2560
        height = 1440
        new_figsize = (width / dpi, height / dpi)
        plt.figure(figsize=new_figsize)
        
        ax1 = plt.gca()
        ax1.set_xlabel('Time')
        ax1.set_ylabel('Quantity', color='green')

        ax2 = ax1.twiny()
        ax3 = ax2.twinx()

        ax4 = ax3.twinx()
        ax4.set_ylabel('CVD', color='blue')

        colormap = {'Buy': 'green', 'Sell': 'red'}
        colors = filtered_df['side'].map(colormap)

        #log_scaling_factor = 3
        #scaled_quantities = filtered_df['quantity'] / (tick * log_scaling_factor)
        #scaled_sizes = np.log(scaled_quantities + 1) * log_scaling_factor

        # Min-max scaling
        min_quantity = filtered_df['quantity'].min()
        max_quantity = filtered_df['quantity'].max()
        scaled_sizes = (filtered_df['quantity'] - min_quantity) / (max_quantity - min_quantity)

        # Scale the sizes to a desired range, e.g., 10 to 100
        min_size = 10
        max_size = 100
        scaled_sizes = min_size + (scaled_sizes * (max_size - min_size))

        ax3.plot(self.symbol_handler.kline_df['Timestamp'], self.symbol_handler.kline_df['Close'], color='gray', linestyle='--')
        ax3.scatter(filtered_df['time'], filtered_df['price'], s=scaled_sizes, c=colors, alpha=0.75)
        ax3.get_yaxis().set_visible(False)

        df_agg.plot.barh(x='price', y=['buy_quantity', 'sell_quantity'], ax=ax1, alpha=0.5)
        ax1.tick_params(axis='y', labelcolor='green')
        ax1.yaxis.set_major_locator(MaxNLocator(integer=True, prune='both', nbins=20))

        ax4.plot(self.symbol_handler.kline_df['Timestamp'], self.symbol_handler.kline_df['cumulative_volume_delta'], color='blue', alpha=0.25, linestyle=':', marker='s', markersize=1)
        ax4.tick_params(axis='y', labelcolor='blue')
        ax1.get_legend().remove()
        
        if is_aggregated:
            plt.title(f"{self.symbol_handler.symbol}, {df['time'].min().strftime('%Y-%m-%d %H:%M:%S')} - {df['time'].max().strftime('%Y-%m-%d %H:%M:%S')}\n {round(filtered_df['quantity'].min()*median_price)} - {round(filtered_df['quantity'].max()*median_price)} USD, {len(filtered_df)} aggregated trades", fontsize=11)
            png_filename = f"/Users/berkes/new_save/plots/{self.symbol_handler.symbol}_{self.symbol_handler.formatted_date}_aggtrades.png"
        else:
            plt.title(f"{self.symbol_handler.symbol}, {df['time'].min().strftime('%Y-%m-%d %H:%M:%S')} - {df['time'].max().strftime('%Y-%m-%d %H:%M:%S')}\n {round(filtered_df['quantity'].min()*median_price)} - {round(filtered_df['quantity'].max()*median_price)} USD, {len(filtered_df)} trades", fontsize=11)
            png_filename = f"/Users/berkes/new_save/plots/{self.symbol_handler.symbol}_{self.symbol_handler.formatted_date}_trades.png"
        plt.savefig(png_filename, dpi=dpi)
        plt.close()   
        return png_filename    
# %%
def filter_symbols(unwanted_symbols, sorted_data, n):
    filtered_data = [d for d in sorted_data if d['symbol'] not in unwanted_symbols]
    top_n_data = filtered_data[:n]
    if len(filtered_data) < n:
        last_index = sorted_data.index(filtered_data[-1])
        top_n_data.append(sorted_data[last_index + 1])
    return top_n_data

class MainHandler:
    def __init__(self):
        self.lock = False

    async def main_custom(self, symbols, is_aggregated):
        if self.lock:
            return
        self.lock = True

        file_names_list = []
        data_dict = {}
        fr_dict = nance_utils.get_fr_from_ws()

        for symbol in symbols:
            data_dict[symbol] = await nance_utils.oi_fr_wrap(symbol, fr_dict)
            
            handler = SymbolHandler(symbol, formatted_date, data_dict[symbol]['funding_rate'])     
            handler.kline_df = await nance_utils.get_the_klines(symbol, "5m")
            if is_aggregated:
                handler.trades_df = await nance_utils.get_hist_aggtrades(symbol)
            else:
                handler.trades_df = await nance_utils.get_hist_trades(symbol)

            try:
                if is_aggregated:
                    await handler.update_recent_aggtrades(fetch_aggtrades_main)
                else:
                    await handler.update_recent_trades(fetch_trades_main)
            except Exception as e:
                print(f"Error while updating recent trades of symbol {symbol}: {e}")
                continue
            await handler.integrity_check(is_aggregated)
            
            plotter = SymbolPlotter(handler)
            file_names_list.append(plotter.trades_plot(is_aggregated))
            file_names_list.append(plotter.chart_it())
        print(file_names_list)

        self.lock = False
        return file_names_list

    async def main(self):
        if self.lock:
            return
        self.lock = True
        is_aggregated = True

        file_names_list = []
        data_dict = {}
        fr_dict = nance_utils.get_fr_from_ws()
        symbols = [symbol for symbol in fr_dict if symbol.endswith('USDT')]

        tasks = [nance_utils.oi_fr_wrap(symbol, fr_dict=fr_dict) for symbol in symbols]
        results = await asyncio.gather(*tasks)
        for result in results:
            if result is not None:
                symbol = result['symbol']
                data_dict[symbol] = {
                    'symbol': symbol,
                    'funding_rate': float(result['funding_rate']),
                    'mark_price': float(result['mark_price']),
                    'open_interest': float(result['open_interest'])*float(result['mark_price'])
                }
                #print(f"\n{symbol}, {data_dict[symbol]}")

        sorted_oi = sorted(data_dict.values(), key=lambda x: x['open_interest'], reverse=True)
        sorted_fr = sorted(data_dict.values(), key=lambda x: x['funding_rate'], reverse=False)
        unwanted_symbols = ["BTCUSDT"]

        initial_n = 25
        n = initial_n
        common_symbols = []
        iterations = 0

        while len(common_symbols) < 5:
            top_n_oi = filter_symbols(unwanted_symbols, sorted_oi, n)
            top_n_fr = filter_symbols(unwanted_symbols, sorted_fr, n)

            common_symbols = [d['symbol'] for d in top_n_oi if d['symbol'] in [x['symbol'] for x in top_n_fr]]
            n += 1
            iterations += 1
        confidence_interval = 1 - (iterations / initial_n)

        print(f"\nConfidence Interval: {confidence_interval * 100:.2f}%")
        print("Common Symbols:")

        symbol_to_funding_rate = {entry['symbol']: entry['funding_rate'] for entry in top_n_fr}
        for index, symbol in enumerate(common_symbols, start=1):
            funding_rate = symbol_to_funding_rate[symbol]
            print(f"{index}. {symbol}, {funding_rate}%")

        for i in common_symbols:    
            i = i.replace('/', '').replace(':USDT', '')
            funding_rate = symbol_to_funding_rate[i] 

            handler = SymbolHandler(i, formatted_date, funding_rate)         
            handler.kline_df = await nance_utils.get_the_klines(i, "5m")
            if is_aggregated:
                handler.trades_df = await nance_utils.get_hist_aggtrades(i)
            else:
                handler.trades_df = await nance_utils.get_hist_trades(i)

            try:
                if is_aggregated:
                    await handler.update_recent_aggtrades(fetch_aggtrades_main)
                else:
                    await handler.update_recent_trades(fetch_trades_main)
            except Exception as e:
                print(f"Error while processing symbol {i}: {e}")
                continue
            await handler.integrity_check(is_aggregated)
            
            plotter = SymbolPlotter(handler)
            file_names_list.append(plotter.trades_plot(is_aggregated))
            file_names_list.append(plotter.chart_it())
        print(file_names_list)
        
        self.lock = False
        return file_names_list

""""
if __name__ == '__main__':
    data_dict = {}
    fr_dict = nance_utils.get_fr_from_ws()
    symbols = [symbol for symbol in fr_dict if symbol.endswith('USDT')]

    oi_fr_wrap_partial = partial(nance_utils.oi_fr_wrap, fr_dict=fr_dict)
    with Pool() as p:
        results = p.map(oi_fr_wrap_partial, symbols)

    for result in results:
        if result is not None:
            symbol = result['symbol']
            data_dict[symbol] = {
                'symbol': symbol,
                'funding_rate': float(result['funding_rate']),
                'mark_price': float(result['mark_price']),
                'open_interest': float(result['open_interest'])*float(result['mark_price'])
            }
            #print(f"\n{symbol}, {data_dict[symbol]}")

    sorted_oi = sorted(data_dict.values(), key=lambda x: x['open_interest'], reverse=True)
    sorted_fr = sorted(data_dict.values(), key=lambda x: x['funding_rate'], reverse=False)
    unwanted_symbols = ["BTCUSDT", "ETHUSDT"]

    initial_n = 25
    n = initial_n
    common_symbols = []
    iterations = 0

    while len(common_symbols) < 5:
        top_n_oi = filter_symbols(unwanted_symbols, sorted_oi, n)
        top_n_fr = filter_symbols(unwanted_symbols, sorted_fr, n)

        common_symbols = [d['symbol'] for d in top_n_oi if d['symbol'] in [x['symbol'] for x in top_n_fr]]
        n += 1
        iterations += 1
    confidence_interval = 1 - (iterations / initial_n)

    print(f"\nConfidence Interval: {confidence_interval * 100:.2f}%")
    print("Common Symbols:")

    symbol_to_funding_rate = {entry['symbol']: entry['funding_rate'] for entry in top_n_fr}
    for index, symbol in enumerate(common_symbols, start=1):
        funding_rate = symbol_to_funding_rate[symbol]
        print(f"{index}. {symbol}, {funding_rate}%")

    #common_symbols = ["RNDR/USDT:USDT"] 
    #symbol_handlers = []
    for i in common_symbols:      
        i = i.replace('/', '').replace(':USDT', '')
        funding_rate = symbol_to_funding_rate[i] 

        handler = SymbolHandler(i, formatted_date, funding_rate)
        
        handler.kline_df = nance_utils.get_the_klines(i, "5m")
        handler.trades_df = nance_utils.get_hist_trades(i)
        try:
            asyncio.run(handler.update_recent_trades(fetch_trades_main))
        except Exception as e:
            print(f"Error while processing symbol {i}: {e}")
            continue
        
        plotter = SymbolPlotter(handler)
        plotter.trades_plot()
        plotter.chart_it()
"""