import sys, asyncio, typing, functools
from xpv_dc_class import MainHandler
import discord
from discord.ext import tasks

#TOKEN = "my_token"
#channel_id = "my_channel_id"

intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

async def run_blocking(blocking_func: typing.Callable, *args, **kwargs) -> typing.Any:
    func = functools.partial(blocking_func, *args, **kwargs) 
    return await client.loop.run_in_executor(None, func)

main_handler = MainHandler()

async def send_plot_to_discord(filename, channel_id):
    channel = client.get_channel(int(channel_id))
    plot_file = discord.File(filename, filename=filename)
    await channel.send(file=plot_file)

async def send_all_plots(file_names_list, channel_id):
    channel = client.get_channel(int(channel_id))
    
    grouped_files = {}
    for file_name in file_names_list:
        symbol = file_name.split('/')[-1].split('_')[0]
        if symbol not in grouped_files:
            grouped_files[symbol] = []
        grouped_files[symbol].append(discord.File(file_name, filename=file_name))
        
    for symbol, files in grouped_files.items():
        await channel.send(f"{symbol}:")
        await channel.send(files=files)


@client.event
async def on_ready():
    print(f"We have logged in as {client.user}")
    #main_task.start()

@tasks.loop(hours=3)
async def main_task():
    file_names_list = await run_blocking(main_handler.main)
    file_names_list = await file_names_list
    channel_id = 1119543394411237456
    await send_all_plots(file_names_list, channel_id)

@client.event
async def on_message(message):
    if message.content.startswith('plot'):
        symbols = message.content.split()[1:]
        file_names_list = await run_blocking(main_handler.main_custom, symbols, is_aggregated=False)
        file_names_list = await file_names_list
        await send_all_plots(file_names_list, message.channel.id)
    
    elif message.content.startswith('aggrplot'):
        symbols = message.content.split()[1:]
        file_names_list = await run_blocking(main_handler.main_custom, symbols, is_aggregated=True)
        file_names_list = await file_names_list
        await send_all_plots(file_names_list, message.channel.id)
    
    elif message.content.startswith('autoplot'):
        await main_task()

client.run(TOKEN)