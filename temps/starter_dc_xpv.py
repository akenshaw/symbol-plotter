import sys, asyncio
from xpv_dc_class import main_custom
import discord

#TOKEN = "my_token"
#channel_id = "my_channel_id"

if __name__ == "__main__":
    symbols = sys.argv[1:]
    file_names_list = main_custom(symbols)

intents = discord.Intents.default()
client = discord.Client(intents=intents)

async def send_plot_to_discord(filename, channel_id):
    channel = client.get_channel(int(channel_id))
    plot_file = discord.File(filename, filename=filename)
    await channel.send(file=plot_file)

async def send_all_plots(file_names_list, channel_id):
    for file_name in file_names_list:
        await send_plot_to_discord(file_name, channel_id)

@client.event
async def on_ready():
    print(f"We have logged in as {client.user}")
    await send_all_plots(file_names_list, channel_id)

client.run(TOKEN)
