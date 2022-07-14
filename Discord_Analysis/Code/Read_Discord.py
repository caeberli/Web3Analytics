import os
from django import db
import psycopg2
import csv
from datetime import datetime
import pandas as pd



def read_discord():

    # channels: introduce yourself, gm, 123, memes, public chat, nba banter, big3banter, fantasy-basketballs, offtopic, nftnyc, ballhogs, rumble kongs,
    channel_ids = [
        "899495366913826816",
        "895692812786466877",
        "977770379777048597",
        "882322405773692948",
        "913935129745948702",
        "849346727923417139",
        "987380652120879124",
        "909942298723373116",
        "856547150350909470",
        "954549283309371443",
        "971511953203810334",
        "971492237038997514",
    ]

    df = pd.read_csv('../data/last_read.csv')
    last_read = df['date'][0]
    # last_read = '2022-01-01 00:00'
    print(f'reading messages since {last_read}')

    for channel in channel_ids:

        discord_token = (
            "OTkwNjMyMDczMzAxNzQxNTg4.GFe3QE.II9nhxkZCK4UJXRXTaEvU_OxrNvyd9T6N5VZmM"
        )
        format = "Json"
        path = f"../data/DiscordExports2/myExport_{channel}.json"
        time = '"2022-06-26 18:34"'
        command = f"dotnet ../Extras/DiscordChatExporter.Cli/DiscordChatExporter.Cli.dll export -t {discord_token} -c {channel} -f {format} -o {path}"  # --after {time}"
        print(
            "####################################################################################################"
        )
        print(command + "\n")
        os.system(f"{command}")

    # compute date
    now = datetime.now()
    dt_string = now.strftime("%Y-%m-%d %H:%M") # current date and time
    last_read = pd.DataFrame([dt_string], columns=['date'])
    last_read.to_csv('../data/last_read.csv')

read_discord()
