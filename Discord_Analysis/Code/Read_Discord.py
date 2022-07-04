import os
from django import db
import psycopg2

# Last time fetched data was 1st July 2022


def read_data():

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

    for channel in channel_ids:

        discord_token = (
            "OTA5NzI0MDk4NzczNTQwODc0.GpjOti.dOqGp6DNR4q85uX-ymR2i7kwtHVfX0QYN6i73I"
        )
        format = "Json"
        path = f"../data/DiscordExports1/myExport{channel}.json"
        time = '"2022-06-26 18:34"'
        command = f"dotnet ../Extras/DiscordChatExporter.Cli.dll export -t {discord_token} -c {channel} -f {format} -o {path}"  # --after {time}"
        # print("\nCopy paste command below this line!")
        print(
            "####################################################################################################"
        )
        print(command + "\n")
        os.system(f"{command}")


read_data()
