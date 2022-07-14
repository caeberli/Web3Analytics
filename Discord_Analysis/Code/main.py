import psycopg2
from Data_Handling import get_data_full
from psycopg2.extensions import AsIs
import os
from Create_Update_Database_Tables import insert_message_data, insert_person_data, read_database, find_ranking
from Read_Discord import read_discord

hostename = 'localhost'
username = 'dustin'
password = 'admin'
database = 'DiscordChatBot'
port_id = '5432'

####################################################################################################

read_discord()

# connect to database
print("Connecting to the PostgreSQL database...")
print(
    "####################################################################################################"
)
conn = psycopg2.connect(
    host=hostename, user=username, password=password, dbname=database, port=port_id
)
cur = conn.cursor()

##################################################################################################


# assign directory
directory = "../data/DiscordExports1"

# iterate over files in
# that directory
for filename in os.listdir(directory):

    PATH = os.path.join(directory, filename)
    print(PATH)
    # checking if it is a file
    if os.path.isfile(PATH):

        print(
            "####################################################################################################"
        )

        # get_data
        # PATH = '/home/dustin/Documents/Ducia/StartUp/Discord_Analysis/data/DiscordExports/myExport.json'
        name, id, timestamp, content, msg_id, reactions, connotation_scores, types, channel_name, guild_name, persons = get_data_full(PATH)


        # insert data
        insert_message_data(cur, conn, name, id, timestamp, content, msg_id, reactions, connotation_scores, types, channel_name)
        insert_person_data(cur, conn, persons, guild_name)
        read_database(cur, conn, guild_name)

find_ranking(cur, guild_name)


#####################################################################################################################
conn.close()