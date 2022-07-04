from django import db
import psycopg2
from Data_Handling import get_data_full
from psycopg2.extensions import AsIs
import os

hostename = "localhost"
username = "postgres"
password = "postgres"
database = "KrauseHouseDiscord"
port_id = "5433"

####################################################################################################

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
        (
            name,
            id,
            timestamp,
            content,
            msg_id,
            reactions,
            connotation_scores,
            types,
            channel_name,
            mentions,
        ) = get_data_full(PATH)

        print(
            "####################################################################################################"
        )
        print(f"{len(name)} messages in channel")
        print(
            "####################################################################################################"
        )

        # Create and Insert

        # get message-Table
        cur.execute(
            """SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'"""
        )
        tables = [table[0] for table in cur.fetchall()]

        if len(tables) > 0 and channel_name in tables:
            print(f"Table {channel_name} already exists.")
            cur.execute(
                """ SELECT msg_id
                FROM %s""",
                (AsIs(channel_name),),
            )
            msg_ids = [msg_ids[0] for msg_ids in cur.fetchall()]

            for i in range(len(name)):
                if msg_id[i] not in msg_ids:
                    cur.execute(
                        """INSERT INTO %s (name, id, timestamp, content, msg_id, reactions, connotation_scores, types)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                        (
                            AsIs(channel_name),
                            name[i],
                            id[i],
                            timestamp[i],
                            content[i],
                            msg_id[i],
                            reactions[i],
                            connotation_scores[i],
                            types[i],
                        ),
                    )
                    conn.commit()
        else:
            print(f"Table {channel_name} does not exist. Creating...")
            cur.execute(
                """CREATE TABLE %(str)s (
                identifier SERIAL PRIMARY KEY,
                name VARCHAR(255),
                id VARCHAR(255),
                timestamp VARCHAR(255),
                content VARCHAR(5255),
                msg_id VARCHAR(255),
                reactions INTEGER,
                connotation_scores FLOAT,
                types VARCHAR(255)
            )""",
                {"str": AsIs(channel_name)},
            ),
            print(f"Table {channel_name} created.")
            print(
                "####################################################################################################"
            )

            # insert data
            print("Inserting data...")
            print(
                "####################################################################################################"
            )
            for i in range(len(name)):
                cur.execute(
                    """INSERT INTO %s (name, id, timestamp, content, msg_id, reactions, connotation_scores, types)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (
                        AsIs(channel_name),
                        name[i],
                        id[i],
                        timestamp[i],
                        content[i],
                        msg_id[i],
                        reactions[i],
                        connotation_scores[i],
                        types[i],
                    ),
                )
                conn.commit()
            print("Data inserted.")
            print(
                "####################################################################################################"
            )


#####################################################################################################################
conn.close()
