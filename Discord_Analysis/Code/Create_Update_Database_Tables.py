import psycopg2
from psycopg2.extensions import AsIs
import os
import pandas as pd
import json
import math


def insert_message_data(cur, conn, name, id, timestamp, content, msg_id, reactions, connotation_scores, types, channel_name):

    print(f'{len(name)} messages in database')

    # get message-Table
    cur.execute("""SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'""")
    tables = [table[0] for table in cur.fetchall()]

    if len(tables) > 0 and channel_name in tables:
        print(f'Table "{channel_name}" already exists.\n Updating Entries')

        cur.execute(""" SELECT msg_id
            FROM %s""",(AsIs(channel_name),))
        msg_ids = [msg_ids[0] for msg_ids in cur.fetchall()]

        msg_added = 0
        for i in range(len(name)):
            if msg_id[i] not in msg_ids:
                msg_added += 1
                cur.execute("""INSERT INTO %s (name, id, timestamp, content, msg_id, reactions, connotation_scores, types)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", (AsIs(channel_name), name[i], id[i], timestamp[i], content[i], msg_id[i], reactions[i], connotation_scores[i], types[i]))
                conn.commit()
        print(f'{msg_added} messages added.\n')
    else:
        print(f'Table {channel_name} does not exist. Creating...')
        cur.execute("""CREATE TABLE %(str)s (
            identifier SERIAL PRIMARY KEY,
            name VARCHAR(255),
            id VARCHAR(255),
            timestamp VARCHAR(255),
            content VARCHAR(5255),
            msg_id VARCHAR(255),
            reactions INTEGER,
            connotation_scores FLOAT,
            types VARCHAR(255)
        )""", {'str': AsIs(channel_name)}),
        print(f'Table {channel_name} created.')

        # insert data
        print('Inserting data...')
        for i in range(len(name)):
            cur.execute("""INSERT INTO %s (name, id, timestamp, content, msg_id, reactions, connotation_scores, types)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", (AsIs(channel_name), name[i], id[i], timestamp[i], content[i], msg_id[i], reactions[i], connotation_scores[i], types[i]))
            conn.commit()
        print(f'{len(name)} messages added.\n')
    
    return channel_name


def insert_person_data(cur, conn, persons, channel_name):

    print(f'{len(persons)-2} persons in database')

    # get message-Table
    cur.execute("""SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'""")
    tables = [table[0] for table in cur.fetchall()]

    if len(tables) > 0 and channel_name + 'persons' in tables: # if table exists
        print(f'Table "{channel_name}persons" already exists.\n Updating Entries')

        cur.execute(""" SELECT id
            FROM %s""",(AsIs(channel_name+'persons'),))
        ids = [ids[0] for ids in cur.fetchall()]

        id_added, id_updated = 0, 0
        for id in persons.keys():
            if id not in ids and id not in ['guild', 'channel']: # create new person
                id_added += 1
                cur.execute("""INSERT INTO %s (id, name, messages, reactions, connotation_scores, types, mentioned, length)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", (AsIs(channel_name+'persons'), id, persons[id]['name'], persons[id]['messages'], persons[id]['reactions'], persons[id]['connotation_scores'], persons[id]['types'], persons[id]['mentioned'], persons[id]['length']))
                conn.commit()
            elif id not in ['guild', 'channel']: # update person
                id_updated += 1
                # get old values
                cur.execute("""SELECT name, messages, reactions, connotation_scores, types, mentioned, length
                    FROM %s
                    WHERE id = %s""", (AsIs(channel_name+'persons'), id))
                old_values = cur.fetchall()
                old_values = [old_values[0][0], old_values[0][1], old_values[0][2], old_values[0][3], old_values[0][4], old_values[0][5], old_values[0][6]]

                # update values
                cur.execute("""UPDATE %s
                    SET name = %s, messages = %s, reactions = %s, connotation_scores = %s, types = %s, mentioned = %s, length = %s
                    WHERE id = %s""", (AsIs(channel_name+'persons'), persons[id]['name'], persons[id]['messages'] + old_values[1], persons[id]['reactions'] + old_values[2], persons[id]['connotation_scores'] + old_values[3], persons[id]['types'] + old_values[4], persons[id]['mentioned'] + old_values[5], persons[id]['length'] + old_values[6], id))
                conn.commit()
                
        print(f'{id_added} persons added and {id_updated} updated.\n')

    else: # create table
        print(f'Table {channel_name}persons does not exist. Creating...')
        cur.execute("""CREATE TABLE %(str)s (
            identifier SERIAL PRIMARY KEY,
            id VARCHAR(255),
            name VARCHAR(255),
            messages INTEGER,
            reactions INTEGER,
            connotation_scores FLOAT,
            types FLOAT,
            mentioned INTEGER,
            length INTEGER
        )""", {'str': AsIs(channel_name+'persons')})
        print(f'Table {channel_name}persons created.')

        # insert data
        print('Inserting data...')
        for id in persons.keys():
            if id not in ['guild', 'channel']:
                cur.execute("""INSERT INTO %s (id, name, messages, reactions, connotation_scores, types, mentioned, length)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", (AsIs(channel_name+'persons'), id, persons[id]['name'], persons[id]['messages'], persons[id]['reactions'], persons[id]['connotation_scores'], persons[id]['types'], persons[id]['mentioned'], persons[id]['length']))
                conn.commit()
        print(f'{len(persons)-2} persons added.\n')

    return channel_name+'persons'
 

def read_database(cur, conn, guild_name):

    cur.execute("""SELECT id, name, messages, reactions, connotation_scores, types, mentioned, length
        FROM %s
        ORDER BY connotation_scores + reactions DESC
        LIMIT 10""", (AsIs(guild_name+'persons'),))
    for row in cur.fetchall():
        print(f'{row}')

    # # group user by id
    # print(f'\ndiscord ID \t number_messages \t scores \t reactions \t discord name \n')
    # cur.execute("""SELECT id, COUNT(*), name, SUM(connotation_scores) / COUNT(*), SUM(reactions)
    #     FROM %s
    #     GROUP BY id, name
    #     ORDER BY COUNT(*) DESC
    #     LIMIT 30""",(AsIs(guild_name),))
    # for row in cur.fetchall():
    #     print(f'{row[0]} \t {row[1]} \t \t %.4f \t {row[4]} \t \t {row[2]}' % (row[3]))


    # cur.execute("""SELECT content 
    #     FROM %s
    #     WHERE id = '901871163800699000'
    #     LIMIT 5""", (AsIs(guild_name),))
    # for row in cur.fetchall():
    #     print(row)

def find_ranking(cur, guild_name):

    # get columns
    # cur.execute("""SELECT column_name
    #     FROM information_schema.columns
    #     WHERE table_name = '%s'""", (AsIs(guild_name+'persons'),))
    # columns = [column[0] for column in cur.fetchall()]
    columns = ['messages', 'reactions', 'connotation_scores', 'mentioned', 'length']

    # get mean and std of all columns
    means, stds, mins, maxs = [], [], [], []
    for i in range(len(columns)):
        cur.execute("""SELECT AVG(Cast(%s as Float)), STDDEV(Cast(%s as Float)), MIN(Cast(%s as Float)), MAX(Cast(%s as Float))
            FROM %s""", (AsIs(columns[i]), AsIs(columns[i]), AsIs(columns[i]), AsIs(columns[i]), AsIs(guild_name+'persons')))
        mean, std, min_, max_ = cur.fetchone()
        means.append(mean)
        stds.append(std)
        mins.append(min_)
        maxs.append(max_)

    print(f'\nMeans: {means}, Stds: {stds}, Mins: {mins}, Maxs: {maxs}')

    # get ranking
    dict_persons = {}
    cur.execute("""SELECT id, name, messages, reactions, connotation_scores / (messages + 0.1), types, mentioned, length, 0.5 * (messages - %s)/ %s + (reactions - %s)/ %s + 5 * connotation_scores / (messages+0.1) + (mentioned - %s)/ %s + 0.5 * (length - %s)/ %s As community_score
        FROM %s
        WHERE id <> '911410076412158002'
        ORDER BY community_score DESC
        LIMIT 40""", (means[0], stds[0], means[1], stds[1], means[2], stds[2], means[4], stds[4], AsIs(guild_name+'persons')))

    for i, row in enumerate(cur.fetchall()):
        #print(f'{row[0]} \t {row[1]} \t \t {row[2]} \t {row[3]} \t \t {row[4]} \t {row[5]} \t {row[6]} \t {row[7]} \t {row[8]}')
        dict_persons[i+1] = {'id': row[0], 'name': row[1], 'messages': f'{normToScore(mins[0], maxs[0], row[2]):.4f}', 'reactions': f'{normToScore(mins[1], maxs[1], row[3]):.4f}', 'connotation_scores': f'{100*row[4]:.4f}', 'types': row[5], 'mentioned': row[6], 'length': row[7], 'community_score': f'{10 * math.sqrt(row[8]):.4f}'}

    with open('../data/ranking_persons.json', 'w') as fp:
        json.dump(dict_persons, fp)

    return

def normToScore(min, max, value):
    return (value - min) / (max - min) * 100

#ToDo 
# before aggregation to community_score normalize all metrics (connotation_scores, reactions, messages, mentioned, length)
