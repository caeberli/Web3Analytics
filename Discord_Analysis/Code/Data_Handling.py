import json
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import re


def get_data_full(path):
    with open(path) as json_file:
        data = json.load(json_file)
    #negative_emojis = [""]

    sid_obj = SentimentIntensityAnalyzer()

    # get Name / Id / Timestamp (transform into day, month, hour, min, year) / Content
    name, ids, timestamp, content, msg_id, reactions, connotation_scores, types = [], [], [], [], [], [], [], []
    persons = {'guild': format_name(data['guild']['name']), 'channel': format_name(data['channel']['name'])}
    
    guild = format_name(data['guild']['name'])
    channel = data['channel']['name']
    channel_name = format_name(f"{guild}_{channel}")
    for msg in data['messages']:
        name.append(msg['author']['name'])
        ids.append(msg['author']['id'])
        timestamp.append(msg['timestamp'])
        content.append(msg['content'])
        msg_id.append(msg['id'])
        types.append(msg['type'])

        # compute reaction counts
        react_count = 0
        for react in msg['reactions']:
            react_count += react['count']
        reactions.append(react_count)

        # compute connotation scores
        sentiment_dict = sid_obj.polarity_scores(msg['content'])
        connotation_scores.append(sentiment_dict['compound'])

        # compute person
        if msg['author']['id'] not in persons.keys(): # if person is not in dict yet then add him
            persons[msg['author']['id']] = {
                'name': msg['author']['name'],
                'messages': 1,
                'reactions': 0,
                'connotation_scores': sentiment_dict['compound'],
                'types': 0, # 0 = message, 1 = reply 
                'mentioned': 0,
                'length': len(msg['content']),
                'temporal_spread': 0,
            }
        else:
            persons[msg['author']['id']]['messages'] += 1
            persons[msg['author']['id']]['reactions'] += react_count
            persons[msg['author']['id']]['connotation_scores'] += sentiment_dict['compound']
            persons[msg['author']['id']]['length'] += len(msg['content'])
            # if msg['type'] == 'reply':
            #     persons[msg['author']['id']]['types'] += 1

        # mentions -> extract user ID who was mentioned (list of lists)
        for mention in msg['mentions']:
            if mention['id'] in persons.keys(): 
                persons[mention['id']]['mentioned'] += 1
            else:
                persons[mention['id']] = {
                    'name': mention['name'],
                    'messages': 0,
                    'reactions': 0,
                    'connotation_scores': 0,
                    'types': 0,
                    'mentioned': 1,
                    'length': 0,
                    'temporal_spread': 0,
                }
 
    return name, ids, timestamp, content, msg_id, reactions, connotation_scores, types, channel_name, guild, persons


def format_name(text):
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F1E0-\U0001F1FF"  # flags (iOS)
        "]+",
        flags=re.UNICODE,
    )

    text = emoji_pattern.sub(r"", text)
    disallowed_characters = "| -!%"

    for character in disallowed_characters:
        text = text.replace(character, "")
    text = text.lower()
    return text


def sentiment_vader(sentence):

    # Create a SentimentIntensityAnalyzer object.
    sid_obj = SentimentIntensityAnalyzer()

    sentiment_dict = sid_obj.polarity_scores(sentence)
    negative = sentiment_dict["neg"]
    neutral = sentiment_dict["neu"]
    positive = sentiment_dict["pos"]
    compound = sentiment_dict["compound"]

    if sentiment_dict["compound"] >= 0.05:
        overall_sentiment = "Positive"

    elif sentiment_dict["compound"] <= -0.05:
        overall_sentiment = "Negative"

    else:
        overall_sentiment = "Neutral"

    return negative, neutral, positive, compound, overall_sentiment
