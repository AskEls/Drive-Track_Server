from pymongo import MongoClient
import pandas as pd
import toml 
import os
import datetime as time
from geopy.geocoders import Nominatim
import requests

def flatten_data(data):
    required_keys = ['timestamp', 'date','longitude' , 'latitude','city','operator','networkmode','device','arfcn','level','qual','dl_bitrate','ul_bitrate']
    flattened = [
        {key: inner_item[key] for key in required_keys}
        for item in data if isinstance(item.get('data'), list)
        for inner_item in item['data'] if all(key in inner_item for key in required_keys)
    ]

    return flattened

def virgin():
    conf_path = os.path.join(os.path.dirname(__file__), 'conf.toml')
    dbconf = toml.load(conf_path)
    return dbconf['credentials-virgin'] 

def master():
    conf_path = os.path.join(os.path.dirname(__file__), 'conf.toml')
    dbconf = toml.load(conf_path)
    return dbconf['credentials-master'] 


def connect_db(): # 
    username = virgin()['username']
    password = virgin()['password']
    host = virgin()['host']
    port = virgin()['port']
    auth = virgin()['authSource']

    connect_string =f"mongodb://{username}:{password}@{host}:{port}/?authSource={auth}"
    mockup_db = f"mongodb://localhost:27017/"            

    client = MongoClient(mockup_db)
    return client



def get_data():
    auth = virgin()['authSource']
    collection = virgin()['collection']
    client = connect_db()
    db = client[auth]
    collection = db[collection]
    data = list(collection.find({}, {

        '_id':0, 'data.timestamp': 1,
        'data.date': 1, 
        'data.longitude': 1, 
        'data.latitude': 1,
        'data.city':1,
        'data.operator': 1,
        'data.networkmode': 1,
        'data.level': 1,
        'data.qual': 1,
        'data.dl_bitrate': 1,
        'data.ul_bitrate': 1         

        }))

    flattened_data = flatten_data(data)
    df = pd.DataFrame(flattened_data)   

    df['operator'] = df['operator'].astype(str).str.replace(',','')
    #df['city'] = df.apply(lambda row: get_city(row['latitude'], row['longitude']), axis=1)

    datawarehouse = df

    return datawarehouse



