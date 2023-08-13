import sys
import time
import glob
import shutil
import pandas as pd
from pymongo import MongoClient
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os
import mimetypes
import toml
from config.db_config import *
from datetime import datetime
from bson.timestamp import Timestamp
import time
import requests
import asyncio
import aiohttp
from threading import Thread
import json

class MyHandler(FileSystemEventHandler):
    def __init__(self):
        super().__init__()

    def on_modified(self, event):
        print(f'File {event.src_path} has been modified')
        asyncio.run_coroutine_threadsafe(self.process(event), self.loop)

    def on_created(self, event):
        print(f'File {event.src_path} has been created')
        asyncio.run_coroutine_threadsafe(self.process(event), self.loop)

    def lte_rename(self, value):
        return 'LTE' if value == '4G' else value

    def get_country(self, lat, lon):
        url = f'https://nominatim.openstreetmap.org/reverse?lat={lat}&lon={lon}&format=json&namedetails=1&accept-language=en&zoom=8'
        try:
            result = requests.get(url=url)
            result_json = result.json()
            return result_json['name'].upper()
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            return None

    async def get_country_async(self, lat, lon):

        url = f'https://nominatim.openstreetmap.org/reverse?lat={lat}&lon={lon}&format=json&namedetails=1&accept-language=en&zoom=8'
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url) as response:
                    result_json = await response.json()
                    city_name = result_json['address'].get('city', 'Unknown')
                    print(f'Fetched city: {city_name}')
                    return city_name.upper()
            except aiohttp.ClientError as e:
                print(f"An error occurred: {e}")
                return None


    def filter_mnc(self, mnc):
        mnc_mapping = {
            51001: 'Telkom',
            51011: 'XL',
            51001: 'IM3',
            51009: 'Smartfreen',
            51099: 'Bolt',
            51089: '3ID'
        }
        return mnc_mapping.get(mnc, None)

    def cleansing_data(self, df):

        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y.%m.%d_%H.%M.%S')
        df['date'] = df['timestamp'].dt.strftime('%Y-%m-%d')

        df['level'] = pd.to_numeric(df['level'], errors='coerce')
        df['level'] = df['level'].fillna(0).astype(int)
        
        df['qual'] = pd.to_numeric(df['qual'], errors='coerce')
        df['qual'] = df['qual'].fillna(0).astype(int)
            
        df.loc[df['networkmode'] == '4G', 'networkmode'] = 'LTE'
        df = df[~( (df['networkmode'] == '2G') & (df['level'] == -113))]
        df.loc[:, 'opname'] = df['operator'].apply(self.filter_mnc)

        return df


    async def process(self, event):

        if event is None or event.is_directory:
            return

        file = event.src_path
        mimetype = mimetypes.guess_type(file)[0]

        if not (mimetype == 'txt' or mimetype is None):
            print(f'File {file} is not a txt or file format. Deleting...')
            try:
                os.remove(file)
            except Exception as e:
                print(f'An error occurred while deleting the file: {e}')
            return

        if not os.path.isfile(file):
            print(f'File {file} no longer exists')
            return

        try:
            print(f'Reading file {file}...')
            df = pd.read_table(file, on_bad_lines='warn')

            print(f'Successfully read file: {file}')

            print('Cleaning data...')
            df.columns = df.columns.str.lower()
            df = df[['timestamp', 'longitude', 'latitude', 'operator', 'networkmode', 'device',
                     'arfcn', 'level', 'qual', 'dl_bitrate', 'ul_bitrate']].copy()

            df = self.cleansing_data(df)
            
            data = df.to_dict('records')

            now = datetime.now()
            document = {
                'file_name': os.path.basename(file),
                'timestamp': datetime.now(),
                'data': data,
            }

            db_config = master()  # Assuming this function provides your database configuration
            connect_string = f"mongodb://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/"
            mockup_db = f"mongodb://localhost:27017/" # mockup db
            
            client = MongoClient(mockup_db)
            db = client[db_config['authSource']]
            collection = db[db_config['collection']]

            try:
                print('Uploading to MongoDB...')
                collection.insert_one(document)
                print('Data inserted successfully.')
               '''
                # Update the 'city' field for each record in the database
                for record in data:
                    lat = record['latitude']
                    lon = record['longitude']
                    city_name = await self.get_country_async(lat, lon)
                    if city_name:
                        collection.update_one(
                            {'_id': record['_id']},  # Assuming _id is the unique identifier for each record
                            {'$set': {'city': city_name}}
                        )
                        print(f'Updated city for record {_id}: {city_name}')
                '''
            except Exception as e:
                print(f"Error occurred: {e}")

            time.sleep(5)

            print('Moving file to backup directory...')
            if not os.path.exists('./storage'):
                os.makedirs('./storage')

            shutil.move(file, './storage')

            print('Done!')

        except Exception as e:
            print(f'Error occurred: {e}')
            os.remove(file)


if __name__ == "__main__":
    event_handler = MyHandler()
    observer = Observer()
    observer.schedule(event_handler, path='./temp', recursive=False)

    event_handler.loop = asyncio.get_event_loop()

    try:
        observer_thread = Thread(target=observer.start)
        observer_thread.daemon = True
        observer_thread.start()

        event_handler.loop.run_forever()

    except KeyboardInterrupt:
        observer.stop()

    observer.join()
