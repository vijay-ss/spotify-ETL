"""
TO DO:

- Create a secrets file
- Pull songs from spotify using credentials
- Refresh the token to be used repeatedly
- extract track info from json request
- store tracks into a db
- validate data and check for errors
- create a job to run this process daily

- timestamp timezone fix
- create subclass for audio features?
"""

import sqlalchemy
import sqlite3
import pandas as pd
import numpy as np
import requests
import json
from dateutil.parser import parse
import datetime
import tzlocal
import pytz
#from pytz import timezone
from secrets import TOKEN, SPOTIFY_USER_ID, DATABASE_LOCATION, PLAYLIST_ID
from refresh import Refresh_Access_Token


class GetHistory:

    def __init__(self):
        self._spotify_user_id = SPOTIFY_USER_ID
        self._spotify_access_token = self.refresh_token()
        self.playlist_id = PLAYLIST_ID
        self.tracks = ""
        self.database_location = DATABASE_LOCATION
        self.received_songs = self.get_songs()
        self.received_features = self.get_features()

    # Step 1: Refresh Spotify token
    def refresh_token(self):
        print("Refreshing Spotify token...")

        refreshcaller = Refresh_Access_Token()
        self.spotify_access_token = refreshcaller.request_token()
        return refreshcaller.request_token()

    # Step 2: Gather recently played songs via request
    def get_songs(self):
        print("Accessing recently played songs via API call...")

        get_query = "https://api.spotify.com/v1/me/player/recently-played"

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "Bearer {token}".format(token = self.spotify_access_token)
        }

        # Convert time to unix format for last 24 hours playback
        today = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday = (today - datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
        today_unix_timestamp = int(today.timestamp()) * 1000

        params = {
            "limit": 50,
            "after": yesterday_unix_timestamp
            #"before": today_unix_timestamp
        }

        response = requests.get(get_query, headers=headers, params=params)
        response_json = response.json()
        return response_json

    # Step 3: Get Audio features
    def get_features(self):
        print('Pulling features based on listening history...')

        get_query = "https://api.spotify.com/v1/audio-features"

        data = self.received_songs

        # Create csv list of Spotify ids
        spotify_id = []

        for ids in data["items"]:
            spotify_id.append(ids["track"]["id"])

        csv_list = ','.join(spotify_id)

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "Bearer {token}".format(token = self.spotify_access_token)
        }

        params = {
            "ids": csv_list
        }
       
        response = requests.get(get_query, headers=headers, params=params)
        response_json = response.json()

        return response_json

    # Step 4: Extract relevant track info
    def extract_songs(self):
        print("Extracting song info from json request...")

        data = self.received_songs

        spotify_id = []
        uri = []
        song_names = []
        artist_names = []
        played_at_list = []
        date = []
        duration = []

        # Extracting only the relevant bits of data from the json object
        # https://developer.spotify.com/documentation/web-api/reference/player/get-recently-played/     
        for song in data["items"]:
            spotify_id.append(song["track"]["id"])
            uri.append(song["track"]["uri"])
            song_names.append(song["track"]["name"])
            artist_names.append(song["track"]["album"]["artists"][0]["name"])
            played_at_list.append(song["played_at"])
            date.append(song["played_at"][0:10])
            duration.append(song["track"]["duration_ms"])

        song_dict = {
            "spotify_id": spotify_id,
            "uri": uri,
            "song_name": song_names,
            "artist_name": artist_names,
            "played_at": played_at_list,
            #"date": date,
            "duration_ms": duration
        }

        keys = list(song_dict.keys())
        song_df = pd.DataFrame(song_dict, columns = keys)

        return song_df

    # Step 5: Extract features
    def extract_features(self):
        print("Extracting features info from json request...")

        data = self.received_features
        features_df = pd.DataFrame.from_dict(data["audio_features"])
        features_df.to_csv(r'features.csv')

        return features_df

    # Step 6: Transform & validate data into usable format
    def validate(self, df: pd.DataFrame) -> bool:
        print("Validating API song data...")

        # Parse dates and convert to local timezone
        local_timezone = tzlocal.get_localzone()
        df["played_at"] = df.apply(lambda row: parse(row["played_at"]).astimezone(local_timezone), axis=1)
        df["date"] = df['played_at'].apply(lambda x: x.strftime("%Y-%m-%d"))
        print("Parsed dates and converted to local timezone.")

        df.to_csv(r'myhistory.csv')

        # Check for empty dataframe
        if df.empty:
            print("No songs downloaded! Finishing execution.")
            return False

        # Check for primary key
        if pd.Series(df['played_at']).is_unique:
            pass
        else:
            raise Exception("Primary key check is violated!")

        # Check for null values
        if df.isnull().values.any():
            raise Exception("Null values found!")

        # Check that all dates are of yesterday's date
        yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

        # dates = df["date"].tolist()
        # for date in dates:
        #     if datetime.datetime.strptime(date, '%Y-%m-%d') != yesterday:
        #         raise Exception(f"At least one of the returned songs do not have yesterday's date of {yesterday}.")
        #     return True

        return df

    # Load data to SQL database
    def load(self, song_df: pd.DataFrame):
        print("Create database to store streaming history and features...")

        engine = sqlalchemy.create_engine(DATABASE_LOCATION)
        conn = sqlite3.connect("streaming_history.sqlite")
        cursor = conn.cursor()

        sql_query = """
        CREATE TABLE IF NOT EXISTS streaming_history(
            spotify_id VARCHAR(200),
            uri VARCHAR(200),
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            played_at VARCHAR(200),
            duration_ms INT,
            date DATE,
            CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
        )
        """

        cursor.execute(sql_query)
        print("Opened database successfully.")

        song_df.to_sql("streaming_history", con=engine, index=False, if_exists='append')

        # try:
        #     song_df.to_sql("streaming_history", con=engine, index=False, if_exists='append')
        # except:
        #     print("Data already exists in the database. Appending new records...")

        conn.close()
        print("Closed database successfully.")

    # Final Step: Run ETL
    def run_ETL(self):
        #self.validate(self.extract_songs())
        #self.extract_features()
        self.load(self.validate(self.extract_songs()))

# if __name__ == '__main__':
#     a = GetHistory()
#     a.run_ETL()