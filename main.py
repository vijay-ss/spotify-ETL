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
        self.received_genres = self.get_genres()

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

    # Step 4: Get Genres

    def get_genres(self):
        print("Pulling genres based on listening history...")

        get_query = "https://api.spotify.com/v1/artists"

        data = self.received_songs

        # Create csv list of Spotify ids
        spotify_id = []

        for ids in data["items"]:
            spotify_id.append(ids["track"]["album"]["artists"][0]["id"])

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

    # Step 5: Extract relevant track info
    def extract_songs(self):
        print("Extracting song info from json request...")

        data = self.received_songs

        etl_dttm = datetime.datetime.now().strftime("%Y-%m-%d, %H:%M:%S")

        spotify_id = []
        uri = []
        song_names = []
        artist_names = []
        played_at_list = []
        date = []
        duration = []
        etl_time = []
        artist_uri = []

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
            etl_time.append(etl_dttm)
            artist_uri.append(song["track"]["album"]["artists"][0]["id"])

        song_dict = {
            "spotify_id": spotify_id,
            "uri": uri,
            "song_name": song_names,
            "artist_name": artist_names,
            "played_at": played_at_list,
            #"date": date,
            "duration_ms": duration,
            "ETL_DTTM": etl_time,
            "artist_uri": artist_uri
        }

        keys = list(song_dict.keys())
        song_df = pd.DataFrame(song_dict, columns = keys)
        print(song_df.head())

        return song_df

    # Step 6: Extract features
    def extract_features(self):
        print("Extracting features info from json request...")

        data = self.received_features
        features_df = pd.DataFrame.from_dict(data["audio_features"])
        features_df['ETL_DTTM'] = datetime.datetime.now().strftime("%Y-%m-%d, %H:%M:%S")
        features_df.to_csv(r'features.csv', index=False)

        return features_df

    # Extract genres
    def extract_genres(self):
        print("Extracting genres info from json request...")

        data = self.received_genres

        etl_dttm = datetime.datetime.now().strftime("%Y-%m-%d, %H:%M:%S")

        spotify_url = []
        total_followers = []
        genres = []
        artist_id = []
        artist_name = []
        popularity = []
        uri = []
        etl_time = []

        for artist in data['artists']:
            spotify_url.append(artist['external_urls']['spotify'])
            total_followers.append(artist['followers']['total'])
            genres.append(artist['genres'])
            artist_id.append(artist['id'])
            artist_name.append(artist['name'])
            popularity.append(artist['popularity'])
            uri.append(artist['uri'])
            etl_time.append(etl_dttm)

        artist_dict = {
            "spotify_url": spotify_url,
            "total_followers": total_followers,
            "genres": genres,
            "artist_id": artist_id,
            "artist_name": artist_name,
            "popularity": popularity,
            "uri": uri,
            "ETL_DTTM": etl_dttm
        }
        
        keys = list(artist_dict.keys())
        genres_df = pd.DataFrame(artist_dict, columns = keys)
        genres_df['genres'] = genres_df['genres'].astype(str)
        genres_df.to_csv(r'genres.csv', index=False)

        return genres_df

    # Step 7: Transform & validate data into usable format
    def validate(self, df: pd.DataFrame) -> bool:
        print("Validating API song data...")

        # Parse dates and convert to local timezone
        local_timezone = tzlocal.get_localzone()
        df["played_at"] = df.apply(lambda row: parse(row["played_at"]).astimezone(local_timezone), axis=1)
        df["date"] = df['played_at'].apply(lambda x: x.strftime("%Y-%m-%d"))
        print("Parsed dates and converted to local timezone.")

        # Delete where date != yesterday's date
        today = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday = (today - datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d")
        df.drop(df.loc[df['date'] != yesterday].index, inplace=True)

        df.to_csv(r'myhistory.csv', index=False)

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

    # Step 8: Load dataframe to SQL DB
    def load_songs(self, song_df: pd.DataFrame):
        print("Create database to store streaming history...")

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
            ETL_DTTM DATETIME,
            artist_uri VARCHAR(200),
            CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
        )
        """

        cursor.execute(sql_query)
        print("Opened database successfully.")

        song_df.to_sql("streaming_history", con=engine, index=False, if_exists='append')
        print("Appending new records...")

        # try:
        #     song_df.to_sql("streaming_history", con=engine, index=False, if_exists='append')
        # except:
        #     print("Data already exists in the database. Appending new records...")

        conn.close()
        print("Closed database successfully.")
    
    # Step 9: Load features to another table in SQL DB
    def load_features(self, features_df: pd.DataFrame):
        print("Create database to store song features...")
        
        engine = sqlalchemy.create_engine(DATABASE_LOCATION)
        conn = sqlite3.connect("streaming_history.sqlite")
        cursor = conn.cursor()

        sql_query = """
        CREATE TABLE IF NOT EXISTS song_features(
            danceability NUMERIC(10,4),
            energy NUMERIC(10,4),
            key INT,
            loudness NUMERIC(10,4),
            mode INT,
            speechiness NUMERIC(10,4),
            acousticness NUMERIC(10,4),
            instrumentalness NUMERIC(12,4),
            liveness NUMERIC(10,4),
            valence NUMERIC(10,4),
            tempo NUMERIC(10,4),
            type VARCHAR(200),
            id VARCHAR(200),
            uri VARCHAR(200),
            track_href VARCHAR(400),
            analysis_url VARCHAR(400),
            duration_ms INT,
            time_signature INT,
            ETL_DTTM DATETIME
        )
        """

        cursor.execute(sql_query)
        print("Opened database successfully.")

        features_df.to_sql("song_features", con=engine, index=False, if_exists='append')
        print("Appending new records...")

        conn.close()
        print("Closed database successfully.")

    # Step 10: Load genre info into SQL DB table
    def load_genres(self, genres_df: pd.DataFrame):
        print("Create database to store song genres...")

        engine = sqlalchemy.create_engine(DATABASE_LOCATION)
        conn = sqlite3.connect("streaming_history.sqlite")
        cursor = conn.cursor()

        sql_query = """
        CREATE TABLE IF NOT EXISTS genres(
            spotify_url VARCHAR(200),
            total_followers INT,
            genres VARCHAR(400),
            artist_id VARCHAR(200),
            artist_name VARCHAR(200),
            popularity INT,
            uri VARCHAR(200),
            ETL_DTTM DATETIME
        )
        """

        cursor.execute(sql_query)
        print("Opened database successfully.")

        genres_df.to_sql("genres", con=engine, index=False, if_exists='append')
        print("Appending new records...")

        conn.close()
        print("Closed database successfully.")

    # Final Step: Run ETL
    def run_ETL(self):
        #self.validate(self.extract_songs())
        #self.extract_features()
        self.load_songs(self.validate(self.extract_songs()))
        self.load_features(self.extract_features())
        self.load_genres(self.extract_genres())

# if __name__ == '__main__':
#     a = GetHistory()
#     a.run_ETL()