from secrets import TOKEN, SPOTIFY_USER_ID, DATABASE_LOCATION, PLAYLIST_ID
from refresh import Refresh_Access_Token
from main import GetHistory

def exec_spotify_pipeline():
    # refreshcaller = Refresh_Access_Token()
    # refreshcaller.request_token()
    a = GetHistory()
    a.run_ETL()
    print('Spotify ETL pipeline executed successfully.')


exec_spotify_pipeline()