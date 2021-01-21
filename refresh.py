from secrets import REFRESH_TOKEN, BASE_64
import requests
import json

class Refresh_Access_Token:
    def __init__(self):
        self.refresh_token = REFRESH_TOKEN
        self.base_64 = BASE_64
    
    # Request new access token
    def request_token(self):

        query = "https://accounts.spotify.com/api/token"

        response = requests.post(query,
        data={"grant_type": "refresh_token",
        "refresh_token": self.refresh_token},
        headers={"Authorization": "Basic " + self.base_64})

        response_json = response.json()

        #print(response_json["access_token"])
        return response_json["access_token"]

# a = Refresh_Access_Token()
# a.request_token()