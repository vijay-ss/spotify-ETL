# Spotify ETL Process

# Project Summary
The main goal of this project is to create a machine learning recommender system from my personal Spotify streaming history. The first step is to creat an automated data feed which will run daily to download my streaming history for the past 24 hours. The song list will be appended to a SQL database for easy access and storage. The job itself will be executed using Apache-Airflow.

# Files
- main.py: main python file which contains code to execute the data pull using the Spotify API
- refresh.py: will refresh the Spotify token which expires after 60 minutes

