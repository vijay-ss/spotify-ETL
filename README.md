# Spotify ETL Process

# Project Summary
The main objective of this project is to develop a machine learning recommender system using my personal Spotify streaming history. The first step is to create an automated data feed (ETL) which will run daily to download my streaming history from the past 24 hours. The song list will be appended to a SQL database for easy access and storage. The job itself will be executed using Apache-Airflow.

# Files
- main.py: main python file which contains the code to fetch the data using the Spotify API
- refresh.py: will refresh the Spotify token which expires after 60 minutes
- pipeline.py: a simple function to execute the ETL process in main.py

