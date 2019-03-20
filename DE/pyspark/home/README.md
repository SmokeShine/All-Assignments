### IMPORTANT
1. in config file, please do not put the credentials in single quotes.

2. As the analytics directory was inaccessible, all parquet folders are created in the root folder.

### Project
Create a star schema optimized for queries on song play analysis. This includes the following tables.

Fact Table
songplays - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
Dimension Tables
users - users in the app
user_id, first_name, last_name, gender, level
songs - songs in music database
song_id, title, artist_id, year, duration
artists - artists in music database
artist_id, name, location, lattitude, longitude
time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday

### The project template includes three files:

etl.py reads data from S3, processes that data using Spark, and writes them back to S3
dl.cfgcontains your AWS credentials
README.md provides discussion on your process and decisions

### HOW TO RUN
1. Update the config file - dl.cfg, with AWS credentails and access key
2. Open python terminal and run the following command - python.etl.py
