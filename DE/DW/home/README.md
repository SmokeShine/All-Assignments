## Project Description
Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to

## How to Run the project
1. Please fill the fields in config file - dwh.cfg. This is required for interacting with redshift and S3.

[CLUSTER]
HOST=''
DB_NAME=''
DB_USER=''
DB_PASSWORD=''
DB_PORT=5439

[IAM_ROLE]
ARN=''

[S3]
LOG_DATA='s3://dend/song_data'
LOG_JSONPATH='s3://dend/log_data'
SONG_DATA='s3://dend/log_data'

[AWS]
ACCESS_KEY=''
SECRET_KEY=''

2. Run the command in terminal `python create_table.py` to create the structure of the data model

3. If successful with Step 2, run the command in terminal `python etl.py` to start loading the data in staging tables, followed by data push to the data model.

## The project template includes four files:

create_table.py is where you'll create your fact and dimension tables for the star schema in Redshift.
etl.py is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.
sql_queries.py is where you'll define you SQL statements, which will be imported into the two other files above.
README.md is where you'll provide discussion on your process and decisions for this ETL pipeline.

## Schema for Song Play Analysis
Star schema optimized for queries on song play analysis. This includes the following tables.

### Fact Table
songplays - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
### Dimension Tables
users - users in the app
user_id, first_name, last_name, gender, level
songs - songs in music database
song_id, title, artist_id, year, duration
artists - artists in music database
artist_id, name, location, lattitude, longitude
time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday

