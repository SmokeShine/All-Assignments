# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE songplays (
timestamp time NOT NULL,
user_id int NOT NULL,
level varchar,
song_id varchar,
artist_id varchar,
sessionId int,
userAgent varchar,
primary key (user_id,timestamp)
)
""")

user_table_create = ("""
CREATE TABLE users (
user_id int NOT NULL PRIMARY KEY,
first_name varchar,
last_name varchar,
gender varchar,
level varchar)
""")

song_table_create = ("""
CREATE TABLE songs (
song_id varchar NOT NULL PRIMARY KEY,
title varchar,
artist_id varchar,
year int,
duration float
);

""")

artist_table_create = ("""
CREATE TABLE artists (
artist_id varchar NOT NULL PRIMARY KEY,
artist_name varchar,
artist_location varchar,
artist_latitude float,
artist_longitude float
);
""")

time_table_create = ("""
CREATE TABLE time (
timestamp time NOT NULL PRIMARY KEY,
hour int,
day int,
week_of_year int,
month int,
year int,
weekday int 
);
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (timestamp ,
                    user_id ,
                    level ,
                    song_id ,
                    artist_id ,
                    sessionId ,
                    userAgent )
VALUES (%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (user_id,timestamp) 
DO UPDATE SET
level=songplays.level,
song_id=songplays.song_id,
artist_id=songplays.artist_id,
sessionId=songplays.sessionId,
userAgent=songplays.userAgent
;
""")

user_table_insert = ("""
INSERT INTO users (user_id,
                    first_name,
                    last_name,
                    gender,
                    level)
VALUES (%s,%s,%s,%s,%s)
ON CONFLICT (user_id) 
DO UPDATE SET
first_name=users.first_name,
last_name=users.last_name,
gender=users.gender,
level=users.level
;
""")

song_table_insert = ("""
INSERT INTO songs (song_id,
                    title,
                    artist_id,
                    year,
                    duration )
VALUES (%s,%s,%s,%s,%s)
ON CONFLICT (song_id) 
DO UPDATE SET
title=songs.title,
artist_id=songs.artist_id,
year=songs.year,
duration=songs.duration
;
""")

artist_table_insert = ("""
INSERT INTO artists (
artist_id ,
artist_name ,
artist_location ,
artist_latitude ,
artist_longitude 
)
VALUES (%s,%s,%s,%s,%s)
ON CONFLICT (artist_id) 
DO UPDATE SET
artist_name=artists.artist_name,
artist_location=artists.artist_location,
artist_latitude=artists.artist_latitude ,
artist_longitude=artists.artist_longitude 
;
""")


time_table_insert = ("""
INSERT INTO time ( timestamp,
                    hour,
                    day,
                    week_of_year,
                    month,
                    year,
                    weekday
                    )
VALUES (%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (timestamp) 
DO UPDATE SET
hour=time.hour,
day=time.day,
week_of_year=time.week_of_year,
month=time.month,
year=time.year,
weekday=time.weekday
;
""")

# FIND SONGS

song_select = ("""
    Select a.song_id,b.artist_id
            from 
            songs a
    inner join
            artists b 
            on a.artist_id=b.artist_id
    
    where 
            a.title in (%s)
            and
            b.artist_name in (%s)
            and 
            a.duration = %s

""")

# QUERY LISTS
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
