import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
	artist varchar(256),
	auth varchar(256),
	firstname varchar(256),
	gender varchar(256),
	iteminsession int4,
	lastname varchar(256),
	length numeric(18,0),
	"level" varchar(256),
	location varchar(256),
	"method" varchar(256),
	page varchar(256),
	registration numeric(18,0),
	sessionid int4,
	song varchar(256),
	status int4,
	ts int8,
	useragent varchar(256),
	userid int4
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
	num_songs int4,
	artist_id varchar(256),
	artist_name varchar(256),
	artist_latitude numeric(18,0),
	artist_longitude numeric(18,0),
	artist_location varchar(256),
	song_id varchar(256),
	title varchar(256),
	duration numeric(18,0),
	"year" int4
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
	playid varchar(32) NOT NULL ,
	start_time timestamp NOT NULL distkey sortkey,
	userid int4 NOT NULL ,
	"level" varchar(256),
	songid varchar(256) ,
	artistid varchar(256),
	sessionid int4,
	location varchar(256),
	user_agent varchar(256),
	CONSTRAINT songplays_pkey PRIMARY KEY (playid)   
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id int NOT NULL PRIMARY KEY sortkey,
first_name varchar,
last_name varchar,
gender varchar,
level varchar)

""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
	songid varchar(256) NOT NULL sortkey,
	title varchar(256),
	artistid varchar(256),
	"year" int4,
	duration numeric(18,0),
	CONSTRAINT songs_pkey PRIMARY KEY (songid)
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
	artistid varchar(256) NOT NULL PRIMARY KEY sortkey,
	name varchar(256),
	location varchar(256),
	lattitude numeric(18,0),
	longitude numeric(18,0)
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
timestamp TIMESTAMP NOT NULL PRIMARY KEY distkey sortkey,
hour int,
day int,
week_of_year int,
month int,
year int,
weekday int 
);
""")

# STAGING TABLES

staging_events_copy = ("""
        COPY {}
        FROM {}
        ACCESS_KEY_ID {}
        SECRET_ACCESS_KEY {}
        csv  IGNOREHEADER 1;
""").format("staging_events",
            config.get('S3','SONG_DATA'),
            config.get('AWS','ACCESS_KEY'),
            config.get('AWS','SECRET_KEY'))

staging_songs_copy = ("""
        COPY {}
        FROM {}
        ACCESS_KEY_ID {}
        SECRET_ACCESS_KEY {}
        json 'auto';
""").format("staging_songs",
            config.get('S3','LOG_DATA'),
            config.get('AWS','ACCESS_KEY'),
            config.get('AWS','SECRET_KEY'))

# FINAL TABLES

songplay_table_insert = ("""
        
        INSERT INTO songplays  
        SELECT  distinct
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
""")

user_table_insert = ("""
        INSERT INTO users
        SELECT distinct ss.userid, ss.firstname, ss.lastname, ss.gender, ss.level
        FROM staging_events ss
        LEFT JOIN users u ON ss.userid = u.user_id 
        WHERE page='NextSong'
        and u.user_id IS NULL
""")

song_table_insert = ("""
        INSERT INTO songs
        SELECT distinct ss.song_id, ss.title, ss.artist_id, ss.year, ss.duration
        FROM staging_songs ss
        LEFT JOIN songs s ON ss.song_id = s.songid 
        where s.songid IS NULL
""")

artist_table_insert = ("""
        INSERT INTO artists
        SELECT distinct ss.artist_id, ss.artist_name, ss.artist_location, ss.artist_latitude, ss.artist_longitude
        FROM staging_songs ss
        LEFT JOIN artists a ON ss.artist_id = a.artistid 
        where a.artistid IS NULL
""")

time_table_insert = ("""
        INSERT INTO time
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
