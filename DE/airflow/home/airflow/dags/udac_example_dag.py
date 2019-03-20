from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')
# https://madeintandem.com/blog/data-into-amazon-redshift/

### Variables

default_args = {
    'owner': 'udacity',
#     'start_date': datetime(2019, 1, 12, 0, 0, 0, 0),
    'start_date': datetime.now(),
    'depends_on_past': False,
    'retries':0,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup':False
    }

sql="""
CREATE TABLE IF NOT EXISTS public.artists (
	artistid varchar(256) NOT NULL,
	name varchar(256),
	location varchar(256),
	lattitude numeric(18,0),
	longitude numeric(18,0)
);

CREATE TABLE IF NOT EXISTS public.songplays (
	playid varchar(32) NOT NULL,
	start_time timestamp NOT NULL,
	userid int4 NOT NULL,
	"level" varchar(256),
	songid varchar(256),
	artistid varchar(256),
	sessionid int4,
	location varchar(256),
	user_agent varchar(256),
	CONSTRAINT songplays_pkey PRIMARY KEY (playid)
);

CREATE TABLE IF NOT EXISTS public.songs (
	songid varchar(256) NOT NULL,
	title varchar(256),
	artistid varchar(256),
	"year" int4,
	duration numeric(18,0),
	CONSTRAINT songs_pkey PRIMARY KEY (songid)
);

CREATE TABLE IF NOT EXISTS public.staging_events (
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

CREATE TABLE IF NOT EXISTS public.staging_songs (
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

CREATE TABLE IF NOT EXISTS public."time" (
	start_time timestamp NOT NULL,
	"hour" int4,
	"day" int4,
	week int4,
	"month" varchar(256),
	"year" int4,
	weekday varchar(256),
	CONSTRAINT time_pkey PRIMARY KEY (start_time)
) ;

CREATE TABLE IF NOT EXISTS public.users (
	userid int4 NOT NULL,
	first_name varchar(256),
	last_name varchar(256),
	gender varchar(256),
	"level" varchar(256),
	CONSTRAINT users_pkey PRIMARY KEY (userid)
);
"""

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )


### Creating Tables in Redshift

#### Commented on reviewer's feedback - Table creation needs to be done manually before starting the DAG

# start_operator = PostgresOperator(
#                     task_id='Begin_execution',
#                     postgres_conn_id='redshift',
#                     sql=sql,
#                     dag=dag
#                 )
# ############

## Starting the DAG
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

## Load Data From S3 to Redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    s3_key="log_data",
    filetype='csv')

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    s3_key="song_data",
    filetype='json'
)

## Load Data In Fact Table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',dag=dag,
    table="songplays",
    sqlstatement=SqlQueries.songplay_table_insert
)

## Load Data In Dimension Table
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table="users",
    sqlstatement=SqlQueries.user_table_insert,
    loadtype="truncate" #truncate/update
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table="songs",
    sqlstatement=SqlQueries.song_table_insert,
    loadtype="truncate" #truncate/update
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table="artists",
    sqlstatement=SqlQueries.artist_table_insert,
    loadtype="truncate" #truncate/update    
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="time",
    sqlstatement=SqlQueries.time_table_insert,
    loadtype="truncate" #truncate/update
)

## Quality Checks to ensure tables are loading

## Added parameter for custom sql check as per reviewer's feedback
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    provide_context=True,
    tables=["artists","songplays","songs","staging_events","staging_songs","time","users"],
    custom_check={
        'test'      : 'select count(*) from songs where year > 2000',
        'expected'  : 28
    }
)

## End Operator
end_operator = DummyOperator(task_id='end_execution',  dag=dag)


start_operator>>stage_events_to_redshift
start_operator>>stage_songs_to_redshift
stage_events_to_redshift>>load_songplays_table
stage_songs_to_redshift>>load_songplays_table
load_songplays_table>>load_song_dimension_table
load_songplays_table>>load_user_dimension_table
load_songplays_table>>load_artist_dimension_table
load_songplays_table>>load_time_dimension_table
load_song_dimension_table>>run_quality_checks
load_user_dimension_table>>run_quality_checks
load_artist_dimension_table>>run_quality_checks
load_time_dimension_table>>run_quality_checks
run_quality_checks>>end_operator 
