import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# CREATE SCHEMAS
fact_schema= ("CREATE SCHEMA IF NOT EXISTS fact_tables")
dimension_schema= ("CREATE SCHEMA IF NOT EXISTS dimension_tables")
staging_schema= ("CREATE SCHEMA IF NOT EXISTS staging_tables")

# DROP SCHEMAS
fact_schema_drop= ("DROP SCHEMA IF EXISTS fact_tables CASCADE")
dimension_schema_drop= ("DROP SCHEMA IF EXISTS dimension_tables CASCADE")
staging_schema_drop= ("DROP SCHEMA IF EXISTS staging_tables CASCADE")


# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"

#Fact Table
songplay_table_drop = "DROP TABLE IF EXISTS fact_tables.songplays"

#Dimension Tables
user_table_drop = "DROP TABLE IF EXISTS dimension_tables.users "
song_table_drop = "DROP TABLE IF EXISTS dimension_tables.songs "
artist_table_drop = "DROP TABLE IF EXISTS dimension_tables.artists "
time_table_drop = "DROP TABLE IF EXISTS dimension_tables.time "

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_tables.staging_events 
(
     artist varchar(200)
     ,auth varchar(200)
     ,firstName varchar(100)
     ,gender Char(1)
     ,itemInSession INT
     ,lastName varchar(100)
     ,length varchar(50)
     ,level varchar(25)
     ,location varchar(200)
     ,method varchar(25)
     ,page  varchar(100)
     ,registration varchar(50)
     ,sessionId INT
     ,song varchar(200)
     ,status INT
     ,ts BIGINT 
     ,userAgent varchar(200)
     ,userId INT
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_tables.staging_songs 
(
    num_songs INT
    ,artist_id varchar(50)
    ,artist_latitude varchar(25)
    ,artist_longitude varchar(25)
    ,artist_location varchar(200)
    ,artist_name varchar(200)
    ,song_id varchar(50)
    ,title varchar(200)
    ,duration DECIMAL (10,5)
    ,year INT
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_tables.songplays 
(
    songplay_id BIGINT IDENTITY(1,1) PRIMARY KEY
    ,start_time varchar(30)
    ,user_id varchar(50)
    ,level varchar(25)
    ,song_id varchar(50)
    ,artist_id varchar(50)
    ,session_id INT
    ,location varchar(100)
    ,user_agent varchar(200)
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dimension_tables.users 
(
    user_id INT PRIMARY KEY
    ,first_name varchar(100)
    ,last_name varchar(100)
    ,gender char(1)
    ,level varchar(25)
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dimension_tables.songs 
(
    song_id varchar(50) PRIMARY KEY
    ,title varchar(200)
    ,artist_id varchar(50)
    ,year INT
    ,duration DECIMAL (10,5)
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dimension_tables.artists 
(
     artist_id varchar(50) PRIMARY KEY
    , name varchar(200)
    , location varchar(200)
    , latitude varchar(25)
    , longitude varchar(25)
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dimension_tables.time 
(
    start_time TIMESTAMP PRIMARY KEY
    , hour int
    , day int
    , week int
    , month int
    , year int
    , weekday int
)
""")

# STAGING TABLES

#read it in you file sql_queries.py
config = configparser.ConfigParser()
config.read('dwh.cfg')
IAM = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']

staging_events_copy = ("""copy staging_tables.staging_events 
                            from {}
                            iam_role {}
                            json {}
                        """).format(LOG_DATA, IAM, LOG_JSONPATH)

staging_songs_copy = (""" copy staging_tables.staging_songs
                            from {}
                            iam_role {}
                            json 'auto'
                      """).format(SONG_DATA, IAM)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO fact_tables.songplays(start_time,user_id, level,song_id,artist_id,session_id,location,user_agent)
SELECT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,userid,level,song_id, artist_id, sessionid, location, useragent
FROM staging_tables.staging_events AS a
INNER JOIN staging_tables.staging_songs AS b
    ON b.artist_name = a.artist
""")

user_table_insert = ("""
INSERT INTO dimension_tables.users (user_id, first_name, last_name, gender, level)
SELECT userId, firstName, lastName, gender, level
FROM staging_tables.staging_events 
WHERE userId IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO dimension_tables.songs (song_id, title, artist_id, year, duration)
SELECT song_id, title, artist_id, year, duration
FROM staging_tables.staging_songs
""")

artist_table_insert = ("""
INSERT INTO dimension_tables.artists (artist_id, name, location, latitude, longitude)
SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_tables.staging_events AS a
INNER JOIN staging_tables.staging_songs AS b
    ON b.artist_name = a.artist
""")

time_table_insert = ("""
INSERT INTO dimension_tables.time (start_time, hour, day, week, month, year, weekday)
select
TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time
,DATE_PART(day, TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')as day
,DATE_PART(hour, TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')as hour
,DATE_PART(month, TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')as month
,DATE_PART(week, TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')as week
,DATE_PART(dayofweek, TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')as weekday
,DATE_PART(yrs,TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')as year
from staging_tables.staging_events
""")

# QUERY LISTS

create_schemas_queries = [fact_schema, dimension_schema, staging_schema]
drop_schemas_queries = [fact_schema, dimension_schema, staging_schema]

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]

