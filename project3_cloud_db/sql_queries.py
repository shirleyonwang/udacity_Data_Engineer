import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table IF EXISTS staging_events"
staging_songs_table_drop = "drop table IF EXISTS staging_songs"
songplay_table_drop = "drop table IF EXISTS songplays"
user_table_drop = "drop table IF EXISTS users"
song_table_drop = "drop table IF EXISTS songs"
artist_table_drop = "drop table IF EXISTS artists"
time_table_drop = "drop table IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
                                artist    varchar,
                                auth      varchar,
                                firstName varchar,
                                gender    varchar,
                                ItemInSession integer, 
                                lastName  varchar,
                                length    FLOAT,
                                level     varchar,
                                location  varchar,
                                method    varchar,
                                page      varchar,
                                registration  FLOAT,
                                sessionId  integer, 
                                song       varchar,
                                status     integer, 
                                ts         TIMESTAMP,
                                userAgent  varchar,
                                userId     INTEGER 
                        )
                        """)

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
                                num_songs        integer, 
                                artist_id        varchar , 
                                artist_latitude  float, 
                                artist_longitude float,                                
                                artist_location  varchar, 
                                artist_name      varchar, 
                                song_id          varchar, 
                                title            varchar, 
                                duration         FLOAT,
                                year             integer                               
                        )
                        """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
                                songplay_id bigint  identity(0, 1), 
                                start_time          varchar NOT NULL, 
                                user_id             integer NOT NULL, 
                                level               varchar, 
                                song_id             varchar, 
                                artist_id           varchar, 
                                session_id          integer, 
                                location            varchar, 
                                user_agent          varchar,
                                PRIMARY KEY(songplay_id,start_time,user_id)
                        )
                        """)


user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
                            user_id    integer PRIMARY KEY, 
                            first_name varchar, 
                            last_name  varchar, 
                            gender     varchar, 
                            level      varchar
                        )
                        """)


song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
                        song_id   varchar PRIMARY KEY, 
                        title     varchar, 
                        artist_id varchar, 
                        year      integer, 
                        duration  float
                    )
                    """)


artist_table_create = ("""CREATE TABLE IF NOT EXISTS  artists(
                                artist_id varchar  PRIMARY KEY, 
                                name      varchar, 
                                location  varchar, 
                                latitude  float, 
                                longitude float
                            )
                            """)


time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
                            start_time varchar  PRIMARY KEY, 
                            hour    integer, 
                            day     integer, 
                            week    integer, 
                            month   integer, 
                            year    integer, 
                            weekday integer
                        )
                        """)



# STAGING TABLES

LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'
ARN='arn:aws:iam::226792442351:role/myRedshiftRole'

staging_events_copy = ("""
            copy staging_events 
            from '{}' 
            credentials 'aws_iam_role={}'
            json '{}'
            region 'us-west-2'
            TIMEFORMAT 'epochmillisecs';
        """).format(LOG_DATA,ARN,LOG_JSONPATH)

staging_songs_copy = ("""
            copy staging_songs 
            from '{}' 
            credentials 'aws_iam_role={}'
            json 'auto'
            region 'us-west-2' ;
        """).format(SONG_DATA,ARN)



songplay_table_insert = ("""INSERT INTO songplays 
                        ( start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
                        select distinct to_char(a.ts,'dd-mm-yyyy hh24:mi:ss') , a.userId, 
                                a.level,b.song_id,artist_id, a.sessionId,a.location,a.userAgent
                        from  staging_events a, staging_songs b 
                        where a.artist=b.artist_name 
                                and a.song=b.title 
                                and a.location=b.artist_location 
                                and a.length=b.duration""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)  
                        select distinct a.userId,a.firstName,a.lastName,a.gender,a.level
                        from staging_events a  
                        where a.userId is not null
                        """ )

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)  
                        select distinct song_id,title,artist_id,year, duration
                        from staging_songs 
                        """ )

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
                        select distinct artist_id, artist_name, artist_location, 
                                artist_latitude, artist_longitude
                        from staging_songs 
                        """ )


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)  
                        SELECT  to_char(ts,'dd-mm-yyyy hh24:mi:ss')  ,EXTRACT(HOUR FROM ts), 
                                EXTRACT(DAY FROM ts), EXTRACT(WEEK FROM ts), 
                                EXTRACT(MONTH FROM ts), EXTRACT(YEAR FROM ts), EXTRACT(WEEKDAY FROM ts)
                        FROM staging_events 
                        """ )


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

