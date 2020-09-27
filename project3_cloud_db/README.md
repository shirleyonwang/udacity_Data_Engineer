# description about the project
the project is used to build an ETL pipeline that extracts data from S3, stages them in Redshift, and transforms data into a set of dimensional tables.

# database schema design 
it includes below tables</p>
* staging_events
 * artist    varchar,
 * auth      varchar,
 * firstName varchar,
 *  gender    varchar,
 * ItemInSession integer, 
 * lastName  varchar,
 * length    FLOAT,
 * level     varchar,
 * location  varchar,
 * method    varchar,
 * page      varchar,
 * registration  FLOAT,
 * sessionId  integer, 
 * song       varchar,
 * status     integer, 
 * ts         TIMESTAMP,
 * userAgent  varchar,
 * userId     INTEGER 

* staging_songs
 * num_songs        integer, 
 * artist_id        varchar , 
 * artist_latitude  float, 
 * artist_longitude float,
 * artist_location  varchar, 
 * artist_name      varchar, 
 * song_id          varchar, 
 * title            varchar, 
 * duration         FLOAT,
 * year             integer     

* songplays</br>
 * songplay_id bigint  identity(0, 1), 
 * start_time          varchar NOT NULL, 
 * user_id             integer NOT NULL, 
 * level               varchar, 
 * song_id             varchar, 
 * artist_id           varchar, 
 * session_id          integer, 
 * location            varchar, 
 * user_agent          varchar,
 * PRIMARY KEY(songplay_id,start_time,user_id)
                            

* users</br>
 * user_id    integer PRIMARY KEY, 
 * first_name varchar, 
 * last_name  varchar, 
 * gender     varchar, 
 * level      varchar
 
* songs</br>
 * song_id   varchar PRIMARY KEY, 
 * title     varchar, 
 * artist_id varchar, 
 * year      integer, 
 * duration  float
                    
* artists</br>
 * artist_id varchar  PRIMARY KEY, 
 * name      varchar, 
 * location  varchar, 
 * latitude  float, 
 * longitude float
                            
* time</br>
 * start_time varchar  PRIMARY KEY, 
 * hour    integer, 
 * day     integer, 
 * week    integer, 
 * month   integer, 
 * year    integer, 
 * weekday integer

# ETL pipeline
1. extract data from S3, and copy to related tables:staging_events and staging_songs
2. distribute data from staging_events and staging_songs to tables: songplays,users,songs,artists,time
3. select data from redshift and check whether data is successfully extracted.

# How to run the scripts.
1. Run create_tables.py
2. Run etl.py
also you can run projects_all_code.ipynb

# Result tables

* songplays</br>
![avatar](image/songplay.PNG)
* users</br>
![avatar](image/user.PNG)
* songs</br>
![avatar](image/song.PNG)
* artists</br>
![avatar](image/artists.PNG)
* time</br>
![avatar](image/time.PNG)