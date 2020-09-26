# description about the project
<p>this database is to store song,artists,time and log.</p>
the project is used to practise the sql query in postgres. create table, select table and drop table.

# database schema design 
it includes below tables</p>
* songplays</br>
 * songplay_id SERIAL , 
 * start_time varchar NOT NULL, 
 * user_id integer NOT NULL, 
 * level varchar, 
 * song_id varchar, 
 * artist_id varchar, 
 * session_id integer, 
 * location varchar, 
 * user_agent varchar,
 * PRIMARY KEY(songplay_id,start_time,user_id)
                            

* users</br>
 * user_id integer PRIMARY KEY, 
 * first_name varchar, 
 * last_name varchar, 
 * gender varchar, 
 * evel varchar
 
* songs</br>
 * song_id varchar PRIMARY KEY, 
 * title varchar, 
 * artist_id varchar, 
 * year integer, 
 * duration float
                    
* artists</br>
 * artist_id varchar  PRIMARY KEY, 
 * name varchar, 
 * location varchar, 
 * latitude float, 
 * longitude float
                            
* time</br>
 * start_time varchar  PRIMARY KEY, 
 * hour integer, 
 * day integer, 
 * week integer, 
 * month integer, 
 * year integer, 
 * weekday integer

# ETL pipeline
read files and extract to song, aritis and log<\p>
when inserting to songplay or user table, if user id is null, then ignore.


# How to run the scripts.
1. Run create_tables.py
2. Run etl.py
3. Run test.ipynb to check the database result

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