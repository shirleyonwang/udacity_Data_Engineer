# description about the project
<p>this database is to store song,artists,time and log.</p>
the project is used to practise the sql query in postgres. create table, select table and drop table.

# database schema design 
it includes below tables</p>
1.songplays</br>
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
                            

2.users</br>
 * user_id integer PRIMARY KEY, 
 * first_name varchar, 
 * last_name varchar, 
 * gender varchar, 
 * evel varchar
 
3.songs</br>
 * song_id varchar PRIMARY KEY, 
 * title varchar, 
 * artist_id varchar, 
 * year integer, 
 * duration float
                    
4.artists</br>
 * artist_id varchar  PRIMARY KEY, 
 * name varchar, 
 * location varchar, 
 * latitude float, 
 * longitude float
                            
5.time</br>
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
![songplay](https://github.com/shirleyonwang/udacity_Data_Engineer/blob/master/project1_postgresql/image/songplay.png)
* users</br>
![users](https://github.com/shirleyonwang/udacity_Data_Engineer/blob/master/project1_postgresql/image/user.PNG)
* songs</br>
![songs](https://github.com/shirleyonwang/udacity_Data_Engineer/blob/master/project1_postgresql/image/song.PNG)
* artists</br>
![artists](https://github.com/shirleyonwang/udacity_Data_Engineer/blob/master/project1_postgresql/image/artists.PNG)
* time</br>
![time](https://github.com/shirleyonwang/udacity_Data_Engineer/blob/master/project1_postgresql/image/time.PNG)
