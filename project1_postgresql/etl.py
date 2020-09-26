import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Description: This function can be used to read the file in the filepath (data/song_data)
    to get the song and artist info and used to populate the song and artist dim tables.

    Arguments:
        cur: the cursor object. 
        filepath: song data file path. 

    Returns:
        None
    """
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data = [list(row) for row in song_data.itertuples(index=False)]
    song_data = song_data[0]
    print(song_data[0])

    cur.execute(song_table_insert, (song_data[0],song_data[1],song_data[2],int(song_data[3]),song_data[4]))
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_data= [list(row) for row in artist_data.itertuples(index=False)][0]

    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Description: This function can be used to read the file in the filepath (data/log_data)
    to get the user and time info and used to populate the users and time dim tables.

    Arguments:
        cur: the cursor object. 
        filepath: log data file path. 

    Returns:
        None
    """
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    #df = 

    # convert timestamp column to datetime
    df['ts']=pd.to_datetime(df['ts'], unit='ms')
    t =  df[['ts']]
    t.columns=['start_time']
    
    t['hour']=t['start_time'].dt.hour
    t['day']=t['start_time'].dt.day
    t['week']=t['start_time'].dt.week
    t['month']=t['start_time'].dt.month
    t['year']=t['start_time'].dt.year
    t['weekday']=t['start_time'].dt.weekday
    
    # insert time data records
    time_data = [list(row) for row in t.itertuples(index=False)]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = t

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        if row[0]=='':
            continue
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
            
        if row.userId =='':
            continue

        # insert songplay record
       
        songplay_data = (row.ts,row.userId,row.level, songid, artistid,row.sessionId,row.location,row.userAgent)
        
        
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Description: This function can be used to get all files matching extension from directory
    

    Arguments:
        cur: the cursor object. 
        filepath: data file path. 
        func: the function to process data

    Returns:
        None
    """
    
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()