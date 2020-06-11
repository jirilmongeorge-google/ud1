import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from datetime import datetime


def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    step1 = df[["song_id", "title", "artist_id", "year", "duration"]]
    song_data = step1.values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    df.where(pd.notnull(df), None)
    step2 = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]] 
    artist_data = step2.values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong'] 

    # convert timestamp column to datetime
    t = df['ts'].apply(lambda x: (pd.Timestamp(pd.to_datetime(x, unit='ms')), 
                             pd.Series(pd.to_datetime(x, unit='ms')).dt.hour[0], pd.Series(pd.to_datetime(x, unit='ms')).dt.day[0]
                             , pd.Series(pd.to_datetime(x, unit='ms')).dt.weekofyear[0], pd.Series(pd.to_datetime(x, unit='ms')).dt.month[0]
                             , pd.Series(pd.to_datetime(x, unit='ms')).dt.year[0], pd.Series(pd.to_datetime(x, unit='ms')).dt.weekday[0]))
    
    # insert time data records
    time_data = t.tolist()
    pd.unique(time_data)
    column_labels = ["timestamp", "hour", "day", "week of year", "month", "year", "weekday"]
    time_df = pd.DataFrame(time_data, columns = column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]] 

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.Timestamp(pd.to_datetime(row.ts, unit='ms')), row.userId, row.level, songid, artistid,  row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
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