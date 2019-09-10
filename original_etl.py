import os
import glob
import psycopg2
import pandas as pd
import json
import numpy
from sql_queries import *
import datetime

def get_file(filepath):
    """Retrieve a list of data from json files directory.

    Args:
        filepath (string): Path to look for json files.

    Returns:
        all_jsons (list): json data appended.

    """
    all_jsons = []
    # create files paths list
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            # merge json files
            with open(f, "rb") as infile:
                # iteration each line
                for line in infile:
                    line = json.loads(line.strip())
                    all_jsons.append(line)
    num_lines = len(all_jsons)
    print('{} objects found in {}'.format(num_lines, filepath))
    return all_jsons

def process_song_file(cur, datafile):
    """Do some data quality control and populate song table and artist table.

    Args:
        cur (cursor): Database connection object.
        datafile (list): json data to be persisted on the database.

    """
    # open song file
    df = pd.DataFrame(datafile)

    # Quality control
    # replacing empty string to NaN
    df['song_id'] = df['song_id'].replace('', numpy.nan)
    df['artist_id'] = df['artist_id'].replace('', numpy.nan)
    # removing row with NaN values (not null atributes)
    df = df.dropna(axis=0, subset=['song_id'])
    df = df.dropna(axis=0, subset=['artist_id'])

    # select columns tables in dataframe
    df_song = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    df_artist = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]

    # convert dataframe to list
    songs = [list(row) for row in df_song.itertuples(index=False)]
    artists = [list(row) for row in df_artist.itertuples(index=False)]

    # inserts
    try:
        for row in songs:
            cur.executemany(song_table_insert, (row,))
    except psycopg2.Error as e:
        print("Error inserting in song table")
        print(row)
        print(e)
    print("song ok")

    try:
        for row in artists:
            cur.executemany(artist_table_insert, (row,))
    except psycopg2.Error as e:
        print("Error inserting in artist table")
        print(row)
        print(e)
    print("artist ok")

def process_log_file(cur, datafile):
    """Do some data quality control and populate time table, user table and song_play table. Convert date format.

    Args:
        cur (cursor): Database connection object.
        datafile (list): json data to be persisted on the database.

    """
    # open song file
    df = pd.DataFrame(datafile)

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')

    # new dataframe
    dates_df = pd.DataFrame(columns=['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday'])
    # extracting dates
    dates_df['start_time'] = pd.DatetimeIndex(df['ts'])
    dates_df['hour'] = pd.DatetimeIndex(df['ts']).hour
    dates_df['day'] = pd.DatetimeIndex(df['ts']).day
    dates_df['week'] = pd.DatetimeIndex(df['ts']).weekofyear
    dates_df['month'] = pd.DatetimeIndex(df['ts']).month
    dates_df['year'] = pd.DatetimeIndex(df['ts']).year
    dates_df['weekday'] = pd.DatetimeIndex(df['ts']).weekday

    # Quality control. removing row with empty userId
    # replacing empty string to NaN
    df['userId'] = df['userId'].replace('', numpy.nan)
    df['ts'] = df['ts'].replace('', numpy.nan)
    # removing row with NaN values (not null atributes)
    df = df.dropna(axis=0, subset=['userId'])
    df = df.dropna(axis=0, subset=['ts'])

    # select columns tables in dataframe
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    songplay_df = df[['ts', 'userId', 'level', 'sessionId', 'location', 'userAgent', 'artist', 'song', 'length']]
    songplay_df['ts'] = pd.DatetimeIndex(df['ts'])

    # convert dataframe to list
    dates = [list(row) for row in dates_df.itertuples(index=False)]
    users = [list(row) for row in user_df.itertuples(index=False)]

    # insert time data records
    try:
        for row in dates:
            cur.executemany(time_table_insert, (row,))
    except psycopg2.Error as e:
        print("Error inserting in time table")
        print(row)
        print(e)
    print("time ok")

    # insert user records
    try:
        for row in users:
            cur.executemany(user_table_insert, (row,))
    except psycopg2.Error as e:
        print("Error inserting in user table")
        print(row)
        print(e)
    print("user ok")

    # insert songplay records
    try:
        for index, row in songplay_df.iterrows():
            # get songId and artistId from song and artist tables
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()
    except psycopg2.Error as e:
        print("Error selecting in song table")
        print(row)
        print(e)

        if results:
            songid, artistid = results
            try:
                # insert songplay record
                cur.execute(songplay_table_insert, (row.ts, row.userId, row.level, songid, artistid, \
                                                    row.sessionId, row.location, row.userAgent))
            except psycopg2.Error as e:
                print("Error inserting in songplay table")
                print(row)
                print(e)
        else:
            songid, artistid = None, None
        print(results)
    print("songplay ok")

def process_data(cur, conn, filepath, func):
    """Generate the datafile and pass it to apropriate function.

    Args:
        cur (cursor): Database connection object.
        conn (connection): Database connection object.
        filepath (string): Path to look for json files.
        func (string): function to call.

    """
    # get data from files in filepath
    datafile = get_file(filepath)
    func(cur, datafile)

def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    # conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=superuser")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    # process_data(cur, conn, filepath='C:\\data\\song_data', func=process_song_file)
    # process_data(cur, conn, filepath='C:\\data\\log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()