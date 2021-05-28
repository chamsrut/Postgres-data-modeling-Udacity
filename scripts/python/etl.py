import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Function parsing the .json song_data using pandas.read_json method and executing the queries: song_table_insert,
    artist_table_insert (contained in sql_queries.py file).

    INPUTS:
    cur = cursor to perform database operations
    filepath = filepath to the song_data
    """

    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = list(
        df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Function parsing the .json log_data using pandas.read_json method, filtering the data by "NextSong" action,
    converting timestamp column to datetime and executing the queries: time_table_insert, user_table_insert,
    songplay_table_insert (contained in sql_queries.py file).

    INPUTS:
    cur = cursor to perform database operations
    filepath = filepath to the song_data
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert gender and level to dummies, I write them to the db as a boolean type
    # since these are binary variables in our dataset
    df = pd.concat([df, (pd.get_dummies(df.gender, prefix='gender_dummy') == True)], axis=1)
    df = pd.concat([df, (pd.get_dummies(df.level, prefix='level_dummy') == True)], axis=1)

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit='ms')

    # insert time data records
    time_data = list(pd.concat([t.dt.time, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday],
                               axis=1).values)
    column_labels = ['timestamp', 'hour', 'day', 'week_of_year', 'month', 'year', 'week_day']
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender_dummy_F', 'level_dummy_free']]

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
        songplay_data = (index, row.ts, row.userId, row.level_dummy_free, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Function performing the population of tables for all data contained in data directory

    INPUTS:
    cur = cursor to perform database operations
    conn = connection object
    filepath = filepath to the song_data
    func = function that performs data pre-processing and data insertion. In this case it will be one of:
    process_song_file, process_log_file
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
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
    """
    Main function performing:
    - connection to the database
    - pre-processing and population of the tables
    - closing connection to the db after populating the tables
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
