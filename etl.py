import pandas as pd
import os
import psycopg2
from sql_quries import *

def log_songs_files(filepath):
    '''
    search for all json files and append the log files to log_files list &
    songs files to songs_files list and return them
    :param filepath: filepath to the data
    :return: log_files , songs_files
    '''
    log_files = []
    songs_files = []
    for (root, dirs, files) in os.walk(filepath, topdown=True):
        if len(files) != 0:
            if 'log_data' in root:
                for i in files:
                    log_files.append(os.path.join(root, i))

            if 'song_data' in root:
                for i in files:
                    songs_files.append(os.path.join(root, i))

    return log_files , songs_files

def processing_songs_files(cur , songs_files):
    '''
    Extract songs data & artists data to load them into songs and artists Dimension tables
    :param cur: cursor to execute the queries in the database
    :param songs_files: directories to songs json files
    '''
    for json_file in songs_files:
        df = pd.read_json(json_file, lines=True)
        song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
        insert_into_songs(cur, tuple(song_data))
        artist_data = df[['artist_id', 'artist_name', 'artist_location',\
                          'artist_latitude', 'artist_longitude']].values[0].tolist()
        insert_into_artists(cur, tuple(artist_data))

def processing_log_files(cur , log_files):
    '''
    Extract song plays , time data & users data from the log files ,transform time data to
    have 'start_time' , 'hour' , 'day' , 'week' , 'month' , 'year' , 'weekday' columns
    then insert the data to time Dimension table  , users Dimension table  &
    songplays Fact table
    :param cur: cursor to execute the queries in the database
    :param log_files: directories to logs json files
    '''
    for json_file in log_files:
        df = pd.read_json(json_file, lines=True)
        df = df[df['page'] == 'NextSong']
        df['ts'] = pd.to_datetime(df['ts'], unit='ms')

        #Transforming the ts table to create new time tables
        time_data = (df['ts'], df['ts'].dt.hour, df['ts'].dt.day,df['ts'].dt.isocalendar().week, df['ts'].dt.month,\
                     df['ts'].dt.year, df['ts'].dt.weekday)
        cols_names = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
        time_df = pd.DataFrame(columns=cols_names)
        for idx, col_name in enumerate(cols_names):
            time_df[col_name] = time_data[idx]

        for _, row in time_df.iterrows():
            insert_into_time(cur, tuple(row.tolist()))

        user_data = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
        for _, row in user_data.iterrows():
            insert_into_users(cur, tuple(row.tolist()))

        for _, row in df.iterrows():
            #Find the song and artist ids related to this row
            song_artist_ids = song_select(cur, row['song'], row['length'], row['artist'])

            if song_artist_ids:
                song_id, artist_id = song_artist_ids
            else:

                song_id, artist_id = None, None

            values = (row['ts'], row['userId'], row['level'], song_id, artist_id, row['sessionId'], row['location'],
                      row['userAgent'])
            insert_into_songplays(cur, values)


def connect_to_db():
    '''
    connect to postgres
    :return:cursor to execute the queries in the database & connection attribute to close
    the connection after finishing the process
    '''
    try:
        conn = psycopg2.connect("dbname = sparkifydb user = postgres password=")
    except psycopg2.Error as e:
        print("Couldnot connect to database")
        print(e)

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Couldnot get cursor")
        print(e)

    conn.set_session(autocommit=True)

    return cur , conn
def main():
    cur , conn = connect_to_db()


    log_files , songs_files = log_songs_files('data')

    processing_songs_files(cur , songs_files)
    processing_log_files(cur , log_files)

    cur.close()
    conn.close()

if __name__ == '__main__':
    main()





