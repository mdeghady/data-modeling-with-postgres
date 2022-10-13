import psycopg2
drop_songplays = "DROP TABLE IF EXISTS songplays"
drop_users = "DROP TABLE IF EXISTS users"
drop_songs = "DROP TABLE IF EXISTS songs"
drop_artists = "DROP TABLE IF EXISTS artists"
drop_time = "DROP TABLE IF EXISTS time"

create_users = ("""CREATE TABLE IF NOT EXISTS users (
                    user_id int PRIMARY KEY,
                    first_name varchar NOT NULL,
                    last_name varchar NOT NULL,
                    gender varchar(1) NOT NULL,
                    level varchar NOT NULL)""")

create_songs = ("""CREATE TABLE IF NOT EXISTS songs(
                    song_id varchar PRIMARY KEY,
                    title varchar NOT NULL,
                    artist_id varchar NOT NULL,
                    year int NOT NULL,
                    duration numeric NOT NULL)""")

create_artists = ("""CREATE TABLE IF NOT EXISTS artists(
                    artist_id varchar PRIMARY KEY,
                    name varchar NOT NULL,
                    location varchar NOT NULL,
                    latitude numeric,
                    longitude numeric)""")

create_time = ("""CREATE TABLE IF NOT EXISTS time(
                    start_time timestamp PRIMARY KEY,
                    hour int NOT NULL,
                    day int NOT NULL,
                    week int NOT NULL,
                    month int NOT NULL,
                    year int NOT NULL,
                    weekday int NOT NULL)""")

create_songplays = ("""CREATE TABLE IF NOT EXISTS songplays(
                        songplay_id serial PRIMARY KEY,
                        start_time timestamp NOT NULL,
                        user_id int NOT NULL,
                        level varchar NOT NULL,
                        song_id varchar ,
                        artist_id varchar ,
                        session_id int NOT NULL,
                        location varchar NOT NULL,
                        user_agent varchar NOT NULL,
                        CONSTRAINT FK_user_id FOREIGN KEY (user_id) REFERENCES users (user_id),
                        CONSTRAINT FK_song_id FOREIGN KEY (song_id) REFERENCES songs (song_id),
                        CONSTRAINT FK_artist_id FOREIGN KEY (artist_id) REFERENCES artists (artist_id),
                        CONSTRAINT FK_start_time FOREIGN KEY (start_time) REFERENCES time (start_time)
                        )""")

create_table_quries = [create_users , create_songs , create_artists , create_time , create_songplays]
drop_table_quries = [drop_users , drop_songs , drop_artists , drop_time , drop_songplays]


def insert_into_songs(cur , values):
    query = """INSERT INTO songs(song_id,title , artist_id , year , duration)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (song_id)
                DO NOTHING;"""

    try:
        cur.execute(query , values)
    except psycopg2.Error as e:
        print("cannot insert data into songs table ")
        print(e)

def insert_into_artists(cur , values):
    query = """INSERT INTO artists(artist_id, name, location, latitude, longitude)
                    VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT (artist_id)
                    DO NOTHING;"""

    try:
        cur.execute(query, values)
    except psycopg2.Error as e:
        print("cannot insert data into artists table ")
        print(e)

def insert_into_time(cur , values):
    query = """INSERT INTO time(start_time, hour, day, week, month, year, weekday)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (start_time)
                        DO NOTHING;"""
    try:
        cur.execute(query, values)
    except psycopg2.Error as e:
        print("cannot insert data into time table ")
        print(e)

def insert_into_users(cur , values):
    query = """INSERT INTO users(user_id, first_name, last_name, gender, level)
                    VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT (user_id)
                    DO NOTHING;"""

    try:
        cur.execute(query, values)
    except psycopg2.Error as e:
        print("cannot insert data into users table ")
        print(e)

def song_select(cur , song , duration , artist):
    query = """SELECT s.song_id , a.artist_id FROM songs s
                INNER JOIN artists a USING(artist_id)
                    WHERE s.title = %s  AND s.duration = %s AND a.name = %s"""
    try:
        cur.execute(query, (song, duration , artist))
    except psycopg2.Error as e:
        print("cannot get the song_id")
        print(e)
    song_artist_ids = cur.fetchone()

    return song_artist_ids

def insert_into_songplays(cur , values):
    query = """INSERT INTO songplays( start_time, user_id, level, song_id, artist_id, session_id,
                location, user_agent)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s);"""

    try:
        cur.execute(query, values)
    except psycopg2.Error as e:
        print("cannot insert data into songplays table ")
        print(e)

def select_all(cur , table_name):
    query = f"SELECT * FROM {table_name}"

    try:
        cur.execute(query)
    except psycopg2.Error as e:
        print(f"cannot select all rows from {table_name}")
        print(e)

    row = cur.fetchone()
    while row:
        print(row)
        row = cur.fetchone()
