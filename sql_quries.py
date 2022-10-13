
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
                    song_id int PRIMARY KEY,
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
                        song_id int NOT NULL,
                        artist_id varchar NOT NULL,
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