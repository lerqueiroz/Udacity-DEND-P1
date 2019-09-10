# DROP TABLES

songplay_table_drop = ("drop table IF EXISTS songplay CASCADE")
user_table_drop = ("drop table IF EXISTS users CASCADE")
song_table_drop = ("drop table IF EXISTS songs CASCADE")
artist_table_drop = ("drop table IF EXISTS artists CASCADE")
time_table_drop = ("drop table IF EXISTS time CASCADE")

# CREATE TABLES

songplay_table_create = ("create table if not exists songplay (songplay_id SERIAL PRIMARY KEY, start_time int not null, \
user_id int not null, level varchar, song_id not null varchar, artist_id not null varchar, session_id int, \
location varchar, user_agent varchar);")

user_table_create = ("create table if not exists users (user_id int not null PRIMARY KEY, first_name varchar, \
last_name varchar, gender char, level varchar);")

song_table_create = ("create table if not exists songs (song_id varchar not null PRIMARY KEY, title not null varchar, artist_id not null varchar, \
year int, duration not null int);")

artist_table_create = ("create table if not exists artists (artist_id varchar not null PRIMARY KEY, name not null varchar, \
location varchar, latitude float, longitude float);")

time_table_create = ("create table if not exists time(start_time timestamp not null PRIMARY KEY, hour int, day int, week int, \
month int, year int, weekday int);")

# INSERT RECORDS

songplay_table_insert = ("insert into songplay (start_time, \
user_id, level, song_id, artist_id, session_id, \
location, user_agent) values (%s, %s, %s, %s, %s, %s, %s, %s)")

user_table_insert = ("insert into users (user_id, first_name, \
last_name, gender, level) values (%s, %s, %s, %s, %s) \
ON CONFLICT (user_id) DO UPDATE SET \
level = EXCLUDED.level")

song_table_insert = ("insert into songs (song_id, title, artist_id, \
year, duration) values (%s, %s, %s, %s, %s)")

artist_table_insert = ("insert into artists (artist_id, name, \
location, latitude, longitude) values (%s, %s, %s, %s, %s) \
ON CONFLICT (artist_id) DO NOTHING")

time_table_insert = ("insert into time (start_time, hour, day, week, \
month, year, weekday) values (%s, %s, %s, %s, %s, %s, %s) \
ON CONFLICT (start_time) DO NOTHING")

# FIND SONGS

song_select = ("SELECT s.song_id, a.artist_id \
FROM songs s LEFT JOIN artists a ON (s.artist_id = a.artist_id) \
WHERE s.title = (%s) \
AND a.name = (%s) \
AND s.duration = (%s)")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
