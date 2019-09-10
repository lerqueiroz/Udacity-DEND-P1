
# Sparkify ETL
> Python ETL. Postgres Database.

##Introduction
This is the first project for the Udacity Data Engineering Nano Degree (DEND) course. We create a database (Sparkify) \
which contains 1 Fact Table and 4 Dimension Tables. The Fact Table represents the relationship between users and songs \
listened to. The Fact Table, together with the Dimension Tables allow Sparkify (the company) to analyze user behaviour. \
Examples include which users listen to what music, favorite artists, songs, total listening time, etc.
##Description of the Fact Table and Dimensions Tables (Schema Design)
###Fact Table 
songplay - contains the vast majority of information. This is essentially the log information from the log files \
subsetted to just the records that are pertinent to songs being listened to (NextSong). This subsetting on NextSong \
ripples down to all of the dimension tables that are derived from the log file. The songplay table contains \
(start_time, user_id, level, song_id, artist_id, session_id, location, and user_agent.
###Dimension Tables
song table – This is derived from the song files. Each row in the song file is parsed to create a song row that \
contains song_id, title, artist_id, year, duration. Year is stored as integer and duration as float. 
users table  - This is derived from the log file. Each row in the log file is parsed and stores user_id, first_name,  \
last_name, gender,  and level
artist table  - This is derived from the log file records. Each row in the log file is parsed and stores artist_id, \
name, location, latitude, and longitude.
time table  - This is derived from the log file records. Each row in the log file is parsed and stores start_time, \
hour, day, week, month, year, weekday.
## Usage of the Sparkify Data Warehouse
The 4 Dimension Tables are joined with the Fact Table to produce query results. An example would be the artist_table \
being joined with songplay table and answering the question who is the most popular artist in terms of number of \
song_plays. A simple join between the songplay (Fact Table) and the artists (Dimension Table) joined on artist_id \
would yield the name of the artists and a count sorted DESC, thus answering this and many other important analytical \
questions.  

## Installing / Getting started

Developed with Python version 3.7 and Postgres 10.7

Dependency library:
>Psycopg2
numpy
pandas
datetime
os
glob
json

```shell
pip install psycopg2
pip install numpy
pip install pandas
```

### Initial Configuration

Postgres database is required.
JSON files are required as input data.
Configure JSON files path is required.
Define database server, db name and credentials.

## Developing

N/A

### Building

N/A

### Deploying / Publishing

N/A

## Features

sql_queries.py -> responsible for manage DB tables. create, drop, insert, select queries \
, data types and constraints.

etl.py -> main code. It will transform json file data into Postgres tables. sql_queries.py \
file is a dependency.

create_tables -> execute first to create the initial database/tables to localhost, \
dbname=studentdb, user=student and password=student. sql_queries.py file \
is a dependency.

## Configuration

N/A

## Contributing

If you'd like to contribute, please fork the repository and use a feature
branch. Pull requests are warmly welcome.

## Links

- Udacity site: http://udacity.com/
- Developed by Leandro Queiroz: lerqueiroz@gmail.com

## Licensing

The code in this project is licensed under GPL license
04/2019

