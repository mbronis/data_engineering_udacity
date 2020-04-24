import configparser


# CONFIG

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

{"artist":"Stephen Lynch","auth":"Logged In",
 "firstName":"Jayden","gender":"M","itemInSession":0,"lastName":"Bell","length":182.85669,"level":"free","location":"Dallas-Fort Worth-Arlington, TX","method":"PUT","page":"NextSong","registration":1540991795796.0,"sessionId":829,"song":"Jim Henson's Dead","status":200,"ts":1543537327796,"userAgent":"Mozilla\/5.0 (compatible; MSIE 10.0; Windows NT 6.2; WOW64; Trident\/6.0)","userId":"91"}

staging_events_table_create = """\
CREATE TABLE IF NOT EXISTS staging_events (\
    artist VARCHAR, \
    auth VARCHAR, \
    firstName VARCHAR, \
    gender VARCHAR, \
    itemInSession INT, \
    lastName VARCHAR, \
    length NUMERIC, \
    level VARCHAR, \
    location VARCHAR, \
    method VARCHAR, \
    page VARCHAR, \
    registration NUMERIC, \
    sessionId INT, \
    song VARCHAR, \
    status INT, \
    ts BIGINT, \
    userAgent VARCHAR, \
    userId VARCHAR \
);\
"""

staging_songs_table_create = """\
CREATE TABLE IF NOT EXISTS staging_songs (\
    num_songs INT, \
    artist_id VARCHAR NOT NULL, \
    artist_latitude NUMERIC, \
    artist_longitude NUMERIC, \
    artist_location VARCHAR, \
    artist_name VARCHAR, \
    song_id VARCHAR PRIMARY KEY, \
    title VARCHAR NOT NULL, \
    duration NUMERIC, \
    year INT \
);\
"""

songplay_table_create = ("""
""")

user_table_create = ("""
""")

song_table_create = ("""
""")

artist_table_create = ("""
""")

time_table_create = ("""
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

#create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
# copy_table_queries = [staging_events_copy, staging_songs_copy]
# insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

create_table_queries = [staging_songs_table_create, staging_events_table_create]
copy_table_queries = [staging_songs_copy, staging_events_table_create]
insert_table_queries = []
