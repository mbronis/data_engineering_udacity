import configparser

aws_config = 'configs/aws.cfg'

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

    
staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_events (
    artist        VARCHAR,
    auth          VARCHAR,
    firstName     VARCHAR,
    gender        VARCHAR,
    itemInSession INT,
    lastName      VARCHAR,
    length        NUMERIC,
    level         VARCHAR,
    location      VARCHAR,
    method        VARCHAR,
    page          VARCHAR,
    registration  NUMERIC,
    sessionId     INT,
    song          VARCHAR,
    status        INT,
    ts            BIGINT,
    userAgent     VARCHAR,
    userId        VARCHAR
);
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs        INT,
    artist_id        VARCHAR,
    artist_latitude  NUMERIC,
    artist_longitude NUMERIC,
    artist_location  VARCHAR,
    artist_name      VARCHAR,
    song_id          VARCHAR,
    title            VARCHAR,
    duration         NUMERIC,
    year             INT
);
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT        IDENTITY(1, 1) PRIMARY KEY,
    start_time  TIMESTAMP,
    user_id     VARCHAR    NOT NULL,
    level       VARCHAR,
    song        VARCHAR,
    song_id     VARCHAR    SORTKEY DISTKEY,
    artist_id   VARCHAR,
    session_id  INT        NOT NULL,
    location    VARCHAR,
    user_agent  VARCHAR
);
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs (
    song_id   VARCHAR    PRIMARY KEY SORTKEY DISTKEY,
    title     VARCHAR,
    artist_id VARCHAR    NOT NULL,
    year      INT,
    duration  NUMERIC
);
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users (
    user_id    VARCHAR    PRIMARY KEY SORTKEY,
    first_name VARCHAR,
    last_name  VARCHAR,
    gender     VARCHAR,
    level      VARCHAR
)
DISTSTYLE ALL;
"""

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR    PRIMARY KEY SORTKEY,
    name      VARCHAR,
    location  VARCHAR,
    latitude  INT,
    longitude INT
)
DISTSTYLE ALL;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP    PRIMARY KEY SORTKEY,
    hour       INT,
    day        INT,
    week       INT,
    month      INT,
    year       INT,
    weekday    INT
)
DISTSTYLE ALL;
""")

# STAGING TABLES

# get arn for role to read from S3
config = configparser.ConfigParser()
config.read_file(open(aws_config))
cl_iam_arn = config.get("CLUSTER", "cl_iam_arn")
    
staging_songs_copy = """
    copy staging_songs 
    from 's3://udacity-dend/song_data/A/A/A/TR' 
    credentials 'aws_iam_role={}'
    json 'auto'
    region 'us-west-2'
    ;
    """.format(cl_iam_arn)

staging_events_copy = """
    copy staging_events 
    from 's3://udacity-dend/log_data/2018/11/2018-11-01' 
    credentials 'aws_iam_role={}'
    json 'auto'
    region 'us-west-2'
    ;
    """.format(cl_iam_arn)

# FINAL TABLES

songplay_table_insert = """
SELECT
    *start_time, userId, level, song, *song_id, *artist_id, sessionId, location, userAgent
INTO songplays
FORM staging_events as e
WHERE
    page = 'NextSong'
"""

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
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
copy_table_queries = [staging_songs_copy, staging_events_copy]
# insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

insert_table_queries = []
