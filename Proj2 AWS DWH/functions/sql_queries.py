import configparser

aws_config = 'configs/aws.cfg'

# CONFIG

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

    
staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_events (
    artist        VARCHAR,
    auth          VARCHAR,
    firstname     VARCHAR,
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
)
DISTSTYLE KEY;
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

# get paths to S3 data and a arn for role for read
config = configparser.ConfigParser()
config.read_file(open(aws_config))

s3_songs = config.get("S3", "song_data")
s3_logs = config.get("S3", "log_data")
s3_json_manifest = config.get("S3", "log_jsonpath")
cl_iam_arn = config.get("CLUSTER", "cl_iam_arn")
    
staging_songs_copy = """
    copy staging_songs 
    from {}
    credentials 'aws_iam_role={}'
    json 'auto'
    region 'us-west-2'
    ;
    """.format(s3_songs, cl_iam_arn)

staging_events_copy = """
    copy staging_events
    from {}
    credentials 'aws_iam_role={}'
    json {}
    region 'us-west-2'
    ;
    """.format(s3_logs, cl_iam_arn, s3_json_manifest)

# FINAL TABLES

user_table_insert = """
INSERT INTO users
WITH 
users_stage AS (
    SELECT
        userId, firstName, lastName, gender, level,        
        DENSE_RANK() OVER (PARTITION BY userId ORDER BY ts DESC) AS r
    FROM
        staging_events
    WHERE
        page = 'NextSong'
        AND NOT userId IS NULL
)
SELECT
    userId, firstName, lastName, gender, level
FROM
    users_stage
WHERE
    r = 1
;
"""

artist_table_insert = ("""
INSERT INTO artists
WITH
staging_artists AS (
    SELECT
        artist_id, artist_name, artist_location, artist_latitude, artist_longitude,
        DENSE_RANK() OVER (
            PARTITION BY
                artist_id
            ORDER BY
                songs_count DESC,
                artist_name, artist_location, artist_latitude, artist_longitude
                ) as r
    FROM (
        SELECT
            artist_id, artist_name, artist_location, artist_latitude, artist_longitude,
            COUNT(*) AS songs_count
        FROM
            staging_songs
        GROUP BY 1,2,3,4,5
    ) AS foo    
)
SELECT
    artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM
    staging_artists
WHERE
    r = 1
;    
""")
    
song_table_insert = ("""
INSERT INTO songs
WITH
staging_song AS (
    SELECT
        song_id, title, artist_id, year, duration,
        DENSE_RANK() OVER (
            PARTITION BY
                song_id
            ORDER BY
                song_count DESC,
                title, artist_id, year, duration
            ) AS r
    FROM (
        SELECT
            song_id, title, artist_id, year, duration,
            COUNT(*) AS song_count
        FROM
            staging_songs
        GROUP BY 1,2,3,4,5
    ) AS foo
)
SELECT
    song_id, title, artist_id, year, duration
FROM
    staging_song
WHERE
    r = 1
;
""")
    
time_table_insert = ("""
INSERT INTO time
SELECT
     start_time
    ,TO_CHAR(start_time, 'HH24')::int AS hour
    ,TO_CHAR(start_time, 'DD')::int AS day
    ,TO_CHAR(start_time, 'WW')::int AS week
    ,TO_CHAR(start_time, 'MM')::int AS month
    ,TO_CHAR(start_time, 'YYYY')::int AS year
    ,TO_CHAR(start_time, 'ID')::int AS weekday
FROM
    (
    SELECT
        ('1970-01-01'::date + ts/1000 * interval '1 second')::timestamp AS start_time
    FROM 
        (SELECT DISTINCT ts FROM staging_events)
    )
;
""")

songplay_table_insert = """
INSERT INTO songplays (
     start_time
    ,user_id
    ,level
    ,song
    ,song_id
    ,artist_id
    ,session_id
    ,location
    ,user_agent    
)
SELECT
     ('1970-01-01'::date + e.ts/1000 * interval '1 second')::timestamp as start_time
    ,e.userId
    ,u.level
    ,e.song
    ,s.song_id
    ,a.artist_id
    ,e.sessionId
    ,a.location
    ,e.userAgent    
FROM
    staging_events AS e
LEFT JOIN
    users AS u ON e.userId::INT = u.user_id
LEFT JOIN
    artists AS a ON e.artist = a.name
LEFT JOIN
    songs AS s ON e.song = s.title AND a.artist_id = s.artist_id
WHERE
    page = 'NextSong'
;
"""

# QUERY LISTS

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

copy_table_queries = [staging_songs_copy, staging_events_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
