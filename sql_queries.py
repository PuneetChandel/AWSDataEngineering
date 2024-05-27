import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        event_id    BIGINT IDENTITY(0,1),
        artist      VARCHAR,
        auth        VARCHAR,
        firstName   VARCHAR,
        gender      VARCHAR,
        itemInSession VARCHAR,
        lastName    VARCHAR,
        length      VARCHAR,
        level       VARCHAR,
        location    VARCHAR,
        method      VARCHAR,
        page        VARCHAR,
        registration VARCHAR,
        sessionId   INTEGER SORTKEY DISTKEY,
        song        VARCHAR,
        status      INTEGER,
        ts          BIGINT,
        userAgent   VARCHAR,
        userId      INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs           INTEGER,
        artist_id           VARCHAR SORTKEY DISTKEY,
        artist_latitude     VARCHAR,
        artist_longitude    VARCHAR,
        artist_location     VARCHAR(500),
        artist_name         VARCHAR(500),
        song_id             VARCHAR,
        title               VARCHAR(500),
        duration            DECIMAL(9),
        year                INTEGER
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INTEGER IDENTITY(0,1) NOT NULL SORTKEY,
        start_time  TIMESTAMP NOT NULL,
        user_id     INTEGER NOT NULL DISTKEY,
        level       VARCHAR(10) NOT NULL,
        song_id     VARCHAR(50) NOT NULL,
        artist_id   VARCHAR(50) NOT NULL,
        session_id  INTEGER NOT NULL,
        location    VARCHAR(100),
        user_agent  VARCHAR(255)
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id     INTEGER NOT NULL SORTKEY,
        first_name  VARCHAR(50),
        last_name   VARCHAR(80),
        gender      VARCHAR(10),
        level       VARCHAR(10)
    ) diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id     VARCHAR(50) NOT NULL SORTKEY,
        title       VARCHAR(500) NOT NULL,
        artist_id   VARCHAR(50) NOT NULL,
        year        INTEGER NOT NULL,
        duration    DECIMAL(9) NOT NULL
    ) diststyle all;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id   VARCHAR(50) NOT NULL SORTKEY,
        name        VARCHAR(500),
        location    VARCHAR(500),
        latitude    DECIMAL(9),
        longitude   DECIMAL(9)
    ) diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time  TIMESTAMP NOT NULL SORTKEY,
        hour        SMALLINT,
        day         SMALLINT,
        week        SMALLINT,
        month       SMALLINT,
        year        SMALLINT,
        weekday     SMALLINT
    ) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events 
    FROM '{}' 
    credentials 'aws_iam_role={}' 
    format as json '{}' 
    region 'us-west-2';
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY staging_songs 
    FROM '{}' 
    credentials 'aws_iam_role={}' 
    format as json 'auto' 
    region 'us-west-2';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT 
        DISTINCT TIMESTAMP 'epoch' + stge.ts/1000 * interval '1 second' AS start_time, 
        stge.userId        AS user_id, 
        stge.level         AS level, 
        stgs.song_id       AS song_id, 
        stgs.artist_id     AS artist_id, 
        stge.sessionId     AS session_id, 
        stge.location      AS location, 
        stge.userAgent     AS user_agent
    FROM staging_events stge
    JOIN staging_songs  stgs   
    ON (stge.song = stgs.title AND stge.artist = stgs.artist_name)
    WHERE stge.page  =  'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT 
        stge.userId  AS user_id,
        stge.firstName AS first_name,
        stge.lastName  AS last_name,
        stge.gender    AS gender,
        stge.level     AS level
    FROM staging_events stge
    WHERE stge.userId IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT 
        stgs.song_id AS song_id,
        stgs.title   AS title,
        stgs.artist_id AS artist_id,
        stgs.year    AS year,
        stgs.duration AS duration
    FROM staging_songs stgs
    WHERE stgs.song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT 
        stgs.artist_id   AS artist_id,
        stgs.artist_name AS name,
        stgs.artist_location AS location,
        stgs.artist_latitude  AS latitude,
        stgs.artist_longitude AS longitude
    FROM staging_songs stgs
    WHERE stgs.artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        start_time,
        EXTRACT(hour FROM start_time)       AS hour,
        EXTRACT(day FROM start_time)        AS day,
        EXTRACT(week FROM start_time)       AS week,
        EXTRACT(month FROM start_time)      AS month,
        EXTRACT(year FROM start_time)       AS year,
        EXTRACT(weekday FROM start_time)    AS weekday
    FROM songplays;
""")

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create
]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert
]
