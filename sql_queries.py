import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

SONG_DATA = config.get("S3", "SONG_DATA")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
ARN = config.get("IAM_ROLE", "ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
       artist         VARCHAR,
       auth           VARCHAR,
       firstName      VARCHAR,
       gender         VARCHAR,
       itemInSession  INT,
       lastName       VARCHAR,
       length         DECIMAL,
       level          VARCHAR,
       location       VARCHAR,
       methode        VARCHAR,
       page           VARCHAR,
       registeration  VARCHAR,
       sessionId      INT,
       song           VARCHAR,
       status         INT,
       ts             TIMESTAMP,
       userAgent      VARCHAR,
       userId         INT
        
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_song         INT,
        artist_id        VARCHAR,
        artist_latitude  DECIMAL,
        artist_longitude DECIMAL,
        artist_name      VARCHAR,
        song_id          VARCHAR,
        title            VARCHAR,
        duration         DECIMAL,
        year             INT
 
    );
""")



user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id     INT      NOT NULL PRIMARY KEY   distkey,
        first_name  VARCHAR  NOT NULL,
        last_name   VARCHAR  NOT NULL,
        gender      VARCHAR,
        level       VARCHAR
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id    VARCHAR  PRIMARY KEY   sortkey,
        title      VARCHAR  NOT NULL,
        artist_id  VARCHAR  NOT NULL,
        year       INT,
        duration   DECIMAL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist(
        artist_id   VARCHAR  PRIMARY KEY  sortkey,
        name        VARCHAR  NOT NULL,
        location    VARCHAR,
        latitude    DECIMAL,
        longitude   DECIMAL
    );
""")

time_table_create = ("""
     CREATE TABLE IF NOT EXISTS time(
        start_time   TIMESTAMP  PRIMARY KEY sortkey,
        hour         INT,
        day          INT,
        week         INT,
        month        INT,
        year         INT,
        weekday      INT
     );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id  INT   IDENTITY(0,1) PRIMARY KEY,
        start_time   TIMESTAMP  NOT NULL      sortkey distkey,
        user_id      INT        NOT NULL,
        level        VARCHAR,
        song_id      VARCHAR    NOT NULL,
        artist_id    VARCHAR    NOT NULL,
        session_id   INT,
        location     VARCHAR,
        user_agent   VARCHAR
    );
""")
# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as JSON {}
    timeformat as 'epochmillisecs';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as JSON 'auto';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCE(e.ts)  AS start_time,
           e.userId        AS user_id,
           e.level,
           s.song_id,
           s.artist_id,
           e.sessionId     AS session_id,
           e.location,
           e.userAgent     AS user_agent 
    FROM staging_events e
    JOIN staging_songs  s ON (s.title = e.song AND s.artist_name = e.artist)
    AND e.page == 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT(userId)  AS user_id,
           firstName         AS first_name,
           lastName          AS last_name,
           gender,
           level
    FROM staging_events
    WHERE user_id IS NOT NULL
    AND page == 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    SELECT DISTINCT(song_id),
           title,
           artist_id,
           year,
           duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longitude)
    SELECT DISTINCT(artist_id),
           artist_name       AS name,
           location,
           artist_latitude   AS latitude,
           artist_longitude  AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT start_time,
           EXTRACT(hour      FROM start_time),
           EXTRACT(day       FROM start_time),
           EXTRACT(week      FROM start_time),
           EXTRACT(month     FROM start_time),
           EXTRACT(year      FROM start_time),
           EXTRACT(dayofweek FROM start_time)
    FROM songplays;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
