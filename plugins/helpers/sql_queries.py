STAGING_TABLE_NAME_EVENT = "staging_events"
STAGING_TABLE_NAME_SONG = "staging_songs"
TABLE_NAME_SONGPLAY = "songplays"
TABLE_NAME_USER = "users"
TABLE_NAME_SONG = "songs"
TABLE_NAME_ARTIST = "artists"
TABLE_NAME_TIME = "time_table"

class SqlQueries:
    # Table Creation queries
    staging_events_table_create= f"""
        CREATE TABLE IF NOT EXISTS {STAGING_TABLE_NAME_EVENT}(
            artist              VARCHAR(MAX),
            auth                VARCHAR(MAX),
            firstName           VARCHAR(MAX),
            gender              VARCHAR(MAX),
            itemInSession       INT,
            lastName            VARCHAR(MAX),
            length              FLOAT,
            level               VARCHAR(MAX),
            location            VARCHAR(MAX),
            method              VARCHAR(MAX),
            page                VARCHAR(MAX),
            registration        TIMESTAMP,
            sessionId           INT,
            song                VARCHAR(MAX),
            status              INT,
            ts                  TIMESTAMP,
            userAgent           VARCHAR(MAX),
            userId              INT
        );
        """

    staging_songs_table_create = f"""
        CREATE TABLE IF NOT EXISTS {STAGING_TABLE_NAME_SONG}(
            artist_id           VARCHAR(MAX),
            artist_latitude     FLOAT, 
            artist_location     VARCHAR(MAX),
            artist_longitude    FLOAT,
            artist_name         VARCHAR(MAX),
            duration            FLOAT,
            num_songs           INT,
            song_id             VARCHAR(MAX),
            title               VARCHAR(MAX),
            year                INT
        );
    """

    songplay_table_create = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME_SONGPLAY}(
            songplay_id         INT                 IDENTITY(0,1),
            start_time          TIMESTAMP,
            user_id             INT,
            level               VARCHAR(MAX),
            song_id             VARCHAR(MAX)        DISTKEY,
            artist_id           VARCHAR(MAX),
            session_id          INT,
            location            VARCHAR(MAX),
            user_agent          VARCHAR(MAX),

            FOREIGN KEY (start_time) REFERENCES time_table(start_time),
            FOREIGN KEY (user_id) REFERENCES {TABLE_NAME_USER}(user_id),
            FOREIGN KEY (song_id) REFERENCES {TABLE_NAME_SONG}(song_id),
            FOREIGN KEY (artist_id) REFERENCES {TABLE_NAME_ARTIST}(artist_id)
        );
    """


    user_table_create = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME_USER}(
            user_id             INT                 PRIMARY KEY,
            first_name          VARCHAR(MAX),
            last_name           VARCHAR(MAX),
            gender              VARCHAR(MAX),
            level               VARCHAR(MAX)
        )
        DISTSTYLE AUTO;
        """

    song_table_create = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME_SONG}(
            song_id             VARCHAR(MAX)        PRIMARY KEY     DISTKEY,
            title               VARCHAR(MAX),
            artist_id           VARCHAR(MAX),
            year                INT,
            duration            FLOAT,

            FOREIGN KEY (artist_id) REFERENCES {TABLE_NAME_ARTIST}(artist_id)
        );
        """

    artist_table_create = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME_ARTIST}(
            artist_id           VARCHAR(MAX)        PRIMARY KEY,
            name                VARCHAR(MAX),
            location            VARCHAR(MAX),
            latitude            VARCHAR(MAX),
            longitude           VARCHAR(MAX)
        )
        DISTSTYLE AUTO;
        """

    time_table_create = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME_TIME}(
            start_time          TIMESTAMP           PRIMARY KEY,
            hour                INT,
            day                 INT,
            week                INT,
            month               INT,
            year                INT,
            weekday             INT
        )
        DISTSTYLE AUTO;
        """


    # table insertion queries
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)