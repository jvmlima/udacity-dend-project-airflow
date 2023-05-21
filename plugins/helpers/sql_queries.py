class SqlQueries:

    # CREATE TABLES

    # Stating ---

    staging_events_table_create = ("""
        CREATE TABLE public.staging_events (
            artist varchar(256),
            auth varchar(256),
            firstname varchar(256),
            gender varchar(256),
            iteminsession int4,
            lastname varchar(256),
            length numeric(18,0),
            "level" varchar(256),
            location varchar(256),
            "method" varchar(256),
            page varchar(256),
            registration numeric(18,0),
            sessionid int4,
            song varchar(256),
            status int4,
            ts int8,
            useragent varchar(256),
            userid int4
        );
    """)

    staging_songs_table_create = ("""
        CREATE TABLE IF NOT EXISTS public.staging_songs (
            num_songs        BIGINT               NULL,
            artist_id        VARCHAR          NOT NULL,
            artist_latitude  DOUBLE PRECISION     NULL,
            artist_longitude DOUBLE PRECISION     NULL,
            artist_location  VARCHAR              NULL,
            artist_name      VARCHAR              NULL,
            song_id          VARCHAR          NOT NULL,
            title            VARCHAR              NULL,
            duration         DOUBLE PRECISION     NULL,
            year             SMALLINT             NULL DISTKEY
        );
    """)

    

    # Fact ---

    songplay_table_create = ("""
        CREATE TABLE public.songplays (
            playid varchar(32) NOT NULL,
            start_time timestamp NOT NULL,
            userid int4 NOT NULL,
            "level" varchar(256),
            songid varchar(256),
            artistid varchar(256),
            sessionid int4,
            location varchar(256),
            user_agent varchar(256),
            CONSTRAINT songplays_pkey PRIMARY KEY (playid)
        );
    """)

    # Dimensions ---

    user_table_create = ("""
        CREATE TABLE public.users (
            userid int4 NOT NULL,
            first_name varchar(256),
            last_name varchar(256),
            gender varchar(256),
            "level" varchar(256),
            CONSTRAINT users_pkey PRIMARY KEY (userid)
        );
    """)

    song_table_create = ("""
        CREATE TABLE public.songs (
            songid varchar(256) NOT NULL,
            title varchar(256),
            artistid varchar(256),
            "year" int4,
            duration numeric(18,0),
            CONSTRAINT songs_pkey PRIMARY KEY (songid)
        );
    """)

    artist_table_create = ("""
        CREATE TABLE public.artists (
            artistid varchar(256) NOT NULL,
            name varchar(256),
            location varchar(256),
            lattitude numeric(18,0),
            longitude numeric(18,0)
        );
    """)

    time_table_create = ("""
        CREATE TABLE public."times" (
            start_time timestamp NOT NULL,
            "hour" int4,
            "day" int4,
            week int4,
            "month" varchar(256),
            "year" int4,
            weekday varchar(256),
            CONSTRAINT time_pkey PRIMARY KEY (start_time)
        );
    """)

    
    # FINAL TABLES

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

    # QUERY LISTS
    create_table_queries = {'staging_events': staging_events_table_create,
                            'staging_songs':  staging_songs_table_create,
                            'songplays':      songplay_table_create,
                            'users':          user_table_create,
                            'songs':          song_table_create,
                            'artists':        artist_table_create,
                            'times':          time_table_create}

    insert_table_queries = {'songplays':      songplay_table_insert,
                            'users':          user_table_insert,
                            'songs':          song_table_insert,
                            'artists':        artist_table_insert,
                            'times':          time_table_insert}
    