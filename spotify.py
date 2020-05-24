#%%

# Connect to API

def spotify_connect(client_id, client_secret):
    # Import Packages
    import spotipy
    import spotipy.util
    from spotipy.oauth2 import SpotifyClientCredentials #To access authorised Spotify data

    # Connect to Spotify
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    return sp

#%%

# Get Artist URI

def artist_table(artist, sp):
    
    import pandas as pd
    
    name = artist

    # Search For Artist
    result = sp.search({name})

    #Extract Artist's URI
    uri_artist = result['tracks']['items'][0]['artists'][0]['uri']
    
    artist_df = pd.DataFrame({
        'artist_uri': [uri_artist],
        'artist_name': [name]
        })
    
    return artist_df

#%%

# Create df of ALL Albums

def album_table(uri, sp):
    
    albums = sp.artist_albums(uri, album_type = 'album')
    
    import pandas as pd
    
    album_uri=[]
    album_name=[]
    album_tracks=[]
    release_date=[]
    
    # Extract Data From API
    
    for album in range(len(albums['items'])):
        album_uri.append(albums['items'][album]['uri']) #
        album_name.append(albums['items'][album]['name']) #
        album_tracks.append(albums['items'][album]['total_tracks'])
        release_date.append(albums['items'][album]['release_date'])

    album_df = pd.DataFrame({
        'album_uri': album_uri,
        'album_name': album_name,
        'release_date': release_date,
        'album_tracks': album_tracks
        })
    
    return album_df

#%%
        
# Create df of ALL tracks

def track_table(uri, sp):
    
    import pandas as pd
    
    # Album List
    albums = sp.artist_albums(uri)
    album_uri_loop = []
    
    for album in range(len(albums['items'])):
        album_uri_loop.append(albums['items'][album]['uri'])
    
    # Preparing Empty Lists
    album_uri = []
    disc_number = []
    duration_ms = []
    track_name = []
    track_number = []
    track_uri = []
    artist_uri = []
    
    # Loop
    for album in album_uri_loop:
        tracks = sp.album_tracks(album_id=album, offset=0)
        for track in range(len(tracks['items'])):
            
            album_uri.append(album)
            disc_number.append(tracks['items'][track]['disc_number'])
            duration_ms.append(tracks['items'][track]['duration_ms'])
            track_name.append(tracks['items'][track]['name'])     
            track_number.append(tracks['items'][track]['track_number'])
            track_uri.append(tracks['items'][track]['uri'])
            artist_uri.append(tracks['items'][track]['artists'][0]['uri'])
     
    track_df = pd.DataFrame({
        'track_uri': track_uri,   
        'track_name': track_name,
        'track_number': track_number,
        'duration_ms': duration_ms,
        'disc_number': disc_number,
        'album_uri': album_uri,
        'artist_uri': artist_uri
        })
    
    return track_df

#%%

def top_track_table(track_df, uri, sp):
    
    import pandas as pd
    
    # Create Empty Lists
    
    track_market = []
    top_track_pop = []
    top_track_rank = []
    track_uri = []
    album_uri = []
    artist_uri = []
    
    top_tracks = sp.artist_top_tracks(uri)
    top_tracks = top_tracks['tracks']
    
    for track in top_tracks:
        index = top_tracks.index(track)    
        
        top_track_pop.append(top_tracks[index]['popularity'])
        top_track_rank.append(index + 1)
        track_uri.append(top_tracks[index]['uri'])
        album_uri.append(top_tracks[index]['album']['uri'])
        artist_uri.append(top_tracks[index]['artists'][0]['uri'])
            
    top_track_df = pd.DataFrame({
            'track_uri': track_uri,
            'top_track_pop': top_track_pop,
            'top_track_rank': top_track_rank,
            'album_uri': album_uri,        
            'artist_uri': artist_uri
            })
    return top_track_df

#%%
    
def track_pop_table(track_df, uri, sp):
    
    import pandas as pd

    tracklist = track_df['track_uri'].to_list()     
    tracks = []
    
    for track in tracklist:
        if track not in tracks:
            tracks.append(track)
    
    track_uri = []
    track_pop = []
    
    for track in tracks:
        
        track = sp.track(track)
        
        track_uri.append(track['uri'])
        track_pop.append(track['popularity'])

    track_pop_df = pd.DataFrame({
            'track_uri': track_uri,
            'track_pop': track_pop,
            })
    return track_pop_df

#%%

# CONNECT TO POSTGRESQL
def postgre_connect():
    import psycopg2
    
    conn = psycopg2.connect(
        host='127.0.0.1',
        database='BeatlesBops',
        user = 'postgres',
        password = 'postgres')
    return conn

# QUERIES

def etl_queries():
    
# Drop Table Queries    
    drop_track_df = "DROP TABLE  IF EXISTS track_df"
    drop_abum_df = "DROP TABLE  IF EXISTS album_df"
    drop_top_track_df = "DROP TABLE  IF EXISTS top_track_df"
    drop_artist_df = "DROP TABLE  IF EXISTS artist_df"
    drop_track_pop_df = "DROP TABLE  IF EXISTS track_pop_df"
    
    # Create Table Queries
    create_top_track_df = """CREATE TABLE IF NOT EXISTS top_track_df(
        track_uri varchar(200),
        top_track_pop int,
        top_track_rank int,
        album_uri varchar(200),
        artist_uri varchar(200)
        )"""
        
    create_album_df = """CREATE TABLE IF NOT EXISTS album_df(
        album_uri varchar(200),
        album_name varchar(200),
        release_date date,
        album_tracks int
        )"""
        
    create_track_df =  """CREATE TABLE IF NOT EXISTS track_df(
        track_uri varchar(300),
        track_name varchar(300),
        track_number int,
        duration_ms int,
        disc_number int,
        album_uri varchar(300),
        artist_uri varchar(300)
        )"""
   
    create_artist_df = """CREATE TABLE IF NOT EXISTS artist_df(
        artist_uri varchar(200),
        artist_name varchar(200)
        )"""
    
    create_track_pop_df = """CREATE TABLE IF NOT EXISTS track_pop_df(
        track_uri varchar(200),
        track_pop int
        )"""
    
    # Insert Queries
    insert_top_track_df = """insert into top_track_df values (%s, %s, 
                %s, %s, %s)"""
    insert_album_df = """insert into album_df values (%s, %s, %s, %s)"""
    insert_track_df = """insert into track_df values (%s, %s, %s, 
                 %s, %s, %s, %s)"""
    insert_artist_df = """insert into artist_df values (%s, %s)"""
    insert_track_pop_df = """insert into track_pop_df values (%s, %s)"""
    
    #Query List
    queries = [create_top_track_df, create_album_df, create_track_df,
               insert_top_track_df, insert_album_df, insert_track_df,
               drop_track_df, drop_abum_df, drop_top_track_df,
               drop_artist_df, create_artist_df, insert_artist_df,
               drop_track_pop_df, create_track_pop_df, insert_track_pop_df,]
    return queries


# EXECUTION    
def execute_queries(conn, queries, top_track_df, album_df, 
                    track_df, artist_df, track_pop_df):
    cur = conn.cursor()

    # Execute Drop Table Queries
    cur.execute(queries[6])
    conn.commit()
    
    cur.execute(queries[7])
    conn.commit()
    
    cur.execute(queries[8])
    conn.commit()
    
    cur.execute(queries[9])
    conn.commit()
    
    cur.execute(queries[12])
    conn.commit()
    
    # Execute Create Table Queries   
    
    # Top Track    
    cur.execute(queries[0])
    conn.commit()
    
    # Album
    cur.execute(queries[1])
    conn.commit()
    
    # Track
    cur.execute(queries[2])
    conn.commit()
    
    # Artist
    cur.execute(queries[10])
    conn.commit()
    
    # Track Desc
    cur.execute(queries[13])
    conn.commit()
    
    # Execute Insert Queries
    
    for index, row in top_track_df.iterrows():
        row = row.tolist()
        cur.execute(queries[3], (row[0], row[1], row[2],
                row[3], row[4]))
        conn.commit()
    
    for index, row in album_df.iterrows():
        row = row.tolist()
        cur.execute(queries[4], (row[0], row[1], row[2],
                row[3]))
        conn.commit()
    
    for index, row in track_df.iterrows():
        row = row.tolist()
        cur.execute(queries[5], (row[0], row[1], row[2],
                row[3], row[4], row[5], row[6]))
        conn.commit()    
    
    for index, row in artist_df.iterrows():
        row = row.tolist()
        cur.execute(queries[11], (row[0], row[1]))
        conn.commit()    
    
    for index, row in track_pop_df.iterrows():
        row = row.tolist()
        cur.execute(queries[14], (row[0], row[1]))
        conn.commit()    
        
    # CLOSE CONNECTIONS
    cur.close()
    conn.close()

#%%
    
def application():
    import pandas as pd
    
    # Inputs 
    client_id = client_id
    client_secret = client_secret
    artist = 'The Beatles'
    
    # ETL Process
    sp = spotify_connect(client_id, client_secret)
    artist_df = artist_table(artist, sp)
    uri = artist_df['artist_uri'][0]
    track_df = track_table(uri, sp)
    album_df = album_table(uri, sp)
    top_track_df = top_track_table(track_df, uri, sp)
    track_pop_df = track_pop_table(track_df, uri, sp)
    conn = postgre_connect()
    queries = etl_queries()
    execute_queries(conn, queries, top_track_df, 
                    album_df, track_df, artist_df, track_pop_df)

#%%

application()

#%%
# Sources 
# https://spotipy.readthedocs.io/en/2.12.0/
# https://medium.com/@RareLoot/extracting-spotify-data-on-your-favourite-artist-via-python-d58bc92a4330