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

def uri_artist(artist, sp):
    name = {artist}

    # Search For Artist
    result = sp.search(name)

    #Extract Artist's URI
    uri_artist = result['tracks']['items'][0]['artists'][0]['uri']
    
    return uri_artist

#%%

# Create df of ALL Albums

def album_table(uri, sp):
    
    albums = sp.artist_albums(uri)
    
    import ast
    import pandas as pd
    
    album_uri=[]
    album_type=[]
    name=[]
    album_name=[]
    total_tracks=[]
    release_date=[]
    
    # Extract Data From API
    
    for album in range(len(albums['items'])):
        album_uri.append(albums['items'][album]['uri']) #
        album_type.append(albums['items'][album]['type'])
        album_name.append(albums['items'][album]['name']) #
        total_tracks.append(albums['items'][album]['total_tracks'])
        release_date.append(albums['items'][album]['release_date'])
        
        
        artist = str(albums['items'][album]['artists']).replace('[{','{').replace('}]', '}')
        artist = ast.literal_eval(artist)
        name.append(artist['name'])

    album_df = pd.DataFrame({
        'artist': name,
        'album_name': album_name,
        'release_date': release_date,
        'total_tracks': total_tracks,
        'album_uri': album_uri,
        'album_type': album_type
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
    track_id = []
    track_name = []
    track_number = []
    track_type = []
    track_uri = []
    track_market = []
    artist_uri = []
    name = []
    
    # Loop
    for album in album_uri_loop:
        tracks = sp.album_tracks(album_id=album, offset=0)
        for track in range(len(tracks['items'])):
            
            album_uri.append(album)
            disc_number.append(tracks['items'][track]['disc_number'])
            duration_ms.append(tracks['items'][track]['duration_ms'])
            track_id.append(tracks['items'][track]['id'])
            track_name.append(tracks['items'][track]['name'])     
            track_number.append(tracks['items'][track]['track_number'])
            track_type.append(tracks['items'][track]['type'])
            track_uri.append(tracks['items'][track]['uri'])
            track_market.append(tracks['items'][track]['available_markets'])
            artist_uri.append(tracks['items'][track]['artists'][0]['uri'])
            name.append(tracks['items'][track]['artists'][0]['name'])
     
    track_df = pd.DataFrame({
        'album_uri': album_uri,
        'disc_number': disc_number,
        'duration_ms': duration_ms,
        'track_id': track_id,
        'track_name': track_name,
        'track_number': track_number,
        'track_type': track_type,
        'track_uri': track_uri,        
        'track_market': track_market,
        'artist_uri': artist_uri,
        'artist': name
        })
    
    keys = ['album_uri', 'disc_number', 'duration_ms',
                     'track_id', 'track_name', 'track_number', 'track_type', 'track_uri',
                     'artist_uri', 'artist']
    
    track_df2 = track_df.track_market.apply(pd.Series)\
        .merge(track_df, left_index=True, right_index=True)\
        .drop(["track_market"], axis = 1)\
        .melt(id_vars = keys, value_name = "track_market")\
        .drop("variable", axis = 1)\
        .dropna()
    
    return track_df2

#%%

def top_track_table(track_df, uri, sp):
    
    import pandas as pd
    
    # Obtain All Markets
    markets = track_df['track_market'].unique().tolist()
    
    # Top Tracks
    top_tracks = sp.artist_top_tracks(uri, country='US')
    
    # Create Empty Lists
    
    track_market = []
    track_name = []
    top_track_pop = []
    top_track_rank = []
    track_uri = []
    track_id = []
    album_uri = []
    artist_uri = []
    name = []
    
    top_tracks['tracks']
    for market in markets:
        top_tracks = sp.artist_top_tracks(uri, country=market)
        top_tracks = top_tracks['tracks']
    
    
    
        for track in top_tracks:
            index = top_tracks.index(track)    
            
            track_market.append(market)
            track_name.append(top_tracks[index]['name'])
            top_track_pop.append(top_tracks[index]['popularity'])
            top_track_rank.append(index + 1)
            track_uri.append(top_tracks[index]['uri'])
            track_id.append(top_tracks[index]['id'])
            album_uri.append(top_tracks[index]['album']['uri'])
            artist_uri.append(top_tracks[index]['artists'][0]['uri'])
            name.append(top_tracks[index]['artists'][0]['name'])
            
    top_track_df = pd.DataFrame({
            'track_market': track_market,
            'track_name': track_name,
            'top_track_pop': top_track_pop,
            'top_track_rank': top_track_rank,
            'track_uri': track_uri,
            'track_id': track_id,
            'album_uri': album_uri,        
            'artist_uri': artist_uri,
            'artist': name
            })
    return top_track_df

#%%

# Inputs 
client_id = client_id
client_secret = client_secret
artist = 'The Beatles'

# ETL Process
sp = spotify_connect(client_id, client_secret)
uri = uri_artist(artist, sp)
track_df = track_table(uri, sp)
album_df = album_table(uri, sp)
top_track_df = top_track_table(track_df, uri, sp)

#%%

# CONNECT TO POSTGRESQL

import psycopg2

conn = psycopg2.connect(
    host='127.0.0.1',
    database='BeatlesBops',
    user = 'postgres',
    password = 'postgres')

cur = conn.cursor()

cur.execute("DROP TABLE  IF EXISTS track_df")
conn.commit()

cur.execute("DROP TABLE  IF EXISTS album_df")
conn.commit()

cur.execute("DROP TABLE  IF EXISTS top_track_df")
conn.commit()

# QUERIES

# Create Table Queries
create_top_track_df = """CREATE TABLE IF NOT EXISTS top_track_df(
    track_market varchar(200),
    track_name varchar(200),
    top_track_pop varchar(200),
    top_track_rank varchar(200),
    track_uri varchar(200),
    track_id varchar(200),
    album_uri varchar(200),
    artist_uri varchar(200),
    artist varchar(200))"""
    
create_album_df = """CREATE TABLE IF NOT EXISTS album_df(
    album_uri varchar(200),
    album_type varchar(200),
    artist varchar(200),
    album_name varchar(200),
    total_tracks varchar(200),
    release_date varchar(200),
    album_market varchar(200))"""
    
create_track_df =  """CREATE TABLE IF NOT EXISTS track_df(
    album_uri varchar(300),
    disc_number varchar(300),
    duration_ms varchar(300),
    track_id varchar(300),
    track_name varchar(300),
    track_number varchar(300),
    track_type varchar(300),
    track_uri varchar(300),
    track_market varchar(300),
    artist_uri varchar(300),
    artist varchar(300))"""

# Insert Queries
insert_top_track_df = """insert into top_track_df values (%s, %s, %s, 
            %s, %s, %s, %s, %s, %s)"""
insert_album_df = """insert into album_df values (%s, %s, %s, 
            %s, %s, %s)"""
insert_track_df = """insert into track_df values (%s, %s, %s, 
             %s, %s, %s, %s, %s, %s, %s, %s)"""
    
queries = [create_top_track_df, create_album_df, create_track_df,
           insert_top_track_df, insert_album_df, insert_track_df]
    
# EXECUTION    
    
# Execute Create Table Queries
cur.execute(queries[0])
conn.commit()

cur.execute(queries[1])
conn.commit()

cur.execute(queries[2])
conn.commit()
    
# Execute Insert Queries

for index, row in top_track_df.iterrows():
    row = row.tolist()
    cur.execute(queries[3], (row[0], row[1], row[2],
            row[3], row[4], row[5], row[6], row[7], row[8]))
    conn.commit()

for index, row in album_df.iterrows():
    row = row.tolist()
    cur.execute(queries[4], (row[0], row[1], row[2],
            row[3], row[4], row[5]))
    conn.commit()

for index, row in track_df.iterrows():
    row = row.tolist()
    cur.execute(queries[5], (row[0], row[1], row[2],
            row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10]))
    conn.commit()    
    
# CLOSE CONNECTIONS
cur.close()
conn.close()

#%%
# Sources 
# https://spotipy.readthedocs.io/en/2.12.0/
# https://medium.com/@RareLoot/extracting-spotify-data-on-your-favourite-artist-via-python-d58bc92a4330
        
albums = sp.artist_albums(uri, album_type = ['album', 'compilation'])

for album in range(len(albums['items'])):
    print(albums['items'][album]['name'])


