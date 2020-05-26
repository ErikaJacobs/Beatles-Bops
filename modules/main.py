# Beatles Bops - Main Application

def run():
    ####################################

    # Inputs - General
    client_id = client_id
    client_secret = secret_id
    artist = 'The Beatles'
    
    # Inputs - PostGres
    host='127.0.0.1'
    database='BeatlesBops'
    user = 'postgres'
    password = 'postgres'

    ####################################
    
    # Import Modules
    from modules.spotify_connect import spotify_connect
    from modules.artist_table import artist_table
    from modules.track_table import track_table
    from modules.album_table import album_table
    from modules.track_pop_table import track_pop_table
    from modules.postgres_connect import postgres_connect
    from modules.etl_queries import etl_queries
    from modules.execute_etl_queries import execute_etl_queries
    from modules.fact import fact
    
    # Import Packages
    import pandas as pd
    from datetime import datetime
    
    ####################################
    
    # ETL Process
    print(f'SPOTIFY ETL STARTED AT {datetime.now()}')
    sp = spotify_connect(client_id, client_secret)
    artist_df = artist_table(artist, sp)
    uri = artist_df['artist_uri'][0]
    track_df = track_table(uri, sp)
    album_df = album_table(uri, sp)
    track_pop_df = track_pop_table(track_df, uri, sp)
    conn = postgres_connect(host, database, user, password)
    queries = etl_queries()
    execute_etl_queries(conn, queries, album_df, track_df, 
                    artist_df, track_pop_df)
    fact(conn, artist, uri)
    print(f'SPOTIFY ETL COMPLETED AT {datetime.now()}')
    
#%%