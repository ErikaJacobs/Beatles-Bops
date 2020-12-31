# ETL PostGres Queries

def queries():
    
    # Drop Table Queries    
    drop_track_df = "DROP TABLE  IF EXISTS track_df"
    drop_album_df = "DROP TABLE  IF EXISTS album_df"
    drop_artist_df = "DROP TABLE  IF EXISTS artist_df"
    drop_track_pop_df = "DROP TABLE  IF EXISTS track_pop_df"
    
    # Create Table Queries
        
    create_album_df = """CREATE TABLE IF NOT EXISTS album_df(
        album_uri varchar(40),
        album_name varchar(70),
        release_date date,
        album_tracks int
        )"""
        
    create_track_df =  """CREATE TABLE IF NOT EXISTS track_df(
        track_uri varchar(40),
        track_name varchar(215),
        track_number int,
        duration_ms int,
        disc_number int,
        album_uri varchar(40),
        artist_uri varchar(40)
        )"""
   
    create_artist_df = """CREATE TABLE IF NOT EXISTS artist_df(
        artist_uri varchar(40),
        artist_name varchar(20)
        )"""
    
    create_track_pop_df = """CREATE TABLE IF NOT EXISTS track_pop_df(
        track_uri varchar(40),
        track_pop int
        )"""
    
    # Insert Queries
    insert_album_df = """insert into album_df values (%s, %s, %s, %s)"""
    insert_track_df = """insert into track_df values (%s, %s, %s, 
                 %s, %s, %s, %s)"""
    insert_artist_df = """insert into artist_df values (%s, %s)"""
    insert_track_pop_df = """insert into track_pop_df values (%s, %s)"""
    
    #Query Dict
    drop_queries = [drop_track_df, drop_album_df, drop_artist_df, drop_track_pop_df]
    create_queries = [create_track_df, create_album_df, create_artist_df, create_track_pop_df]
    insert_queries = {'track_df': insert_track_df, 
                      'album_df': insert_album_df, 
                      'artist_df': insert_artist_df, 
                      'track_pop_df': insert_track_pop_df}
    
    queries = {'drop_queries': drop_queries, 
               'create_queries': create_queries,
               'insert_queries': insert_queries}
    return queries
