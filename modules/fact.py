# Creates Fact Table
# One Module for The Beatles specifically - One for Any Other Artist

def fact(conn, artist, uri):
    # Connection
    cur = conn.cursor()
    
    if artist == 'The Beatles':
        drop1 = "DROP TABLE IF EXISTS beatles_fact;"
        insert1 = (
                   '''WITH df as (
                   SELECT a.track_uri, b.album_uri, b.artist_uri, 
                   ROUND(CAST(b.duration_ms as DECIMAL(8,2))/1000/60,2) as duration_mins, 
                   a.track_pop
                   FROM track_pop_df as a
                   LEFT JOIN track_df as b on a.track_uri =b.track_uri
                   LEFT JOIN artist_df as c on b.artist_uri = c.artist_uri
                   LEFT JOIN album_df as d on b.album_uri = d.album_uri
                   WHERE b.artist_uri='{}'
                   ORDER BY track_pop DESC, track_name)
                   
                   SELECT *
                   INTO beatles_fact
                   FROM df;'''.format(uri))
        drop2 = "DROP TABLE IF EXISTS track_pop_df;"
        alter1 = '''ALTER TABLE track_df
                    DROP COLUMN duration_ms;'''
            
    else:
        drop1 = "DROP TABLE IF EXISTS spotify_fact;"
        insert1 = (
                   '''WITH df as (
                   SELECT a.track_uri, b.album_uri, b.artist_uri, 
                   ROUND(CAST(b.duration_ms as DECIMAL(8,2))/1000/60,2) as duration_mins, 
                   a.track_pop
                   FROM track_pop_df as a
                   LEFT JOIN track_df as b on a.track_uri =b.track_uri
                   LEFT JOIN artist_df as c on b.artist_uri = c.artist_uri
                   LEFT JOIN album_df as d on b.album_uri = d.album_uri
                   WHERE b.artist_uri='{}'
                   ORDER BY track_pop DESC, track_name)
                   
                   SELECT *
                   INTO spotify_fact
                   FROM df;'''.format(uri))
        drop2 = "DROP TABLE IF EXISTS track_pop_df;"
        alter1 = '''ALTER TABLE track_df
                    DROP COLUMN duration_ms;'''
    # Execute Queries
    cur.execute(drop1)
    conn.commit()
    
    cur.execute(insert1)
    conn.commit()
    
    cur.execute(drop2)
    conn.commit()
    
    cur.execute(alter1)
    conn.commit()

    # Close Connections
    cur.close()
    conn.close()