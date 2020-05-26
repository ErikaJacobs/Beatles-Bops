# Execute ETL Queries

def execute_etl_queries(conn, queries, album_df, 
                    track_df, artist_df, track_pop_df):
    cur = conn.cursor()

    # Execute Drop Table Queries
    cur.execute(queries[0])
    conn.commit()
    
    cur.execute(queries[3])
    conn.commit()
    
    cur.execute(queries[6])
    conn.commit()
    
    cur.execute(queries[9])
    conn.commit()
    
    # Execute Create Table Queries   

    cur.execute(queries[1])
    conn.commit()
    
    cur.execute(queries[4])
    conn.commit()
    
    cur.execute(queries[7])
    conn.commit()
    
    cur.execute(queries[10])
    conn.commit()
    
    # Execute Insert Queries
    
    for index, row in track_df.iterrows():
        row = row.tolist()
        cur.execute(queries[2], (row[0], row[1], row[2],
                row[3], row[4], row[5], row[6]))
        conn.commit()    
    
    for index, row in album_df.iterrows():
        row = row.tolist()
        cur.execute(queries[5], (row[0], row[1], row[2],
                row[3]))
        conn.commit()
    
    for index, row in artist_df.iterrows():
        row = row.tolist()
        cur.execute(queries[8], (row[0], row[1]))
        conn.commit()    
    
    for index, row in track_pop_df.iterrows():
        row = row.tolist()
        cur.execute(queries[11], (row[0], row[1]))
        conn.commit()    
    
    # Close Cursor
    cur.close()