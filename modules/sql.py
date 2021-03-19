import psycopg2

class Sql:
    
    def __init__(self, setup, spotify, queries):
        self.conn = psycopg2.connect(host = setup.configs['host'],
                                database = setup.configs['database'],
                                user = setup.configs['user'],
                                password = setup.configs['password'])
        self.queries = queries
        self.album_df = spotify.album_df
        self.track_df = spotify.track_df
        self.artist_df = spotify.artist_df
        self.track_pop_df = spotify.track_pop_df
        self.uri = spotify.uri
        self.artist = setup.configs['artist']

    # Execute ETL Queries
    def etl_queries(self):
        conn = self.conn
        cur = conn.cursor()
        queries = self.queries
    
        # Execute Drop Table Queries
        
        for query in queries['drop_queries']:
            cur.execute(query)
            conn.commit()
        
        # Execute Create Table Queries   
        
        for query in queries['create_queries']:
            cur.execute(query)
            conn.commit()
    
        # Execute Insert Queries
        for index, row in self.track_df.iterrows():
            row = row.tolist()
            cur.execute(queries['insert_queries']['track_df'], 
                        (row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
            conn.commit()    
        
        for index, row in self.album_df.iterrows():
            row = row.tolist()
            cur.execute(queries['insert_queries']['album_df'], 
                        (row[0], row[1], row[2], row[3]))
            conn.commit()
        
        for index, row in self.artist_df.iterrows():
            row = row.tolist()
            cur.execute(queries['insert_queries']['artist_df'], 
                        (row[0], row[1]))
            conn.commit()    
        
        for index, row in self.track_pop_df.iterrows():
            row = row.tolist()
            cur.execute(queries['insert_queries']['track_pop_df'], 
                        (row[0], row[1]))
            conn.commit()    
        
        # Close Cursor
        cur.close()

    # Creates Fact Table
    # For The Beatles OR Any Artist
    def fact_queries(self):
        
        # Connection
        conn = self.conn
        cur = conn.cursor()
        
        if self.artist == 'The Beatles':
            keyword = 'beatles'
        else:
            keyword = 'spotify_fact'
            
            
        drop1 = f"DROP TABLE IF EXISTS {keyword}_fact;"
        insert1 = (
                   f'''WITH df as (
                   SELECT a.track_uri, b.album_uri, b.artist_uri, 
                   ROUND(CAST(b.duration_ms as DECIMAL(8,2))/1000/60,2) as duration_mins, 
                   a.track_pop
                   FROM track_pop_df as a
                   LEFT JOIN track_df as b on a.track_uri =b.track_uri
                   LEFT JOIN artist_df as c on b.artist_uri = c.artist_uri
                   LEFT JOIN album_df as d on b.album_uri = d.album_uri
                   WHERE b.artist_uri='{self.uri}'
                   ORDER BY track_pop DESC, track_name)
                   
                   SELECT *
                   INTO {keyword}_fact
                   FROM df;''')
        drop2 = "DROP TABLE IF EXISTS track_pop_df;"
        alter1 = '''ALTER TABLE track_df
                    DROP COLUMN duration_ms,
                    DROP COLUMN album_uri,
                    DROP COLUMN artist_uri;'''
                        
        # Execute Queries
        fact_queries = [drop1, insert1, drop2, alter1]
        
        for query in fact_queries: 
            cur.execute(query)
            conn.commit()
    
        # Close Connections
        cur.close()
        
    def execute_queries(self):
        self.etl_queries()
        self.fact_queries()
        self.conn.close()