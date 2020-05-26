# Connect Python and PostGres

def postgres_connect(host, database, user, password):
    import psycopg2
    
    conn = psycopg2.connect(
        host=host,
        database=database,
        user = user,
        password = password)
    return conn
