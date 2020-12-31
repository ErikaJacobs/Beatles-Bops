from modules.setup import Setup
from modules.spotify import Spotify
from modules.queries import queries
from modules.sql import Sql
from datetime import datetime

def run():    
    
    print(f'SPOTIFY ETL STARTED AT {datetime.now()}')
    
    # Setup
    setup = Setup()
    setup.config()
    
    # Spotify
    spotify = Spotify(setup)
    spotify.get_tables()
    
    # Queries
    etl_queries = queries()
    
    # SQL
    sql = Sql(setup, spotify, etl_queries)
    sql.execute_queries()
    print(f'SPOTIFY ETL COMPLETED AT {datetime.now()}')