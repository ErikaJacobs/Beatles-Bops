# Spotify API ETL of Track Information to PostgreSQL
This project connects to the Spotify API to collect all useful track, album, and artist information about The Beatles. An ETL pipeline then loads all track information by The Beatles to PostgreSQL, in which the data is normalized utilizing a star schema. 

While this project was originally focused on creating an ETL pipeline for The Beatles as a band, inputs can be adjusted to utilize this project for any artist.

## Methods Used
* ETL
* Data Modeling
* API Connection

## Technologies Used
* Python
* PostgreSQL
* pgAdmin

## Packages Used
* Psycopg2
* Spotipy
* Pandas

# Featured Notebooks, Scripts, Analysis, or Deliverables
* [```run.py```](https://github.com/ErikaJacobs/Beatles-Bops/blob/master/run.py)

# Other Repository Contents
* Modules
     * [```album_table.py```](https://github.com/ErikaJacobs/Beatles-Bops/blob/master/modules/album_table.py) - Pulls album information and creates table
     * [```artist_table.py```](https://github.com/ErikaJacobs/Beatles-Bops/blob/master/modules/artist_table.py) - Pulls artist information and creates table
     * [```etl_queries.py```](https://github.com/ErikaJacobs/Beatles-Bops/blob/master/modules/etl_queries.py) - Queries to drop, create, and insert data into tables
     * [```execute_etl_queries.py```](https://github.com/ErikaJacobs/Beatles-Bops/blob/master/modules/execute_etl_queries.py) Executes written queries
     * [```fact.py```](https://github.com/ErikaJacobs/Beatles-Bops/blob/master/modules/fact.py) - Creates final fact table and star schema structure
     * [```main.py```](https://github.com/ErikaJacobs/Beatles-Bops/blob/master/modules/main.py) - Organizes execution of all modules
     * [```postgres_connect.py```](https://github.com/ErikaJacobs/Beatles-Bops/blob/master/modules/postgres_connect.py) - Creates connection to PostgreSQL database
     * [```spotify_connect.py```](https://github.com/ErikaJacobs/Beatles-Bops/blob/master/modules/spotify_connect.py) - Creates connection to Spotify
     * [```track_pop_table.py```](https://github.com/ErikaJacobs/Beatles-Bops/blob/master/modules/track_pop_table.py) - Pulls track popularity and creates table
     * [```track_table.py```](https://github.com/ErikaJacobs/Beatles-Bops/blob/master/modules/track_table.py) - Pulls track information and creates table
* Schema Design
     * [```schema_design.jpg```](https://github.com/ErikaJacobs/Beatles-Bops/blob/master/schema_design.jpg) - image of star schema

# Sources
* [Spotipy](https://spotipy.readthedocs.io/)
