# Spotify API ETL of Track Information to PostgreSQL
This project connects to the Spotify API to collect all useful track, album, and artist information about The Beatles. An ETL pipeline then loads all track information by The Beatles to PostgreSQL, in which the data is normalized utilizing a star schema. 

While this project was originally focused on creating an ETL pipeline for The Beatles as a band, this project can be configured for any artist.

## Methods Used
* ETL
* Data Modeling
* Normalization
* API Connection

## Technologies Used
* Python
* PostgreSQL
* pgAdmin

## Packages Used
* Psycopg2
* Spotipy
* Pandas

## How To Run

##### *Adjust Configurations*
Configurations in the config.ini

##### *Obtain Spotify API Tokens*
In order to use the Spotipy package, API tokens will need to be obtained directly from Spotify. [Click here](https://developer.spotify.com/documentation/general/guides/authorization-guide/) for more information on this process.

##### *Set Environment Variables*

# Featured Scripts or Deliverables
* [```run.py```](run.py)

# Other Repository Contents
* Modules
    * [```main.py```](/main.py) - Organizes execution of all modules
    * [```queries.py```](/queries.py) - Queries to drop, create, and insert data into tables
    * [```setup.py```](/setup.py) - Creates connection to spotify
    * [```spotify.py```](/spotify.py) - Pulls album, artist, and track information to create tables
    * [```sql.py```](/sql.py) - Connects to PostGreSQL, and executes queries to create fact table and star schema structure
* [```config.ini```](config.ini) - Configurations for PostGreSQL connection and Spotify artist
* [```requirements.txt```](requirements.txt) - Python package requirements
* [```schema_design.jpg```](https://github.com/ErikaJacobs/Beatles-Bops/blob/master/schema_design.jpg) - image of star schema

# Sources
* [Spotipy](https://spotipy.readthedocs.io/)
