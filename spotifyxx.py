# Import Packages
import spotipy
import spotipy.util
from spotipy.oauth2 import SpotifyClientCredentials #To access authorised Spotify data

#%%

# Connect to API

client_id = CLIENT_ID
client_secret = CLIENT_SECRET

client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager) #spotify object to access API
#%%

# Search for Band

# Choose Artist
name = {"The Beatles"}

# Search For Artist
result = sp.search(name)
print(result)

#Extract Artist's URI
artist_uri = result['tracks']['items'][0]['artists'][0]['uri']



#%%

# Create Album Table

# All Artist Albums and Singles
albums = sp.artist_albums(artist_uri, album_type=['album','single'])
print(albums)

# items[type] ("Album_Type"), name ("Artist")
# items[i]['name'] ("Album_Name"), items[release_date] ("Album_Release_Dt"), 
# items[total_tracks] ("Total_Tracks"), items[uri] ("Album_URI")

def album_table(albums):
    album_uris=[]
    for album in range(len(albums['items'])):
        album_uris.append(albums['items'][i]['uri'])
        
        
#%%
        
# Create Tracks Table

# album_tracks(album_id, limit=50, offset=0)
        
#%%
        
# Create Top Tracks Table - By Country
        
# 'available_markets': ['AD', 'AE', 'AR', 'AT', 'AU', 'BE', 'BG', 'BH', 'BO', 'BR', 'CA', 'CH', 'CL', 'CO', 'CR', 'CY', 'CZ', 'DE', 'DK', 'DO', 'DZ', 'EC', 'EE', 'EG', 'ES', 'FI', 'FR', 'GB', 'GR', 'GT', 'HK', 'HN', 'HU', 'ID', 'IE', 'IL', 'IN', 'IS', 'IT', 'JO', 'JP', 'KW', 'LB', 'LI', 'LT', 'LU', 'LV', 'MA', 'MC', 'MT', 'MX', 'MY', 'NI', 'NL', 'NO', 'NZ', 'OM', 'PA', 'PE', 'PH', 'PL', 'PS', 'PT', 'PY', 'QA', 'RO', 'SA', 'SE', 'SG', 'SK', 'SV', 'TH', 'TN', 'TR', 'TW', 'US', 'UY', 'VN', 'ZA']
        
        
#%%
# Sources 
# https://spotipy.readthedocs.io/en/2.12.0/
# https://medium.com/@RareLoot/extracting-spotify-data-on-your-favourite-artist-via-python-d58bc92a4330
        
    


