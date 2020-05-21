#%%

# Connect to API

def spotify_connect(client_id, client_secret):
    # Import Packages
    import spotipy
    import spotipy.util
    from spotipy.oauth2 import SpotifyClientCredentials #To access authorised Spotify data

    # Connect to Spotify
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    return sp

#%%

# Get Artist URI

def uri_artist(artist, sp):
    name = {artist}

    # Search For Artist
    result = sp.search(name)

    #Extract Artist's URI
    uri_artist = result['tracks']['items'][0]['artists'][0]['uri']
    
    return uri_artist

#%%

# Create df of ALL Albums

def album_df(uri, sp):
    
    albums = sp.artist_albums(uri, album_type=['album','single'])
    
    import ast
    import pandas as pd
    
    album_uri=[]
    album_type=[]
    name=[]
    album_name=[]
    total_tracks=[]
    release_date=[]
    
    # Extract Data From API
    
    for album in range(len(albums['items'])):
        album_uri.append(albums['items'][album]['uri']) #
        album_type.append(albums['items'][album]['type'])
        album_name.append(albums['items'][album]['name']) #
        total_tracks.append(albums['items'][album]['total_tracks'])
        release_date.append(albums['items'][album]['release_date'])
        
        
        artist = str(albums['items'][album]['artists']).replace('[{','{').replace('}]', '}')
        artist = ast.literal_eval(artist)
        name.append(artist['name'])

    album_df = pd.DataFrame({
        'artist': name,
        'album_name': album_name,
        'release_date': release_date,
        'total_tracks': total_tracks,
        'album_uri': album_uri,
        'album_type': album_type
        })
    
    return album_df

#%%
        
# Create df of ALL tracks

def track_df(uri, sp):
    
    import pandas as pd
    
    # Album List
    albums = sp.artist_albums(uri, album_type=['album','single'])
    album_uri_loop = []
    
    for album in range(len(albums['items'])):
        album_uri_loop.append(albums['items'][album]['uri'])
    
    # Preparing Empty Lists
    album_uri = []
    disc_number = []
    duration_ms = []
    track_id = []
    track_name = []
    track_number = []
    track_type = []
    track_uri = []
    track_markets = []
    artist_uri = []
    name = []
    
    # Loop
    for album in album_uri_loop:
        tracks = sp.album_tracks(album_id=album, offset=0)
        for track in range(len(tracks['items'])):
            
            album_uri.append(album)
            disc_number.append(tracks['items'][track]['disc_number'])
            duration_ms.append(tracks['items'][track]['duration_ms'])
            track_id.append(tracks['items'][track]['id'])
            track_name.append(tracks['items'][track]['name'])     
            track_number.append(tracks['items'][track]['track_number'])
            track_type.append(tracks['items'][track]['type'])
            track_uri.append(tracks['items'][track]['duration_ms'])
            track_markets.append(tracks['items'][track]['duration_ms'])
            artist_uri.append(tracks['items'][track]['artists'][0]['uri'])
            name.append(tracks['items'][track]['artists'][0]['name'])
     
    track_df = pd.DataFrame({
        'album_uri': album_uri,
        'disc_number': disc_number,
        'duration_ms': duration_ms,
        'track_id': track_id,
        'track_name': track_name,
        'track_type': track_type,
        'track_uri': track_uri,        
        'track_markets': track_markets,
        'artist_uri': artist_uri,
        'artist': name
        })
    
    return track_df

#%%
        
# Create Top Tracks Table - By Country
        
# 'available_markets': ['AD', 'AE', 'AR', 'AT', 'AU', 'BE', 'BG', 'BH', 'BO', 'BR', 'CA', 'CH', 'CL', 'CO', 'CR', 'CY', 'CZ', 'DE', 'DK', 'DO', 'DZ', 'EC', 'EE', 'EG', 'ES', 'FI', 'FR', 'GB', 'GR', 'GT', 'HK', 'HN', 'HU', 'ID', 'IE', 'IL', 'IN', 'IS', 'IT', 'JO', 'JP', 'KW', 'LB', 'LI', 'LT', 'LU', 'LV', 'MA', 'MC', 'MT', 'MX', 'MY', 'NI', 'NL', 'NO', 'NZ', 'OM', 'PA', 'PE', 'PH', 'PL', 'PS', 'PT', 'PY', 'QA', 'RO', 'SA', 'SE', 'SG', 'SK', 'SV', 'TH', 'TN', 'TR', 'TW', 'US', 'UY', 'VN', 'ZA']

#%%

# Inputs 
client_id = client_id
client_secret = client_secret
artist = 'The Beatles'

#%%

# ETL Process
sp = spotify_connect(client_id, client_secret)
uri = uri_artist(artist, sp)
track_df = track_df(uri, sp)
album_df = album_df(uri, sp)
     
#%%
# Sources 
# https://spotipy.readthedocs.io/en/2.12.0/
# https://medium.com/@RareLoot/extracting-spotify-data-on-your-favourite-artist-via-python-d58bc92a4330
        
    


