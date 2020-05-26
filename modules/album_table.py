# Pull Album Information from Spotify API

def album_table(uri, sp):
    
    albums = sp.artist_albums(uri, album_type = 'album')
    
    import pandas as pd
    
    album_uri=[]
    album_name=[]
    album_tracks=[]
    release_date=[]
    
    # Extract Data From API
    
    for album in range(len(albums['items'])):
        album_uri.append(albums['items'][album]['uri']) #
        album_name.append(albums['items'][album]['name']) #
        album_tracks.append(albums['items'][album]['total_tracks'])
        release_date.append(albums['items'][album]['release_date'])

    album_df = pd.DataFrame({
        'album_uri': album_uri,
        'album_name': album_name,
        'release_date': release_date,
        'album_tracks': album_tracks
        })
    
    return album_df

