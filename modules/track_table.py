# Pull Track Information from Spotify API

def track_table(uri, sp):
    
    import pandas as pd
    
    # Album List
    albums = sp.artist_albums(uri)
    album_uri_loop = []
    
    for album in range(len(albums['items'])):
        album_uri_loop.append(albums['items'][album]['uri'])
    
    # Preparing Empty Lists
    album_uri = []
    disc_number = []
    duration_ms = []
    track_name = []
    track_number = []
    track_uri = []
    artist_uri = []
    
    # Loop
    for album in album_uri_loop:
        tracks = sp.album_tracks(album_id=album, offset=0)
        for track in range(len(tracks['items'])):
            
            album_uri.append(album)
            disc_number.append(tracks['items'][track]['disc_number'])
            duration_ms.append(tracks['items'][track]['duration_ms'])
            track_name.append(tracks['items'][track]['name'])     
            track_number.append(tracks['items'][track]['track_number'])
            track_uri.append(tracks['items'][track]['uri'])
            artist_uri.append(tracks['items'][track]['artists'][0]['uri'])
     
    track_df = pd.DataFrame({
        'track_uri': track_uri,   
        'track_name': track_name,
        'track_number': track_number,
        'duration_ms': duration_ms,
        'disc_number': disc_number,
        'album_uri': album_uri,
        'artist_uri': artist_uri
        })
    
    return track_df