# Pull Artist Information from Spotify API

def artist_table(artist, sp):
    
    import pandas as pd
    
    name = artist

    # Search For Artist
    result = sp.search({name})

    #Extract Artist's URI
    uri_artist = result['tracks']['items'][0]['artists'][0]['uri']
    
    artist_df = pd.DataFrame({
        'artist_uri': [uri_artist],
        'artist_name': [name]
        })
    
    return artist_df