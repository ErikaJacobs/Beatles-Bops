# Pull Track Popularity from Spotify API

def track_pop_table(track_df, uri, sp):
    
    import pandas as pd

    tracklist = track_df['track_uri'].to_list()     
    tracks = []
    
    for track in tracklist:
        if track not in tracks:
            tracks.append(track)
    
    track_uri = []
    track_pop = []
    
    for track in tracks:
        
        track = sp.track(track)
        
        track_uri.append(track['uri'])
        track_pop.append(track['popularity'])

    track_pop_df = pd.DataFrame({
            'track_uri': track_uri,
            'track_pop': track_pop,
            })
    return track_pop_df