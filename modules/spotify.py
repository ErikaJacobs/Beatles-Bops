import spotipy
import spotipy.util
from spotipy.oauth2 import SpotifyClientCredentials #To access authorised Spotify data
import pandas as pd
import numpy

class Spotify:
    
    def __init__(self, setup):
        # Connect to Spotify
        client_id = setup.configs['client_id']
        client_secret = setup.configs['client_secret']
        client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)

        self.sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
        self.name = setup.configs['artist']
    
    def artist_table(self):

        # Search For Artist
        result = self.sp.search({self.name})
    
        #Extract Artist's URI
        self.uri = result['tracks']['items'][0]['artists'][0]['uri']
        
        self.artist_df = pd.DataFrame({
            'artist_uri': [self.uri],
            'artist_name': [self.name]
            })
        
    def track_table(self):
        
        uri = self.uri
        sp = self.sp
        
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
                artist_uri.append(self.uri)
         
        self.track_df = pd.DataFrame({
            'track_uri': track_uri,   
            'track_name': track_name,
            'track_number': track_number,
            'duration_ms': duration_ms,
            'disc_number': disc_number,
            'album_uri': album_uri,
            'artist_uri': artist_uri
            })
    
    def album_table(self):
        uri = self.uri
        sp = self.sp
        
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
    
        self.album_df = pd.DataFrame({
            'album_uri': album_uri,
            'album_name': album_name,
            'release_date': release_date,
            'album_tracks': album_tracks
            }) 

    def track_pop_table(self):
        track_df = self.track_df
        sp = self.sp
        
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
    
        self.track_pop_df = pd.DataFrame({
                'track_uri': track_uri,
                'track_pop': track_pop,
                })

    def get_tables(self):
        self.artist_table()
        self.track_table()
        self.album_table()
        self.track_pop_table()
            