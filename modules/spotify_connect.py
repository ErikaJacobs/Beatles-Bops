# Connect To Spotify

def spotify_connect(client_id, client_secret):
    # Import Packages
    import spotipy
    import spotipy.util
    from spotipy.oauth2 import SpotifyClientCredentials #To access authorised Spotify data

    # Connect to Spotify
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    return sp