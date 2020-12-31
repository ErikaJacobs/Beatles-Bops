from modules2.setup import Setup

def run():    
    setup = Setup()
    setup.config()
    setup.spotify_credentials()
    print(setup.configs)