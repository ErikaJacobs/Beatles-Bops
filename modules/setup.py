import os
import configparser

class Setup:
    
    def __init__(self):
        self.path = os.path.dirname(os.path.realpath(__file__)).replace('modules', '')
        self.configs = {}
 
    def config(self):
        # Config File Set-Up
        Config = configparser.ConfigParser()
        Config.read(self.path+"\config.ini")
        config_list = Config.options('Project')
    
        for config in config_list:
            self.configs[config] = Config.get('Project', config)
        
        self.spotify_credentials()
            
    def spotify_credentials(self):
        # Spotify Credentials
        self.configs['client_id'] = os.environ.get('spotify_id')
        self.configs['client_secret'] = os.environ.get('spotify_secret')
