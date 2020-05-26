# Beatles Bops - RUN FILE
# Spotify API Extraction

# NOTE ON INPUTS:
# Project is best designed for the band "The Beatles"
# Inputs below can be adjusted for any artist or PostGres database 
# Inputs can be adjusted in main.py module

# Input below for run.py repo file directory

####################################

# Input - Script Directory
file =  'C:/Users/cluel/Documents/GitHub/Beatles-Bops'

####################################

# Set Working Directory
import os
os.chdir(file)

# Import Run Module
import modules.main as main

# Run Application
if __name__ == '__main__' :
    main.run()
    
#%%



