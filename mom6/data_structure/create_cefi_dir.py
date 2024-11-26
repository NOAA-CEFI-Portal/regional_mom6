"""
This script run the function to 
create cefi data directories structure

The base directory => DATA_BASE
is setup in the config file
"""
from mom6 import DATA_BASE
from mom6.data_structure.portal_data import (
    create_directory_structure
)

# create the CEFI data structure hierarchy to store the data
# DATA_BASE is set in the config file for local storage
create_directory_structure(base_dir=DATA_BASE)
