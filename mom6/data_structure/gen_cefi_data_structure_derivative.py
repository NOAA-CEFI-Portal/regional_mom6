"""
This script
create cefi data directories structure under
'/Projects/CEFI/regional_mom6/' till before release_date

the top level direcory name including the entire data structure is 
'cefi_derivative/' => setup in the portal_data.DataStructure.top_directory_derivative


used for first creation/recreation if the structure has big changes

"""
import os
from itertools import product
from mom6.data_structure.portal_data import (
    DataStructure
)

def create_directory_structure(base_dir:str):
    """Generate the data structure needed for
    storing the cefi data locally

    Parameters
    ----------
    base_dir : str
        the base directory where the user wish 
        the cefi data struture to be located
    """
    data_structure = DataStructure()
    # Iterate through all combinations of available value in attributes
    for combination in product(
        data_structure.top_directory_derivative,
        data_structure.region,
        data_structure.subdomain,
        data_structure.experiment_type,
        data_structure.output_frequency,
        data_structure.grid_type
    ):
        # Build the directory path
        dir_path = os.path.join(base_dir, *combination)

        # Check if the directory already exists
        if not os.path.exists(dir_path):
            print(f"Creating directory: {dir_path}")
            # Create the directory (creates intermediate dirs if they don't exist)
            os.makedirs(dir_path, exist_ok=True)
        else:
            print(f"Directory already exists: {dir_path}")

if __name__=="__main__":
    # create the CEFI data structure hierarchy to store the data
    create_directory_structure(base_dir='/Projects/CEFI/regional_mom6/')
