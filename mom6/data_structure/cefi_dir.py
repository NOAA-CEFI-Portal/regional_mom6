import os
import sys
import json


def get_cefi_structure(source_root:str)->dict:
    """Generate a dictionary representing the CEFI data structure

    Parameters
    ----------
    source_root : str
        root directory where the data structure starts

    Returns
    -------
    dict
        the data structure start from the root directory
    """
    folder_structure = {}

    for root, _, _ in os.walk(source_root):
        # find relative path of each /cefi/data/directories/
        relative_path = os.path.relpath(root, source_root)

        # start from the stored structure
        iter_structure = folder_structure

        # iterate through all subfolders for each /cefi/data/directories/
        for part in relative_path.split(os.sep):
            # the loop reassigned the iter_structure to empty dict {} in each loop
            iter_structure = iter_structure.setdefault(part, {})

    return folder_structure


def create_cefi_structure(destination_root:str, source_json:str):
    """Recreate the folder structure described in a JSON file at `destination_root`.

    Parameters
    ----------
    destination_root : str
        root directory where the data structure is going to be recreated
    """
    def recursive_mkdir(dict_structure, current_path):
        for current_dir, dict_subdirs in dict_structure.items():
            # Create the folder at the current level
            path_with_current_dir = os.path.join(current_path, current_dir)

            # Check if the directory already exists
            if not os.path.exists(path_with_current_dir):
                print(f"Creating directory: {path_with_current_dir}")
                # Create the directory
                os.makedirs(path_with_current_dir, exist_ok=True)
            else:
                print(f"Directory already exists: {path_with_current_dir}")

            # Recurse into subfolders
            recursive_mkdir(dict_subdirs, path_with_current_dir)

    # Load the JSON structure
    with open(source_json, 'r', encoding='utf-8') as file:
        folder_structure = json.load(file)

    # Start creating folders
    recursive_mkdir(folder_structure, destination_root)


if __name__=="__main__":
    if len(sys.argv) != 2:
        sys.exit('Usage: python script.py get/create')

    # Usage: python script.py get
    if sys.argv[1] == 'get':
        structure = get_cefi_structure(source_root='/Projects/CEFI/regional_mom6/cefi_portal/')

        # Save the structure to a JSON file
        with open('cefi_data_structure.json', 'w', encoding='utf-8') as f:
            json.dump(structure, f, indent=4)
            print("Folder structure saved to cefi_data_structure.json")

    # Usage: python script.py create
    elif sys.argv[1] == 'create':

        # Ask for input in the command line
        dest_path = input("Enter the directory path to place the CEFI data:")

        if os.path.exists(dest_path):
            # make the top level dir to include all subdirs
            dest_path = os.path.join(dest_path, 'cefi_portal')

            # Check if the directory already exists
            if not os.path.exists(dest_path):
                print(f"Creating top directory: {dest_path}")
                # Create the directory
                os.makedirs(dest_path, exist_ok=True)
            else:
                print(f"Top directory already exists: {dest_path}")

            create_cefi_structure(
                destination_root=dest_path,
                source_json='cefi_data_structure.json'
            )
        else:
            sys.exit("Entered directory path does not exist")

    else:
        sys.exit('Usage: python script.py get/create')
