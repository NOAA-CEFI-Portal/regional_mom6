import os
import json
import sys
import logging

def load_json(json_file:str,json_path:str=None)->dict:
    """ Load constant settings from a JSON file.

    Parameters
    ----------
    json_file : str
        Path to the JSON file containing settings.

    Returns
    -------
    settings : dict
        A dictionary of loaded settings.
    """
    if json_path is None:
        # refers to the script that was executed from the command line
        script_location = os.path.abspath(sys.argv[0])
        json_file_abs = os.path.join(script_location,json_file)
    else :
        json_file_abs = os.path.join(json_path,json_file)

    try:
        with open(json_file_abs, 'r', encoding='utf-8') as f:
            settings = json.load(f)
            print("Settings loaded successfully!")
            return settings
    except FileNotFoundError:
        print(f"Error: File '{json_file_abs}' not found.")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: Failed to parse JSON file '{json_file_abs}'.")
        sys.exit(1)

def log_filename(json_file:str)->str:
    """Generate a log filename based on the JSON file name.

    Parameters
    ----------
    json_file : str
        The name of the JSON file.

    Returns
    -------
    str
        The generated log filename.
    """
    # Get the base name of the JSON file without extension
    base_name = os.path.splitext(json_file)[0]
    
    # Create the log filename by appending '.log' to the base name
    return f"{base_name}.log"


def setup_logging(logfile):
    """Set up logging to write messages to a log file."""
    logging.basicConfig(
        filename=logfile,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
