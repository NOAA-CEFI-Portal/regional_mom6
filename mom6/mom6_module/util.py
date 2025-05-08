import os
import json
import sys
import logging
import hashlib
import shutil
from pathlib import Path


def sha256sum(file_path, block_size=65536):
    """checksum a file using sha256"""
    h = hashlib.sha256()
    with open(file_path, 'rb') as f:
        while chunk := f.read(block_size):
            h.update(chunk)
    return h.hexdigest()

def safe_overwrite(
    source_path:str,
    destination_path:str
) -> bool:
    """safe overwrite a file using atomic move
    1. copy the source file to a temporary file
    2. verify the checksum of the temporary file
    3. if the checksum does not match, delete the temporary file return False
    4. if the checksum matches, replace the destination file with the temporary file return True
    
    Parameters
    ----------
    src_path : str
        Path to the source file to be copied.
    dest_path : str
        Path to the destination file to be replaced.

    Returns
    -------
    bool
        True if the operation was successful, False otherwise.
    """
    src = Path(source_path)
    dest = Path(destination_path)
    tmp_dest = dest.with_suffix(dest.suffix + ".new")

    if not src.exists():
        raise FileNotFoundError(f"Source file {src} does not exist.")

    logging.info("Copying %s to %s", src, tmp_dest)
    shutil.copy2(src, tmp_dest)

    logging.info("Verifying checksum...")
    if sha256sum(src) != sha256sum(tmp_dest):
        logging.error("❌ Checksum mismatch! Aborting.")
        tmp_dest.unlink(missing_ok=True)
        return False

    logging.info("✅ Checksums match. Replacing destination file.")
    tmp_dest.replace(dest)  # Atomic move

    return True


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
        script_location = os.path.dirname(script_location)
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
