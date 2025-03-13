"""
The script do batch modification of attribute

"""
from typing import Literal
import os
import sys
import json
import logging
import glob
import subprocess

def change_attr_ncatted(
        ncfile: str,
        attr_name: str,
        attr_value: None,
        mode: Literal['m', 'a', 'd', 'c', 'o']
    ):
    """change the netCDF file attribute use ncatted command

    original ncatted command:
    ncatted -O -h -a attr_name,global,m,c,"attr_value" old.nc

    Parameters
    ----------
    ncfile : str
        the file name including the path
    attr_name : str
        the attribute name to be changed
    attr_value : str
        the attribute value to be changed
    mode : Literal['m', 'a', 'd', 'c', 'o']
        the mode of the operation.
        check https://linux.die.net/man/1/ncatted
        a (Append):
          Append value attr_value to current attr_name, if any. 
          If attr_name does not exist, there is no effect.
        c (Create): 
          Create attr_name with attr_value if attr_name does not yet exist. 
          If attribute attr_name exist already, there is no effect.
        d (Delete):
          Delete attr_name. If attr_name does not exist, there is no effect.
          When Delete mode is selected, attr_value arguments will not be considered.
        m (Modify):
          Change value of current attr_name value to attr_value.
          If attr_name does not exist, there is no effect.
        o (Overwrite):
          Write attribute attr_name with value attr_value,
          overwriting existing attribute attr_name, if any. This is the default mode.

    """
    if attr_value is None and mode != 'd':
        raise ValueError('attr_value must be provided when mode is not d')
    elif attr_value is not None and mode == 'd':
        print(f'{attr_value} is ignored due to mode is d')

    # change netcdf global attribute
    ncatted_command = [
        'ncatted', '-O', '-h', '-a',
        f'{attr_name},global,{mode},c,{attr_value}',
        ncfile
    ]
    # Run the NCO command using subprocess
    try:
        print(ncatted_command)
        subprocess.run(ncatted_command, check=True)
    except subprocess.CalledProcessError as e:
        print(f'Error executing NCO command: {e}')

    return


def setup_logging(logfile):
    """Set up logging to write messages to a log file."""
    logging.basicConfig(
        filename=logfile,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

def load_config(json_file):
    """Load directory configuration from a JSON file."""
    with open(json_file, 'r', encoding='utf-8') as jsonfile:
        return json.load(jsonfile)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python batch_change_attr.py <config_file.json>")
        sys.exit(1)

    config_file = sys.argv[1]

    # Derive the log file name from the config file
    log_file = os.path.splitext(config_file)[0] + ".log"

    # remove log file if exist
    if os.path.isfile(log_file):
        os.remove(log_file)

    try:
        # Set up logging
        setup_logging(log_file)

        # Load config
        config = load_config(config_file)
        attr_change_mode = config["attr_change_mode"]
        data_paths = config["data_paths"]
        attribute_names = config["attribute_names"]
        attribute_values = config["attribute_values"]
        skip_static = config["skip_static"]

        if len(attribute_names) != len(attribute_values):
            logging.error('attribute_names and attribute_values must have the same length')
            raise ValueError("attribute_names and attribute_values must have the same length")

        for path in data_paths:
            logging.info(' ===== Processing file in %s',path)
            files = glob.glob(os.path.join(path,'*.nc'))
            if len(files) == 0:
                logging.error('No netCDF files in %s',path)
                sys.exit(f'No file exist in {path}')
            else:
                for file in files:
                    if skip_static and 'static' in file:
                        logging.info('Skipping %s',file)
                        continue
                    logging.info('Processing %s',file)
                    for nattr,attribute_name in enumerate(attribute_names):

                        change_attr_ncatted(
                            file,
                            attribute_name,
                            attribute_values[nattr],
                            attr_change_mode
                        )
        logging.info('============== batch_change_attr.py complete ==============')
    except (FileNotFoundError, json.JSONDecodeError) as e:
        ERROR_MSG = f"ERROR: Could not read json config file '{config_file}': {e}"
        logging.error(ERROR_MSG)
        raise e
