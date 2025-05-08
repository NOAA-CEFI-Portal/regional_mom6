"""
The script do batch fixing the encoding for some int64

"""
import sys
import os
import json
import logging
import glob
import xarray as xr
import mom6.mom6_module.util as util


if __name__ == '__main__':
    # read the config filename
    config_file = sys.argv[1]

    # load the config file
    config = util.load_json(config_file)

    # set up logfile name
    log_file = util.log_filename(config_file)
    # remove log file if exist
    if os.path.isfile(log_file):
        os.remove(log_file)

    # set up logging
    util.setup_logging(log_file)

    try:
        data_paths = config['data_paths']
    except (FileNotFoundError, json.JSONDecodeError) as e:
        ERROR_MSG = f"ERROR: Could not read json config file '{config_file}': {e}"
        logging.error(ERROR_MSG)
        raise e

    for path in data_paths:
        logging.info(' ===== Processing file in %s',path)
        test_path = os.path.join(path,'test')

        files = glob.glob(os.path.join(test_path,'*.nc'))
        if len(files) == 0:
            logging.error('No netCDF files in %s',test_path)
            continue

        for file in files:
            filename = os.path.basename(file)

            logging.info('Processing %s',file)
            
            # test if the file is corrupted
            ds = xr.open_dataset(file)
            
            # perform the save cp operation
            src_file = os.path.join(test_path,filename)
            dest_file = os.path.join(path,filename)
            cp_flag = util.safe_overwrite(src_file,dest_file)
            if not cp_flag:
                logging.error('File %s is corrupted',src_file)
                continue
            logging.info('File %s copied to %s',src_file,dest_file)

    logging.info('============== batch_safe_copy.py complete ==============')

