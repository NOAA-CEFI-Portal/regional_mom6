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
        files = glob.glob(os.path.join(path,'*.nc'))
        if len(files) == 0:
            logging.error('No netCDF files in %s',path)
            continue
        
        # create test directory if not exist
        if not os.path.exists(os.path.join(path,'test')):
            os.makedirs(os.path.join(path,'test'))
        
        for file in files:
            filename = os.path.basename(file)
            if 'static' in file:
                logging.info('Skipping %s',file)
                continue

            logging.info('Processing %s',file)
            ds = xr.open_dataset(file)
            ds['init'].encoding['dtype'] = 'int32'
            ds['lead'].encoding['dtype'] = 'int32'
            ds['member'].encoding['dtype'] = 'int32'

            output_file = os.path.join(path,'test',filename)
            
            if os.path.isfile(output_file):
                try:
                    ds_test = xr.open_dataset(output_file)
                    logging.info('File %s already exist',output_file)
                    continue
                except IOError:
                    logging.info('File %s is corrupted',output_file)
                    # remove the corrupted file
                    logging.info('Removing %s',output_file)
                    os.remove(output_file)
            logging.info('Writing %s',output_file)
            ds.to_netcdf(output_file)

    logging.info('============== batch_fix_int64.py complete ==============')

