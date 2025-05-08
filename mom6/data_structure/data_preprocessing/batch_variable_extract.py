"""
Extract a list of variable from a netcdf file and save them to a new netcdf file.
"""
import os
import glob
import xarray as xr
import logging
from datetime import datetime
import sys

def setup_logging():
    """Setup logging to both file and console in script directory"""
    # Get script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Create logs directory if it doesn't exist
    log_dir = os.path.join(script_dir, 'logs')
    os.makedirs(log_dir, exist_ok=True)

    # Create a log filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f'variable_extract_{timestamp}.log')

    # Setup logging configuration
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()  # This will print to console
        ]
    )
    return log_file

if __name__ == "__main__":
    # Setup logging first
    log_file = setup_logging()
    logging.info("Log file created at: %s", log_file)

    # path list
    path_list = [
        '/gpfs/f5/cefi/scratch/Chia-wei.Hsu/ESM4_SSP126_v2/',
        '/gpfs/f5/cefi/scratch/Chia-wei.Hsu/ESM4_SSP245_v2/',
        '/gpfs/f5/cefi/scratch/Chia-wei.Hsu/ESM4_SSP370_v2/',
        '/gpfs/f5/cefi/scratch/Chia-wei.Hsu/ESM4_SSP585_v2/'   
    ]

    # extract varible list
    ext_var_list = ['ssv','ssu','ssh','tos']

    for path in path_list:
        # find all netcdf files
        files = glob.glob(path+'*.ocean_daily.nc')
        files = sorted(files)

        # create directory to save the new netcdf files
        output_dir = path + 'extracted_variables/'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        logging.info("Starting variable extraction process")
        logging.info("Output directory: %s", output_dir)

        for file in files:
            logging.info("============= Processing =============")
            logging.info("Processing file: %s", file)
            filename = os.path.basename(file)

            try:
                # open the netcdf file
                ds = xr.open_dataset(file)

                # extract the variable list
                for var in ext_var_list:
                    logging.info("Extracting variable: %s", var)
                    try:
                        # create dataset that contain the global attributes
                        ds_new = xr.Dataset()
                        ds_new.attrs = ds.attrs
                        ds_new[var] = ds[var]

                        # save the new dataset to a netcdf file
                        output_filename = filename.replace('.ocean_daily.nc', f'.ocean_daily.{var}.nc')
                        output_path = os.path.join(output_dir, output_filename)
                        ds_new.to_netcdf(output_path)
                        logging.info("Successfully saved: %s", output_filename)
                        del ds_new
                    except Exception as e:
                        logging.error("Error processing variable %s: %s", var, str(e))

                # close the netcdf file
                ds.close()
            except Exception as e:
                logging.error("Error processing file %s: %s", file, str(e))

        logging.info("Completed processing for path: %s", path)
