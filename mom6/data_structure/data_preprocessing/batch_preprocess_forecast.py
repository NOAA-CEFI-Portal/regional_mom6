"""
The script do batch rename from 
original reforecast to cefi format

orignal naming format:
tos_forecast_iYYYYMM.nc

cefi naming format:
<variable>.<region>.<subdomain>.<experiment_type>
.<version>.<output_frequency>.<grid_type>.<iYYYY0M>.nc

also perform using nco?
- add file attribute relative data path
- add file attribute original file name
- add file attribute new file name
- add file attribute GFDL archive path?
- rechunk and compress

"""
import os
import sys
import glob
import shutil
import logging
import subprocess
import xarray as xr
from mom6.data_structure import portal_data
from mom6.mom6_module.util import load_json


# Configure logging
def setup_logging(filename_log):
    logging.basicConfig(
        filename=filename_log,
        filemode='w',
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.DEBUG
    )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

    # Redirect stdout and stderr to the logging system
    sys.stdout = StreamToLogger(logging.getLogger('STDOUT'), logging.INFO)
    sys.stderr = StreamToLogger(logging.getLogger('STDERR'), logging.ERROR)

class StreamToLogger:
    def __init__(self, logger, log_level):
        self.logger = logger
        self.log_level = log_level
        self.linebuf = ''

    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.logger.log(self.log_level, line.rstrip())

    def flush(self):
        pass

def cefi_preprocess(dict_setting:dict):
    """preprocessing the file to CEFI format

    Parameters
    ----------
    dict_setting : dict
        the dictionary contain all the necessary setting
        for preprocess the file
    """
    # original data path
    ori_path = dict_setting['ori_path']

    # new cefi data path setting
    cefi_portal_base = dict_setting['cefi_portal_base']
    release_date = dict_setting['release_date']
    archive_version = dict_setting['archive_version']
    aux = dict_setting['aux']
    region_dir = dict_setting['region_dir']
    region_file = dict_setting['region_file']
    subdomain_dir = dict_setting['subdomain_dir']
    subdomain_file = dict_setting['subdomain_file']
    grid_type = dict_setting['grid_type']
    experiment_type_dir = dict_setting['experiment_type_dir']
    experiment_type_file = dict_setting['experiment_type_file']
    experiment_name = dict_setting['experiment_name']
    output_frequency = dict_setting['output_frequency']
    data_doi = dict_setting['data_doi']
    paper_doi = dict_setting['paper_doi']
    ensemble_info = dict_setting['ensemble_info']


    # loop through all file in the original path
    all_new_dir = []
    # initialize static files and file name
    static_file = None
    static_filename = None
    if len(glob.glob(f'{ori_path}/*.nc')) == 0:
        sys.exit('No *.nc files')

    for file in glob.glob(f'{ori_path}/*.nc'):
        if 'ocean_static.nc' not in file:
            # get all dir names and file name
            file_path_format = file.split('/')
            # Remove empty strings and strings with only spaces
            file_path_format = [name for name in file_path_format if name.strip()]
            # get file name
            filename = file_path_format[-1]

            # each file decipher the format to make sure the file type (CEFI style naming!!!!!!)
            file_format_list = filename.split('.')
            variable = file_format_list[0]
            initial_date = file_format_list[-2]

            # determine the data path
            cefi_rel_path = portal_data.DataPath(
                region=region_dir,
                subdomain=subdomain_dir,
                experiment_type=experiment_type_dir,
                output_frequency=output_frequency,
                grid_type=grid_type,
                release=release_date
            ).cefi_dir
            new_dir = os.path.join(cefi_portal_base,cefi_rel_path)

            # store all new directories for static copy
            if new_dir not in all_new_dir:
                all_new_dir.append(new_dir)

            # Check if the release directory already exists
            if not os.path.exists(new_dir):
                print(f"Creating release folder in last level: {new_dir}")
                # Create the directory
                os.makedirs(new_dir, exist_ok=True)
            else:
                print(f"release folder already exists: {new_dir}")

            # rename to the new format
            filename = portal_data.SeasonalForecastFilename(
                variable=variable,
                region=region_file,
                subdomain=subdomain_file,
                experiment_type=experiment_type_file,
                output_frequency=output_frequency,
                initial_date=initial_date,
                grid_type=grid_type,
                release=release_date,
                ensemble_info=ensemble_info
            ).filename

            # define new global attribute
            file_global_attrs = portal_data.GlobalAttrs(
                cefi_rel_path = cefi_rel_path,
                cefi_filename = filename,
                cefi_variable = variable,
                cefi_ori_filename = file.split('/')[-1],
                cefi_archive_version = archive_version,
                cefi_region = region_file,
                cefi_subdomain = subdomain_file,
                cefi_experiment_type = experiment_type_dir,
                cefi_experiment_name = experiment_name,
                cefi_release = release_date,
                cefi_output_frequency = output_frequency,
                cefi_grid_type = grid_type,
                cefi_init_date = initial_date,
                cefi_data_doi = data_doi,
                cefi_paper_doi = paper_doi,
                cefi_ensemble_info = ensemble_info,
                cefi_aux = aux
            )
            # new file location and name
            new_file = os.path.join(new_dir,filename)
            # find if new file name already exist
            if os.path.exists(new_file):
                print(f"{new_file}: already exists. skipping...")
            else:
                # find the variable dimension info (for chunking)
                print(f"processing {new_file}")
                ds = xr.open_dataset(file,chunks={})
                dims = list(ds[variable].dims)
                
                # assign chunk size for different dim
                #  chunk size design in portal_data.py
                chunks = []
                chunk_info = portal_data.FileChunking()
                for dim in dims:
                    if 'z' in dim :
                        chunks.append(chunk_info.vertical)
                    elif 'time' in dim:
                        chunks.append(chunk_info.time)
                    elif 'lead' in dim:
                        chunks.append(chunk_info.lead)
                    elif 'member' in dim:
                        chunks.append(chunk_info.member)
                    else:
                        chunks.append(chunk_info.horizontal)

                # NCO command for chunking
                nco_command = ['ncks','-h', '-4', '-L', '2']
                for ndim,dim in enumerate(dims):
                    nco_command += [
                        '--cnk_dmn', f'{dim},{chunks[ndim]}'
                    ]
                nco_command += [file, new_file]

                # Run the NCO command using subprocess
                try:
                    subprocess.run(nco_command, check=True)
                    # print(f'NCO rechunk and compress successfully. Output saved to {new_file}')
                except subprocess.CalledProcessError as e:
                    print(f'Error executing NCO command: {e}')


                # NCO command for adding global attribute
                for key, value in file_global_attrs.__dict__.items():
                    #ncatted -O -a source,global,a,c,"satellite_data" input.nc output.nc
                    nco_command = [
                        'ncatted', '-O', '-h', '-a',
                        f'{key},global,a,c,{value}',
                        new_file, new_file
                    ]

                    # Run the NCO command using subprocess
                    try:
                        subprocess.run(nco_command, check=True)
                        # print(f'NCO add attribute {key} successfully. Output saved to {new_file}')
                    except subprocess.CalledProcessError as e:
                        print(f'Error executing NCO command: {e}')

                # remove nco history
                nco_command = [
                    'ncatted', '-O', '-h', '-a', 'history,global,d,c,""',
                    new_file, new_file
                ]
                # Run the NCO command using subprocess
                try:
                    subprocess.run(nco_command, check=True)
                    # print(f'NCO remove history successfully. Output saved to {new_file}')
                except subprocess.CalledProcessError as e:
                    print(f'Error executing NCO command: {e}')

        else:
            # store any static files and file name
            static_file = file
            static_filename = file.split('/')[-1]

    # copy static file
    for new_dir in all_new_dir:
        if static_file is not None and static_filename is not None:
            # create new static file path and filename
            new_static = os.path.join(new_dir,static_filename)
            # copy static to the new folder only if it is not there
            if not os.path.exists(new_static):
                shutil.copy2(static_file, new_static)
                print('ocean_static.nc copying to...')
                print(new_static)
        else:
            print('static file not found so no static file at the new location')

    


if __name__ == "__main__":

    # Ensure a JSON file is provided as an argument
    if len(sys.argv) < 2:
        print("Usage: python batch_preprocess.py xxxx.json")
        sys.exit(1)

    # Get the JSON file path from command-line arguments
    json_setting = sys.argv[1]

    current_location = os.path.dirname(os.path.abspath(__file__))
    log_name = sys.argv[1].split('.')[0]+'.log'
    log_filename = os.path.join(current_location,log_name)

    setup_logging(log_filename)

    try:
        # Load the settings
        dict_json = load_json(json_setting)

        # preprocessing the file to cefi format
        cefi_preprocess(dict_json)

    except Exception as e:
        logging.exception("An error occurred during preprocessing")
