"""
The script do batch rename from 
original archived data to cefi format

TODO:
- need to be flexible to the original file name
- modulize the code
- make sure the member lead and init are in int32

output file name formet:
cefi naming format:
<variable>.<region>.<subdomain>.<experiment_type>
.<version>.<output_frequency>.<grid_type>.<iYYYY0M>.nc

also perform using nco
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
from mom6.mom6_module.util import load_json,setup_logging

def seperater_static_file(file_list:list):
    """seperater static file from the list of files"""
    # remove static file from the list
    static_files = []
    variable_files = []
    for file in file_list:
        if file in portal_data.StaticFile.filenames:
            static_files.append(file)
        else:
            variable_files.append(file)

    return {
        'statics':static_files,
        'variables':variable_files
    }

def archive_filename_decipher(experiement_type:str, filename:str):
    """decipher the archive file name to get essential information

    !!! filename info need to be seperate by '.'


    Parameters
    ----------
    experiement_type : str
        experiement type
    filename : str
        filename to decipher
    """
    if experiement_type == 'hindcast':
        # direct model pp output transfer
        info_list = os.path.splitext(filename).split('.')

        variable = info_list[2]
        date_range = info_list[1]
        archive_ori_category = info_list[0]

        # find frequency based on date range
        if len(date_range) == 13:
            output_frequency = 'monthly'
        elif len(date_range) == 13+4:
            output_frequency = 'daily'
            # reformat the date range for daily data to YYYYMM-YYYYMM
            date_range = f"{date_range[0:0+6]}-{date_range[9:9+6]}"
        else:
            logging.warning('%s skipped date_range format not consider',filename)
            
        return {
            'experiement_type' : experiement_type,
            'variable': variable,
            'date_range': date_range,
            'archive_category': archive_ori_category,
            'output_frequency': output_frequency,
            'initial_date': 'N/A'
        }
    elif 'forecast' in experiement_type:
        # forecast softlink/preprocessed to cefi name style
        info_list = os.path.splitext(filename).split('.')
        variable = info_list[0]
        initial_date = info_list[-1]
            
        # export None to use value in settings
        return {
            'experiement_type' : experiement_type,
            'variable': variable,
            'date_range': 'N/A',
            'archive_category': None,
            'output_frequency': None,
            'initial_date': initial_date
        }
    elif experiement_type == 'long_term_projection':
        # long_term_projection softlink/preprocessed to cefi name style
        info_list = os.path.splitext(filename).split('.')
        variable = info_list[0]
        date_range = info_list[-1]

        # export None to use value in settings
        return {
            'experiement_type' : experiement_type,
            'variable': variable,
            'date_range': date_range,
            'archive_category': None,
            'output_frequency': None,
            'initial_date': 'N/A'
        }
    


def cefi_preprocess(dict_setting:dict):
    """preprocessing the file to CEFI format

    Parameters
    ----------
    dict_setting : dict
        the dictionary contain all the necessary setting
        for preprocess the file
    """
    # unpack the dictionary to variables
    # archive data path in scratch
    ori_path = dict_setting['ori_path']
    # cefi data format setting
    archive_category = dict_setting['archive_category']
    cefi_portal_base = dict_setting['cefi_portal_base']
    release_date = dict_setting['release_date']
    archive_version = dict_setting['archive_version']
    aux = dict_setting['aux']
    region_dir = dict_setting['region_dir']
    region_file = dict_setting['region_file']
    subdomain_dir = dict_setting['subdomain_dir']
    subdomain_file = dict_setting['subdomain_file']
    grid_type = dict_setting['grid_type']
    output_frequency = dict_setting['output_frequency']
    experiment_type_dir = dict_setting['experiment_type_dir']
    experiment_type_file = dict_setting['experiment_type_file']
    experiment_name = dict_setting['experiment_name']
    data_doi = dict_setting['data_doi']
    paper_doi = dict_setting['paper_doi']
    ensemble_info = dict_setting['ensemble_info']
    forcing_info = dict_setting['forcing_info']

    # # loop through all file in the original path
    # all_new_dir = []
    # # initialize static files and file name
    # static_file = None
    # static_filename = None

    # find all netcdf file in the original path
    files = glob.glob(f'{ori_path}/*.nc')
    files.sort()

    # test if the path is correct
    if not os.path.exists(ori_path) :
        logging.error('Original path does not exist')
        sys.exit('No existing path')

    # test if the path is correct and with data
    if len(files) == 0:
        logging.error('No *.nc files found in the original path')
        sys.exit('No *.nc files')

    # remove static file from the list
    dict_files = seperater_static_file(files)
    static_files = dict_files['statics']
    variable_files = dict_files['variables']

    all_new_dir = []
    # loop through all variable files
    for file in variable_files:
        filename = os.path.basename(file)
        
        dict_info = archive_filename_decipher(experiment_type_dir,filename)
        ### fill missing info from the deciphered file name
        if dict_info['date_range'] is not None:
            date_range = dict_info['date_range']
        if dict_info['variable'] is not None:
            variable = dict_info['variable']
        if dict_info['experiement_type'] is not None:   
            experiment_type_dir = dict_info['experiement_type']
        if dict_info['initial_date'] is not None:
            initial_date = dict_info['initial_date']
        if dict_info['archive_category'] is not None:
            archive_category = dict_info['archive_category']
        if dict_info['output_frequency'] is not None:
            output_frequency = dict_info['output_frequency']


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
            logging.info(f"Creating release folder in last level: {new_dir}")
            # Create the directory
            os.makedirs(new_dir, exist_ok=True)
        else:
            logging.warning(f"release folder already exists: {new_dir}")

        # rename to the new format
        filename = portal_data.DecadalForecastFilename(
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
            cefi_ori_category = archive_category,
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
            logging.warning(f"{new_file}: already exists. skipping...")
        else:
            # find the variable dimension info (for chunking)
            logging.info(f"processing {new_file}")
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
                logging.error(f'Error executing NCO command: {e}')


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
                    logging.error(f'Error executing NCO command: {e}')

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
                logging.error(f'Error executing NCO command: {e}')

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
                logging.info('ocean_static.nc copying to...')
                logging.info(new_static)
        else:
            logging.warning('static file not found so no static file at the new location')

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

    if os.path.exists(log_filename):
        os.remove(log_filename)

    setup_logging(log_filename)

    try:
        # Load the settings
        dict_json = load_json(json_setting)

        # preprocessing the file to cefi format
        cefi_preprocess(dict_json)

    except Exception as e:
        logging.exception("An error occurred during preprocessing")
