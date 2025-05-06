"""
The script do batch rename from 
original hindcast to cefi format

!!!!!!!!!!!!!!!!!!! WARNING !!!!!!!!!!!!!!!!!!!
original file name must follow the following pattern
to accurately get the needed info to new file attrs
!!!!!!!!!!!!!!!!!!! WARNING !!!!!!!!!!!!!!!!!!!
orignal naming format:
ocean_cobalt_daily_2d.19930101-20191231.btm_o2.nc
<model_module>.<date_range>.<variable>.nc

cefi naming format:
<variable>.<region>.<subdomain>.<experiment_type>
.<version>.<output_frequency>.<grid_type>.<YYYY0M-YYYY0M>.nc

also perform using nco?
- add file attribute relative data path
- add file attribute original file name
- add file attribute new file name
- add file attribute GFDL archive path?
- rechunk and compress

"""
import os
import sys
import json
import glob
import shutil
import subprocess
import xarray as xr
from mom6.data_structure import portal_data
from mom6.mom6_module.util import load_json

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
    region_dir = dict_setting['region_dir']
    region_file = dict_setting['region_file']
    subdomain_dir = dict_setting['subdomain_dir']
    subdomain_file = dict_setting['subdomain_file']
    grid_type = dict_setting['grid_type']
    experiment_type = dict_setting['experiment_type']
    experiment_name = dict_setting['experiment_name']
    data_doi = dict_setting['data_doi']
    paper_doi = dict_setting['paper_doi']


    # loop through all file in the original path
    all_new_dir = []
    # initialize static files and file name
    static_file = None
    static_filename = None
    for file in glob.glob(f'{ori_path}/*.nc'):
        if 'ocean_static.nc' not in file:
            # get all dir names and file name
            file_path_format = file.split('/')
            # Remove empty strings and strings with only spaces
            file_path_format = [name for name in file_path_format if name.strip()]
            # get file name
            filename = file_path_format[-1]

            # each file decipher the format to make sure the file type
            file_format_list = filename.split('.')
            variable = file_format_list[2]
            date_range = file_format_list[1]

            # find date_range, output_freq, dir_path
            # based on original date_range format
            if len(date_range) == 13:
                OUTPUT_FREQ = 'monthly'
            elif len(date_range) == 13+4:
                OUTPUT_FREQ = 'daily'
                date_range = f"{date_range[0:0+6]}-{date_range[9:9+6]}"
            else:
                print('new date_range format not consider')
                print(f'{file} skipped' )
                continue

            # determine the data path
            cefi_rel_path = portal_data.DataPath(
                region=region_dir,
                subdomain=subdomain_dir,
                experiment_type=experiment_type,
                output_frequency=OUTPUT_FREQ,
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
            filename = portal_data.HindcastFilename(
                variable=variable,
                region=region_file,
                subdomain=subdomain_file,
                output_frequency=OUTPUT_FREQ,
                date_range=date_range,
                grid_type=grid_type,
                release=release_date
            ).filename

            # define new global attribute
            file_global_attrs = portal_data.GlobalAttrs(
                cefi_rel_path = cefi_rel_path,
                cefi_filename = filename,
                cefi_variable = variable,
                cefi_ori_filename = file.split('/')[-1],
                cefi_ori_category = file.split('/')[-1].split('.')[0],
                cefi_archive_version = archive_version,
                cefi_region = region_file,
                cefi_subdomain = subdomain_file,
                cefi_experiment_type = experiment_type,
                cefi_experiment_name = experiment_name,
                cefi_release = release_date,
                cefi_output_frequency = OUTPUT_FREQ,
                cefi_grid_type = grid_type,
                cefi_date_range = date_range,
                cefi_data_doi = data_doi,
                cefi_paper_doi = paper_doi
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

    with open(log_filename, "w", encoding='utf-8') as log_file:
        sys.stdout = log_file
        sys.stderr = log_file

        # Load the settings
        dict_json = load_json(json_setting)

        # preprocessing the file to cefi format
        cefi_preprocess(dict_json)

    # Reset to default after exiting the context manager
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__
