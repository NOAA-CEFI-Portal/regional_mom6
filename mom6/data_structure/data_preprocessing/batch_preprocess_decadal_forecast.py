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
import numpy as np
import xarray as xr
from mom6.data_structure import portal_data
from mom6.mom6_module.util import load_json


def setup_logging(logfile):
    """Set up logging to write messages to a log file."""
    logging.basicConfig(
        filename=logfile,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

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
    archive_category = dict_setting['archive_category']
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


    # find all iYYYY, eE sub folders, and all variables
    list_iyear = []
    list_enss = []
    list_variables = []
    for iyear in glob.glob(f'{ori_path}/*/'):
        if os.path.isdir(iyear):
            iyear_folder_name = os.path.basename(os.path.normpath(iyear))
            list_iyear.append(iyear_folder_name)
            for ens in glob.glob(f'{iyear}/*/'):
                if os.path.isdir(ens):
                    ens_folder_name = os.path.basename(os.path.normpath(ens))
                    list_enss.append(ens_folder_name)
                    for file in glob.glob(f'{ens}/*.nc'):
                        if os.path.isfile(file):
                            # get the variable name
                            variable = os.path.basename(file).split('.')[-2]
                            if variable not in list_variables:
                                list_variables.append(variable)

    list_iyear = sorted(list(set(list_iyear)))
    list_enss = sorted(list(set(list_enss)))
    list_variables = sorted(list(set(list_variables)))

    # merge the data based on cefi grouping rule
    #  same initialization, all ensemble member, single variables
    for variable in list_variables:
        for iyear in list_iyear:
            list_merge_ds = []
            for ens in list_enss:
                # find all files for this combination
                data_path = os.path.join(ori_path,iyear,ens)
                files = glob.glob(f'{data_path}/*.{variable}.*')
                if len(files) == 1:
                    skip_iyear = False
                    ori_filename = os.path.basename(files[0])
                    ds = xr.open_dataset(files[0], chunks='auto', decode_timedelta=False)
                    ds['member'] = np.int32(ens[1:])
                    ds = ds.set_coords('member')
                    ds = ds.rename({'time': 'lead'})
                    list_merge_ds.append(ds)
                else:
                    # skip the ens loop
                    skip_iyear = True
                    logging.warning(f"Multiple or no files found for {variable} in {data_path}. Skipping this combination.")
                    break  # Exit ens loop
            if skip_iyear:
                logging.warning(f"Skipping year {iyear} for variable {variable}.")
                continue

            initial_date = f'{iyear}01'

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
                cefi_ori_filename = ori_filename,
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
                # merge ds on the coordinate member
                ds = xr.concat(list_merge_ds, dim='member').compute()
                ds = ds.sortby('member')
                ds.to_netcdf(os.path.join(new_dir,'temp.nc'))
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
                nco_command += [os.path.join(new_dir,'temp.nc'), new_file]



                # Run the NCO command using subprocess
                try:
                    subprocess.run(nco_command, check=True)
                    # print(f'NCO rechunk and compress successfully. Output saved to {new_file}')
                except subprocess.CalledProcessError as e:
                    logging.error(f'Error executing NCO command: {e}')
                    
                # remove temp file
                os.remove(os.path.join(new_dir,'temp.nc'))


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

    logging.info("Preprocessing finished.")
