"""
The script do batch rename from 
original reforecast to cefi format
The script put the data in the unverified folder NOT public

!!!!!!!!!!!!!!!!!!! WARNING !!!!!!!!!!!!!!!!!!!
ALSO : this script is designed to work with the
original reforecast file format that has all ensemble member and intialization time
seperated and same category variables combined provided by Dmitry.
!!!!!!!!!!!!!!!!!!! WARNING !!!!!!!!!!!!!!!!!!!

also perform using nco:
- add file attribute relative data path
- add file attribute original file name
- add file attribute new file name
- add file attribute GFDL archive path?
- rechunk and compress

"""
import os
import sys
import glob
import logging
import subprocess
import numpy as np
import xarray as xr
from mom6.data_structure import portal_data
from mom6.mom6_module.util import load_json,log_filename,setup_logging

def category_lookup(modified_category:str)->str:
    """category lookup for different modified category
    to original category used in cefi global attribute

    Returns
    -------
    str
        original category name

    Note
    --------
    add more if there is more modified category
    there are currently four modified category used by Dmitry
    """
    dict_cat = {
        "ocean2D_cobalt_btm":"ocean_cobalt_btm",
        "ocean2D_cobalt_tracers":"ocean_cobalt_tracers_int",
        "ocean2D_month":"ocean_monthly",
        "siconc_month":"ice_monthly",
    }

    return dict_cat[modified_category]


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
    ori_start_year = dict_setting['ori_start_year']
    ori_end_year = dict_setting['ori_end_year']

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
    process_file_tags = []

    # loop through year folder
    for year in range(ori_start_year,ori_end_year+1):
        year_path = ori_path.replace('YYYY',f'{year:04d}')
        if len(glob.glob(f'{year_path}/*.nc')) == 0:
            logging.info(f'No *.nc files found in {year_path}, skipping this year')
            continue

        for file in glob.glob(f'{year_path}/*.nc'):
            # get all dir names and file name
            file_path_format = file.split('/')
            # Remove empty strings and strings with only spaces
            file_path_format = [name for name in file_path_format if name.strip()]
            # get file name
            filename = file_path_format[-1]

            # Dmitry naming format "ocean2D_cobalt_btm_1993-10-e01.nc"
            filename_seg = filename.split('_')
            modified_cat = filename_seg[:-1]
            modified_cat = '_'.join(modified_cat)
            category = category_lookup(modified_cat)
            init_date = filename_seg[-1]
            init_date = init_date.split('-')
            iyear = int(init_date[0])
            imonth = int(init_date[1])
            # ensemble = init_date[2].split('.')[0]

            # process file tag
            process_file_tag = f"{modified_cat}_{iyear:04d}-{imonth:02d}"
            if process_file_tag in process_file_tags:
                logging.info(f'file {process_file_tag} already processed, skipping this file {file}')
                continue
            else:
                process_file_tags.append(process_file_tag)

            logging.info(f'processing files in tag {process_file_tag}')
            # determine the data path
            cefi_rel_path = portal_data.DataPath(
                top_directory=portal_data.DataStructure().top_directory_unverified[0],
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
                logging.info(f"release folder already exists: {new_dir}")

            # get file group
            list_ds = []
            for ens in range(1,10+1):
                list_ds.append(xr.open_dataset(f"{year_path}/{process_file_tag}-e{ens:02d}.nc",chunks='auto'))
            ds = xr.concat(list_ds,dim='member')
            ds['time'] = np.arange(0,12)
            ds['member'] = np.arange(1,11)
            ds = ds.rename({'time':'lead'})
            ds['init'] = np.datetime64(f'{iyear:04d}-{imonth:02d}-01')
            ds = ds.set_coords('init')
            ds = ds.set_coords('member')
            dims = list(ds.dims)

            # get all variables
            variables = list(ds.variables)
            # remove coord variables
            for coord in ds.coords:
                if coord in variables:
                    variables.remove(coord)
            # remove member, lead, init
            for coord in ['member','lead','init']:
                if coord in variables:
                    variables.remove(coord)

            # create individual variable file
            for variable in variables:
                ds_init = xr.Dataset()
                # copy attribute
                ds_init[variable] = ds[variable]
                ds_init.attrs = ds.attrs
                iyear = ds_init.init.dt.year.data
                imonth = ds_init.init.dt.month.data
                initial_date = f'i{iyear:04d}{imonth:02d}'
                init_file = f'{year_path}{variable}.{iyear:04d}{imonth:02d}.nc'

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
                    cefi_ori_category = category,
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
                    # create single initial file in scratch (removed later)
                    ds_init['init'].encoding['dtype'] = 'int32'
                    ds_init['member'].encoding['dtype'] = 'int32'
                    ds_init['lead'].encoding['dtype'] = 'int32'
                    ds_init.to_netcdf(init_file)

                    # find the variable dimension info (for chunking)
                    logging.info(f"processing {new_file}")

                    # assign chunk size for different dim
                    #  chunk size design in portal_data.py
                    chunks = []
                    chunk_info = portal_data.FileChunking()
                    for dim in dims:
                        if isinstance(dim, str) and 'z' in dim :
                            chunks.append(chunk_info.vertical)
                        elif isinstance(dim, str) and 'time' in dim:
                            chunks.append(chunk_info.time)
                        elif isinstance(dim, str) and 'lead' in dim:
                            chunks.append(chunk_info.lead)
                        elif isinstance(dim, str) and 'member' in dim:
                            chunks.append(chunk_info.member)
                        else:
                            chunks.append(chunk_info.horizontal)

                    # NCO command for chunking
                    nco_command = ['ncks','-h', '-4', '-L', '2']
                    for ndim,dim in enumerate(dims):
                        nco_command += [
                            '--cnk_dmn', f'{dim},{chunks[ndim]}'
                        ]
                    nco_command += [init_file, new_file]

                    # Run the NCO command using subprocess
                    try:
                        subprocess.run(nco_command, check=True)
                        # logging.info(f'NCO rechunk and compress successfully. Output saved to {new_file}')
                    except subprocess.CalledProcessError as e:
                        logging.error(f'Error executing NCO command: {e}')

                    # remove individual initial time netcdf file created
                    os.remove(init_file)

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
                            # logging.info(f'NCO add attribute {key} successfully. Output saved to {new_file}')
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
                        # logging.info(f'NCO remove history successfully. Output saved to {new_file}')
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

    # remove log file if already exist
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


    logging.info("Batch Dmitry format preprocessing finished.")
