"""
This script is designed to do batch climatology of the 
regional mom6 output using the new mom6_read module

!!!! possible daily calculation can only be done 
one file at a time due to the memory limitation

"""
import os
import sys
import logging
import warnings
import xarray as xr
from dask.distributed import Client
from mom6_rotate_batch import output_processed_data
from mom6.data_structure import portal_data
from mom6.mom6_module.mom6_read import AccessFiles
from mom6.mom6_module.mom6_export import mom6_encode_attr
from mom6.data_structure.batch_preprocess_hindcast import load_json
from mom6.data_structure.portal_data import DataStructure
from mom6.mom6_module.mom6_statistics import HindcastClimatology, ForecastClimatology


def climo_batch(dict_json: dict, logger_object) -> xr.Dataset:
    """perform the batch climatology of the mom6 output

    Parameters
    ----------
    dict_json : dict
        dictionary that contain the constant setting in json

    Returns
    -------
    xr.Dataset
        regridded dataset
    """

    local_top_dir = dict_json['local_top_dir']
    region = dict_json['region']
    subdomain = dict_json['subdomain']
    experiment_type = dict_json['experiment_type']
    output_frequency = dict_json['output_frequency']
    grid_type = dict_json['grid_type']
    release = dict_json['release']
    data_source = dict_json['data_source']
    load = dict_json['load_data_to_memory']

    # determine the data path
    output_cefi_rel_path = portal_data.DataPath(
        top_directory=DataStructure().top_directory_derivative[0],
        region=region,
        subdomain=subdomain,
        experiment_type=experiment_type,
        output_frequency=output_frequency,
        grid_type=grid_type,
        release=release
    ).cefi_dir

    # create the output directory
    output_dir = os.path.join(local_top_dir, output_cefi_rel_path, 'climatology')

    # Check if the release directory already exists
    if not os.path.exists(output_dir):
        logger_object.info(f"Creating release folder in last level of derivative: {output_dir}")
        # Create the directory
        os.makedirs(output_dir, exist_ok=True)
    else:
        logger_object.info(
            f"release folder already exists in last level of derivative: {output_dir}"
        )

    # get all files in the experiment
    local_access = AccessFiles(
        local_top_dir=local_top_dir,
        region=region,
        subdomain=subdomain,
        experiment_type=experiment_type,
        output_frequency=output_frequency,
        grid_type=grid_type,
        release=release,
        data_source=data_source
    )

    if dict_json['experiment_type'] == 'hindcast':
        allfile_list = local_access.get()

        # loop through all file in the original path
        for file in allfile_list:
            # try to avoid the static file
            if 'static' not in file:
                # open the file
                with xr.open_dataset(file, chunks={}) as ds_var:
                    varname = ds_var.attrs['cefi_variable']
                    new_varname = f'{varname}_climatology'

                    # create new filename based on original filename
                    filename = ds_var.attrs['cefi_filename']
                    filename_seg = filename.split('.')
                    filename_seg[0] = new_varname
                    new_filename = '.'.join(filename_seg)

                    # find if new file name already exist
                    new_file = os.path.join(output_dir, new_filename)
                    if os.path.exists(new_file):
                        logger_object.info(f"{new_file}: already exists. skipping...")
                    else:
                        if load:
                            ds_var = ds_var.load()
                        # find the variable dimension info (for chunking)
                        logger_object.info(f"processing {new_file}")

                        # call climatology class
                        # calculate climatology of hindcast
                        class_climo = HindcastClimatology(
                            ds_data=ds_var,
                            var_name=varname,
                            time_name='time',
                            time_frequency=dict_json['output']['climatology_groupby_frequency']
                        )

                        # calculate climatology
                        da_climo = class_climo.generate_climo(
                            climo_start_year=dict_json['output']['climatology_start_year'],
                            climo_end_year=dict_json['output']['climatology_end_year'],
                            dask_option='persist'
                        )

                        # create output dataset
                        ds_climo = xr.Dataset()
                        ds_climo[new_varname] = da_climo

                        # copy the encoding and attributes
                        ds_climo = mom6_encode_attr(ds_var, ds_climo, var_names=[new_varname])

                        # redefine new global attribute
                        # global attributes
                        ds_climo.attrs['cefi_rel_path'] = output_dir
                        ds_climo.attrs['cefi_filename'] = new_filename
                        ds_climo.attrs['cefi_variable'] = new_varname

                        # output the processed data
                        output_processed_data(
                            ds_climo,
                            top_dir=dict_json['local_top_dir'],
                            dict_json_output=dict_json['output']
                        )

    elif 'reforecast' in dict_json['experiment_type']:

        allfile_list = local_access.get()

        # find unique variables
        allvar_list = []
        for file in allfile_list:
            filename = file.split('/')[-1]
            variable_name = filename.split('.')[0]
            # try to avoid the static file
            if 'static' not in variable_name:
                allvar_list.append(variable_name)
        unique_var_list = list(set(allvar_list))

        # loop through all file in the original path
        for var in unique_var_list:
            # open the file
            single_var_file_list = local_access.get(variable=var)
            with xr.open_mfdataset(
                single_var_file_list, combine='nested', concat_dim='init', chunks={}
            ) as ds_var:
                varname = ds_var.attrs['cefi_variable']
                new_varname = f'{varname}_climatology'

                # create new filename based on original filename
                filename = ds_var.attrs['cefi_filename']
                filename_seg = filename.split('.')
                filename_seg[0] = new_varname
                filename_seg.pop(-2)  # remove initial time
                new_filename = '.'.join(filename_seg)

                # find if new file name already exist
                new_file = os.path.join(output_dir, new_filename)
                if os.path.exists(new_file):
                    logger_object.info(f"{new_file}: already exists. skipping...")
                else:
                    if load:
                        ds_var = ds_var.load()
                    # find the variable dimension info (for chunking)
                    logger_object.info(f"processing {new_file}")

                    # calculate climatology of reforecast
                    class_climo = ForecastClimatology(
                        ds_data=ds_var,
                        var_name=varname,
                        time_frequency=dict_json['output']['climatology_groupby_frequency'])

                    # calculate climatology
                    da_climo = class_climo.generate_climo(
                        climo_start_year=dict_json['output']['climatology_start_year'],
                        climo_end_year=dict_json['output']['climatology_end_year'],
                        dask_option='persist'
                    )

                    # create output dataset
                    ds_climo = xr.Dataset()
                    ds_climo[new_varname] = da_climo

                    # copy the encoding and attributes
                    ds_climo = mom6_encode_attr(ds_var, ds_climo, var_names=[new_varname])

                    # redefine new global attribute
                    # global attributes
                    ds_climo.attrs['cefi_rel_path'] = output_dir
                    ds_climo.attrs['cefi_filename'] = new_filename
                    ds_climo.attrs['cefi_variable'] = new_varname
                    ds_climo.attrs['cefi_init_date'] = "entire reforecast"

                    # output the processed data
                    output_processed_data(
                        ds_climo,
                        top_dir=dict_json['local_top_dir'],
                        dict_json_output=dict_json['output']
                    )
    else:
        raise ValueError('experiment_type must be either hindcast or reforecast')

if __name__ == "__main__":

    warnings.simplefilter("ignore")
    client = Client(processes=False, memory_limit='500GB', silence_logs=50)
    print(client)
    print(client.cluster.dashboard_link)

    # Ensure a JSON file is provided as an argument
    if len(sys.argv) < 2:
        print("Usage: python mom6_climo_batch.py xxxx.json")
        sys.exit(1)

    # Get the JSON file path from command-line arguments
    json_setting = sys.argv[1]

    current_location = os.path.dirname(os.path.abspath(__file__))
    log_name = sys.argv[1].split('.')[0] + '.log'
    log_filename = os.path.join(current_location, log_name)

    # remove previous log file if exists
    if os.path.exists(log_filename):
        os.remove(log_filename)

    # Configure logging to write to both console and log file
    logging.basicConfig(
        level=logging.INFO,  # Log INFO and above
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_filename),  # Log to file
            logging.StreamHandler()  # Log to console
        ]
    )
    logger = logging.getLogger()

    try:
        # Load the settings
        dict_json1 = load_json(json_setting, json_path=current_location)

        # preprocessing the file to cefi format
        climo_batch(dict_json1, logger)

    except Exception as e:
        logger.exception("An exception occurred")

    finally:
        logger.info("Climatology process finished.")