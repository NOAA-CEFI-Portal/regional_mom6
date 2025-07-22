"""
This script is designed to do batch climatology of the 
regional mom6 output using the new mom6_read module

!!!! possible daily calculation can only be done 
one file at a time due to the memory limitation

"""
import os
import sys
import glob
import logging
import warnings
import xarray as xr
from dask.distributed import Client
from mom6_rotate_batch import output_processed_data
from mom6.data_structure import portal_data
from mom6.mom6_module.mom6_read import AccessFiles
from mom6.mom6_module.mom6_export import mom6_encode_attr
from mom6.mom6_module.util import load_json
from mom6.data_structure.portal_data import DataStructure
from mom6.mom6_module.mom6_forecast_tercile import Tercile


def tercile_probability_batch(dict_json:dict, logger_object):
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
    tercile_experiment_type = dict_json['tercile_experiment_type']
    output_frequency = dict_json['output_frequency']
    grid_type = dict_json['grid_type']
    release = dict_json['release']
    data_source = dict_json['data_source']
    load = dict_json['load_data_to_memory']

    # determine the output data path
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
    output_dir = os.path.join(local_top_dir, output_cefi_rel_path, 'tercile_probability')

    # Check if the release directory already exists
    if not os.path.exists(output_dir):
        logger_object.info(f"Creating release folder in last level of derivative: {output_dir}")
        # Create the directory
        os.makedirs(output_dir, exist_ok=True)
    else:
        logger_object.info(f"release folder already exists in last level of derivative: {output_dir}")

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

    if 'forecast' in dict_json['experiment_type']:

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
            for file in single_var_file_list:
                with xr.open_dataset(file, chunks={}) as ds_var:
                    variable = ds_var.attrs['cefi_variable']
                    for varname in [f'{variable}', f'{variable}_anom']:
                        new_varname = f'{varname}_tercile_probability'

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

                            # get tercile data path
                            tercile_cefi_rel_path = portal_data.DataPath(
                                top_directory=DataStructure().top_directory_derivative[0],
                                region=region,
                                subdomain=subdomain,
                                experiment_type=tercile_experiment_type,
                                output_frequency=output_frequency,
                                grid_type=grid_type,
                                release=release
                            ).cefi_dir

                            # tercile directory
                            tercile_dir = os.path.join(local_top_dir, tercile_cefi_rel_path, 'tercile')
                            tercile_file = glob.glob(os.path.join(tercile_dir, f'{varname}_tercile.*.nc'))
                            if len(tercile_file) == 1:
                                with xr.open_dataset(tercile_file[0]) as ds_tercile:
                                    # generated month is not in monotonic
                                    ds_tercile = ds_tercile.sortby('month')
                                    # calculate tercile probability of reforecast
                                    ds_tercile_prob = Tercile.generate_tercile_probability(
                                        ds_single_initialization=ds_var,
                                        varname=varname,
                                        ds_tercile=ds_tercile,
                                        lead_bins=None
                                    )

                                    # copy the encoding and attributes
                                    ds_tercile_prob = mom6_encode_attr(ds_var, ds_tercile_prob, var_names=['tercile_prob', 'tercile_prob_max'])

                                    # redefine new global attribute
                                    # global attributes
                                    ds_tercile_prob.attrs['cefi_rel_path'] = output_dir
                                    ds_tercile_prob.attrs['cefi_filename'] = new_filename
                                    ds_tercile_prob.attrs['cefi_variable'] = f"{new_varname} - tercile_prob,tercile_prob_max"
                                    ds_tercile_prob.attrs['cefi_postprocess_note'] = (
                                        f"tercile probability based on {os.path.join(tercile_dir, f'{varname}_tercile.*.nc')}"
                                    )

                                    # output the processed data
                                    output_processed_data(
                                        ds_tercile_prob,
                                        top_dir=dict_json['local_top_dir'],
                                        dict_json_output=dict_json['output']
                                    )
                            else:
                                raise ValueError('more than one tercile file for single variable')
    else:
        raise ValueError('experiment_type must be forecast')

if __name__=="__main__":

    warnings.simplefilter("ignore")
    client = Client(processes=False,memory_limit='500GB',silence_logs=50)
    print(client)
    if client.cluster is not None:
        print(client.cluster.dashboard_link)

    # Ensure a JSON file is provided as an argument
    if len(sys.argv) < 1:
        print("Usage: python mom6_tercile_probability_batch.py xxxx.json")
        sys.exit(1)

    # Get the JSON file path from command-line arguments
    json_setting = sys.argv[1]

    current_location = os.path.dirname(os.path.abspath(__file__))
    log_name = sys.argv[1].split('.')[0]+'.log'
    log_filename = os.path.join(current_location,log_name)

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
        dict_json1 = load_json(json_setting,json_path=current_location)

        # preprocessing the file to cefi format
        tercile_probability_batch(dict_json1, logger)

    except Exception as e:
        logger.exception("An exception occurred")
