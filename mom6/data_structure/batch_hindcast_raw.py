"""
The script do batch rename from 
original hindcast to cefi format

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
import glob
import subprocess
import xarray as xr
from mom6 import DATA_BASE
from mom6.data_structure import portal_data

# original data path
ori_path = os.path.join(DATA_BASE,'northwest_atlantic/hist_run')
path_leng = len(ori_path)+1

# new cefi data path setting
cefi_portal_base = '/Projects/CEFI/regional_mom6/'
release_date = 'r20230519'
archive_version = '/archive/acr/fre/NWA/2023_04/NWA12_COBALT_2023_04_kpo4-coastatten-physics/gfdl.ncrc5-intel22-prod/'
region_dir = 'northwest_atlantic'
region_file = 'nwa'
subdomain_dir = 'full_domain'
subdomain_file = 'full'
grid_type = 'raw'
experiment_type = 'hindcast'
experiment_name = 'nwa12_cobalt'
data_doi = '10.5281/zenodo.7893386'
paper_doi = '10.5194/gmd-16-6943-2023'



# loop through all file in the original path
for file in glob.glob(f'{ori_path}/*.nc'):
    if 'static' not in file:
        # each file decipher the format to make sure the file type
        file_format_list = file[path_leng:].split('.')
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
            cefi_ori_filename = file.split('/')[-1],
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
            print(f"{new_file}: already exists.")
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
                print(f'NCO rechunk and compress successfully. Output saved to {new_file}')
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
                    print(f'NCO add attribute {key} successfully. Output saved to {new_file}')
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
                print(f'NCO remove history successfully. Output saved to {new_file}')
            except subprocess.CalledProcessError as e:
                print(f'Error executing NCO command: {e}')


            # # copy rechunk and compress to the associated cefi data path
            # print(f"copying {file} to")
            # print(f"{new_file}")
            # result = subprocess.run(
            #     ["cp", f"{file}", f"{new_file}"],
            #     capture_output=True,
            #     text=True,
            #     check=True
            # )

            # # Access the output
            # print("STDOUT:", result.stdout)  # Command's output
            # print("STDERR:", result.stderr)  # Any errors
            # print("Return Code:", result.returncode)  # 0 means success


