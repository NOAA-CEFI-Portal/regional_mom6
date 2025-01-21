"""
The script do batch modification of attribute

"""
import os
import sys
import glob
import subprocess

data_paths = [
    '/Projects/CEFI/regional_mom6/cefi_portal/northwest_atlantic/full_domain/seasonal_reforecast/monthly/raw/r20240213/',
    '/Projects/CEFI/regional_mom6/cefi_portal/northwest_atlantic/full_domain/seasonal_reforecast/monthly/regrid/r20240213/'
]
attribute_names = ['cefi_paper_doi']
attribute_values = ['10.5194/os-20-1631-2024']

for path in data_paths:
    files = glob.glob(os.path.join(path,'*.nc'))
    if len(files) == 0:
        print(path)
        sys.exit('No file exist in the set paths')
    else:
        for file in files:
            print(file)
            for nattr,attribute_name in enumerate(attribute_names):

                # remove netcdf global atttribute
                # ncatted -O -h -a START_DATE,global,m,c,"2016-06-12_00:00:00" wrfchemi_d01.nc wrfnew.nc
                nco_command = [
                    'ncatted', '-O', '-h', '-a',
                    f'{attribute_name},global,d,c,{attribute_values[nattr]}',
                    file, file
                ]
                # Run the NCO command using subprocess
                try:
                    subprocess.run(nco_command, check=True)
                    # print(f'NCO remove history successfully. Output saved to {new_file}')
                except subprocess.CalledProcessError as e:
                    print(f'Error executing NCO command: {e}')


                # add netcdf global atttribute
                # ncatted -O -h -a START_DATE,global,m,c,"2016-06-12_00:00:00" wrfchemi_d01.nc wrfnew.nc
                nco_command = [
                    'ncatted', '-O', '-h', '-a',
                    f'{attribute_name},global,a,c,{attribute_values[nattr]}',
                    file, file
                ]

                # Run the NCO command using subprocess
                try:
                    subprocess.run(nco_command, check=True)
                    # print(f'NCO remove history successfully. Output saved to {new_file}')
                except subprocess.CalledProcessError as e:
                    print(f'Error executing NCO command: {e}')
