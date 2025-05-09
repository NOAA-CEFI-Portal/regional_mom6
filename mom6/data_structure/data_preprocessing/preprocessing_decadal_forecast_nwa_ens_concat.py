
import xarray as xr
import numpy as np
import os


dir = '/Projects/CEFI/private/scratch/chsu/NWA_reforecast_decadal_r20250502/'
filename_header = 'decadal.ocean_month'


start_year = 1965
end_year = 2022

output_dir = os.path.join(dir, 'concat')
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

for year in range(start_year, end_year + 1):
    output_file = os.path.join(output_dir, f'tos.{filename_header}.i{year}01.nc')

    if os.path.exists(output_file):
        print(f'File already exists skipping: {output_file}')
        continue

    concat_enss = []
    for ens in range(1, 10+1):
        if ens == 6 and year == 1966:
            ens = 12
        elif ens == 8 and year == 2012:
            ens = 13
        elif ens == 9 and year == 2016:
            ens = 14
        file = os.path.join(dir, f'{filename_header}.i{year}01.e{ens:02d}.tos.nc')

        if os.path.exists(file):
            print(f'Processing {file}')
            with xr.open_dataset(file) as ds:
                ds = ds.rename({'time': 'lead'})
                ds = ds.sortby('lead')
                concat_enss.append(ds)
        else:
            print(f'File not found: {file}')

    ds_concat = xr.concat(concat_enss, dim='member')

    ds_concat['tos'] = ds_concat['tos'].transpose('lead', 'member', 'yh', 'xh')

    ds_concat['init'] = np.datetime64(f'{year}-01-01').astype('datetime64[ns]')
    ds_concat['init'].encoding['dtype'] = 'int32'

    ds_concat['member'] = np.arange(1, 10+1, dtype=int)
    ds_concat['member'].encoding['dtype'] = 'int32'

    ds_concat = ds_concat.drop_dims('time', errors='ignore')
    ds_concat = ds_concat.drop_dims('nv', errors='ignore')

    print(f'Saving concatenated file: {output_file}')
    ds_concat.to_netcdf(os.path.join(dir,'concat', output_file), unlimited_dims=[])

