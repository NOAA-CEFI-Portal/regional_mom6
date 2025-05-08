
import xarray as xr
import os


dir = '/Projects/CEFI/private/scratch/chsu/NWA_reforecast_r20240213/concat/'
filename = 'tos.seasonal.ocean_month.i200703.nc'

output_dir = '/Projects/CEFI/regional_mom6/cefi_portal/northwest_atlantic/full_domain/seasonal_reforecast/monthly/raw/r20240213/'
output_filename = 'tos.nwa.full.ss_refcast.monthly.raw.r20240213.enss.i200703.nc'
duplicate_filename = 'tos.nwa.full.ss_refcast.monthly.raw.r20240213.enss.i200706.nc'


ds_original = xr.open_dataset(os.path.join(dir, filename))
ds_duplicate = xr.open_dataset(os.path.join(output_dir, duplicate_filename))


ds_duplicate[output_filename.split('.')[0]] = ds_original[output_filename.split('.')[0]]
ds_duplicate['month'] = int(output_filename.split('.')[-2][-2:])
ds_duplicate.attrs['cefi_filename'] = output_filename
ds_duplicate.attrs['cefi_init_date'] = output_filename.split('.')[-2]

ds_duplicate.to_netcdf(os.path.join(output_dir,'test', output_filename), unlimited_dims=[])
