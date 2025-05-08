import xarray as xr
import os


dir = '/Projects/CEFI/regional_mom6/cefi_portal/northwest_atlantic/full_domain/seasonal_reforecast/monthly/raw/r20240213/'
filename = 'tos.nwa.full.ss_refcast.monthly.raw.r20240213.enss.i200703.nc'
output_filename = 'tos.nwa.full.ss_refcast.monthly.raw.r20240213.enss.i200703.test.nc'

dir_climo = '/Projects/CEFI/regional_mom6/cefi_derivative/northwest_atlantic/full_domain/seasonal_reforecast/monthly/raw/r20240213/climatology/'
filename_climo = 'tos_climatology.nwa.full.ss_refcast.monthly.raw.r20250212.enss.nc'

ds_original = xr.open_dataset(os.path.join(dir, filename))
ds_original_climo = xr.open_dataset(os.path.join(dir_climo, filename_climo))


ds_original['tos_anom'] = ds_original['tos'] - ds_original_climo['tos_climatology'].sel(month=3)


ds_original.to_netcdf(os.path.join(dir, output_filename), unlimited_dims=[])
