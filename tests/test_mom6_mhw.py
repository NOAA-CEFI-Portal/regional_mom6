"""
Testing the module 
- mom6_statistics.py
- mom6_mhw.py

The testing should works locally and remote when on using GitHub Action

For local testing:
`pytest --location=local`

The location option is implemented through the conftest.py

"""
import numpy as np
import xarray as xr
from mom6.mom6_module import mom6_statistics
from mom6.mom6_module import mom6_mhw

def test_MarineHeatwaveForecast_generate_forecast_single(ds_forecast):
    """test MarineHeatwaveForecast Class generate_forecast_single method"""
    # set correct dataset naming for module use
    ds = ds_forecast.rename({'geolon':'lon','geolat':'lat'})

    # call class
    class_forecast_climo = mom6_statistics.ForecastClimatology(ds,'tos')

    # create anom batch object
    dict_anom = class_forecast_climo.generate_anom_batch(
        1993,1993,1993,1993,'compute',precompute_climo=False
    )
    ds_anom = xr.Dataset()
    ds_anom['tos'] = dict_anom['anomaly']
    ds_anom['lon'] = ds['lon']
    ds_anom['lat'] = ds['lat']
    ds_anom['climo'] = dict_anom['climatology']

    # test generate_quantile
    class_forecast_quantile = mom6_statistics.ForecastQuantile(
        ds_anom
        .sel(init='1993-03')
        .isel(
            xh=slice(100,200),
            yh=slice(0, 100),
            lead=slice(0,1),
            member=slice(0,1)
        ),
        'tos'
    )
    da_threshold = class_forecast_quantile.generate_quantile(1993,1993,90,False)

    class_mhw = mom6_mhw.MarineHeatwaveForecast(
        ds.isel(xh=slice(100,200),yh=slice(0, 100),member=slice(0,1)),'tos'
    )

    ds_mhw_single = class_mhw.generate_forecast_single(
        '2022-03',
        ds_anom['climo'].isel(xh=slice(100,200),yh=slice(0, 100)),
        da_threshold
    )

    assert np.abs(ds_mhw_single.mhw_prob90.sum().data-505.)==0.
    assert np.abs(ds_mhw_single.ssta_avg.max().data-0.37654305) < 1e-5
    assert np.abs(ds_mhw_single.mhw_mag_indentified_ens.max().data-0.37654305) < 1e-5


def test_MarineHeatwaveForecast_generate_forecast_batch(ds_forecast):

    # set correct dataset naming for module use
    ds = ds_forecast.rename({'geolon':'lon','geolat':'lat'})

    class_mhw = mom6_mhw.MarineHeatwaveForecast(
        ds.isel(
            xh=slice(300,350),
            yh=slice(100, 150),
            lead=slice(0,1),
            member=slice(0,1)
        ),
        'tos'
    )
    ds_mhw_batch = class_mhw.generate_forecast_batch(1993,1994,1993,1993,90)

    assert np.abs(ds_mhw_batch.tos_threshold90.max().data-0.37993317)<1e-6
    assert np.abs(ds_mhw_batch.mhw_prob90.sum().data-6775.)==0.
    assert np.abs(ds_mhw_batch.ssta_avg.max().data-0.47491646)<1e-6
    assert np.abs(ds_mhw_batch.mhw_magnitude_indentified_ens.max().compute().data-0.47491646)<1e-6
