"""
Testing the module 
- mom6_statistics.py
- mom6_mhw.py

The testing should works locally and remote when on using GitHub Action

For local testing:
`pytest --location=local`

The location option is implemented through the conftest.py

"""
import pytest
import xarray as xr
import numpy as np
from mom6.mom6_module import mom6_statistics


def test_ForecastClimatology_generate_climo(ds_forecast):
    """test ForecastClimatology Class generate_climo method"""

    # set correct dataset naming for module use
    ds = ds_forecast.rename({'geolon':'lon','geolat':'lat'})

    # call class
    class_forecast_climo = mom6_statistics.ForecastClimatology(ds,'tos')

    # test regular use of generate_climo
    da_climo = class_forecast_climo.generate_climo(1993,1993,'compute')
    # subset testing
    assert np.abs(da_climo.max().data - 32.773476) < 1e-4

    # test error use of generate_climo should have ValueError
    with pytest.raises(ValueError):
        class_forecast_climo.generate_climo(1994,1993,'compute')

    # test error use of generate_climo should have ValueError
    with pytest.raises(ValueError):
        class_forecast_climo.generate_climo(1991,1992,'compute')


def test_ForecastClimatology_generate_anom_batch(ds_forecast):
    """test ForecastClimatology Class generate_anom_batch method"""

    # set correct dataset naming for module use
    ds = ds_forecast.rename({'geolon':'lon','geolat':'lat'})

    # call class
    class_forecast_climo = mom6_statistics.ForecastClimatology(ds,'tos')

    # test regular use of generate_anom_batch method
    dict_anom = class_forecast_climo.generate_anom_batch(
        1993,1993,1993,1993,'compute',precompute_climo=False
    )
    # subset testing
    print(dict_anom['anomaly'].sum().data)
    assert np.abs(dict_anom['anomaly'].max().data - 9.948374) < 1e-5

    # generate climo for testing
    da_climo = class_forecast_climo.generate_climo(1993,1994,'compute')
    # test regular use of generate_anom_batch method with prescribed climo
    dict_anom = class_forecast_climo.generate_anom_batch(
        1993,1993,1993,1993,'compute',precompute_climo=True,da_climo = da_climo
    )
    # subset testing
    print(dict_anom['anomaly'].sum().data)
    assert np.abs(dict_anom['anomaly'].max().data- 10.367273) < 1e-4

    # test error use of generate_anom_batch method with wrong prescribed climo
    #  should have ValueError
    da_climo_error = da_climo.copy()
    del da_climo_error.attrs['period_of_climatology']
    with pytest.raises(ValueError):
        class_forecast_climo.generate_anom_batch(
            1993,1993,1993,1993,'compute',precompute_climo=True,da_climo = da_climo_error
        )

    # test generate_anom_batch method with wrong climo year
    with pytest.raises(ValueError):
        class_forecast_climo.generate_anom_batch(
            1994,1993,1993,1993,'compute',precompute_climo=False
        )

    # test generate_anom_batch method with wrong anomaly year
    with pytest.raises(ValueError):
        class_forecast_climo.generate_anom_batch(
            1993,1994,1994,1993,'compute',precompute_climo=False
        )

    # test generate_anom_batch method with wrong anomaly year
    with pytest.raises(ValueError):
        class_forecast_climo.generate_anom_batch(
            1993,1994,1991,1992,'compute',precompute_climo=False
        )


def test_ForecastQuantile_generate_quantile(ds_forecast):
    """test ForecastClimatology Class generate_anom_batch method"""
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
    assert np.abs(da_threshold.max().data-0.09486389)<1e-7

def test_CoordinateWrangle(ds_forecast):
    """ testing coordinate wrangle"""

    with pytest.raises(KeyError):
        mom6_statistics.CoordinateWrangle(ds_forecast).check_coord_name()

    ds_forecast_latwrong = ds_forecast.rename({'geolon':'lon'})

    with pytest.raises(KeyError):
        mom6_statistics.CoordinateWrangle(ds_forecast_latwrong).check_coord_name()

    ds = ds_forecast_latwrong.rename({'geolat':'lat'})
    assert mom6_statistics.CoordinateWrangle(ds).to_360().lon.min().data > 0
