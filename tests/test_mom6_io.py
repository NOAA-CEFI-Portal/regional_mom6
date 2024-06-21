#!/usr/bin/env python

"""
Testing the module mom6_io
"""
import pytest
import xarray as xr
from mom6.mom6_module import mom6_io

# TEST OPENDAP
def test_OpenDapStore():
    """Test OpenDap Connection
    """

    opendap_raw = mom6_io.OpenDapStore(grid='raw',data_type='historical',region='northwest_atlantic')
    test_url = opendap_raw.get_catalog()[0]
    try:
        ds = xr.open_dataset(test_url)
        print(ds)
    except OSError :
        pytest.fail('OSError is raised OPeNDAP url not working')


    opendap_regrid = mom6_io.OpenDapStore(grid='regrid',data_type='historical',region='northwest_atlantic')
    test_url = opendap_regrid.get_catalog()[0]
    try:
        ds = xr.open_dataset(test_url)
        print(ds)
    except OSError :
        pytest.fail('OSError is raised OPeNDAP url not working')


# TEST FORECAST IO local
def test_MOM6Forecast(location):
    """Test the forecast IO

    currently only available local

    Parameters
    ----------
    location : str, optional
        source of the data 'opendap' or 'local', by default 'opendap'
    """
    if location == 'local':
        forecast_subdir = 'forecast'
        static_subdir = 'static'
        tercile_subdir = 'tercile_calculation'

        # create local raw instance (tercile foreced None)
        fcast_raw_local = mom6_io.MOM6Forecast(
            var='tob',
            data_relative_dir=forecast_subdir,
            static_relative_dir=static_subdir,
            grid='raw',
            source=location
        )

        # create local regrid instance (tercile foreced None)
        fcast_regrid_local = mom6_io.MOM6Forecast(
            var='tos',
            data_relative_dir=forecast_subdir+'/regrid/',
            static_relative_dir=static_subdir,
            grid='regrid',
            source=location
        )

        try:
            ds = fcast_raw_local.get_single(
                iyear=2006,
                imonth=6)
            if ds['init.year'] != 2006 or ds['init.month'] != 6  :
                pytest.fail('Picked time not the same as output time')
            ds = fcast_regrid_local.get_single(
                iyear=2012,
                imonth=9)
            if ds['init.year'] != 2012 or ds['init.month'] != 9  :
                pytest.fail('Picked time not the same as output time')
            ds = fcast_raw_local.get_all()
            ds = fcast_regrid_local.get_all()
        except OSError :
            pytest.fail('OSError is raised with correct function input')


        # create local raw instance (static dir not provided expect error)
        fcast_raw_local_nostaticdir = mom6_io.MOM6Forecast(
            var='tos',
            data_relative_dir=forecast_subdir,
            static_relative_dir=None,
            grid='raw',
            source=location
        )
        with pytest.raises(OSError):
            ds = fcast_raw_local_nostaticdir.get_single()
        with pytest.raises(OSError):
            ds = fcast_raw_local_nostaticdir.get_single()


        # create local regrid instance (regrid dir location error expect error)
        fcast_regrid_local_errorloc = mom6_io.MOM6Forecast(
            var='tos',
            data_relative_dir=forecast_subdir,
            static_relative_dir=static_subdir,
            grid='regrid',
            source=location
        )

        with pytest.raises(OSError):
            ds = fcast_regrid_local_errorloc.get_single()
            fcast_regrid_local_errorloc.get_all()

        # create local regrid instance (raw dir location error expect error)
        fcast_regrid_local_errorgrid = mom6_io.MOM6Forecast(
            var='tos',
            data_relative_dir=forecast_subdir+'/regrid/',
            static_relative_dir=static_subdir,
            grid='raw',
            source=location
        )
        with pytest.raises(OSError):
            fcast_regrid_local_errorgrid.get_single()
            fcast_regrid_local_errorgrid.get_all()

        # create local raw instance (error iyear and imonth input for method get_single expect error)
        fcast_regrid_local_erroryear = mom6_io.MOM6Forecast(
            var='tos',
            data_relative_dir=forecast_subdir,
            static_relative_dir=static_subdir,
            grid='raw',
            source=location
        )
        with pytest.raises(IndexError):
            fcast_regrid_local_erroryear.get_single(iyear=2024,imonth=12)
            fcast_regrid_local_erroryear.get_single(iyear=2024,imonth=8)

        # (first and last iyear and imonth input for method get_single expect NO error)
        try:
            ds = fcast_regrid_local_erroryear.get_single(iyear=1993,imonth=3)
            if ds['init.year'] != 1993 or ds['init.month'] != 3  :
                pytest.fail('Picked time not the same as output time')
            ds = fcast_regrid_local_erroryear.get_single(iyear=2022,imonth=12)
            if ds['init.year'] != 2022 or ds['init.month'] != 12  :
                pytest.fail('Picked time not the same as output time')
        except OSError :
            pytest.fail('OSError is raised with correct function input')

        # create local raw instance (no data dir expect error)
        fcast_raw_local_nodatadir = mom6_io.MOM6Forecast(
            var='tos',
            data_relative_dir=None,
            static_relative_dir=static_subdir,
            grid='raw',
            source=location
        )

        with pytest.raises(OSError):
            fcast_raw_local_nodatadir.get_single()

        # create local raw/regrid instance will all argument provided correctly (expect no Error raised)
        fcast_all3_raw_local = mom6_io.MOM6Forecast(
            var='tos',
            data_relative_dir=forecast_subdir,
            static_relative_dir=static_subdir,
            tercile_relative_dir=tercile_subdir,
            grid='raw',
            source=location
        )

        fcast_all3_regrid_local = mom6_io.MOM6Forecast(
            var='tos',
            data_relative_dir=forecast_subdir+'/regrid/',
            static_relative_dir=None,
            tercile_relative_dir=tercile_subdir+'/regrid',
            grid='regrid',
            source=location
        )

        try:
            ds = fcast_all3_raw_local.get_single(
                iyear=2006,
                imonth=6)
            if ds['init.year'] != 2006 or ds['init.month'] != 6  :
                pytest.fail('Picked time not the same as output time')
            ds = fcast_all3_regrid_local.get_single(
                iyear=2012,
                imonth=9)
            if ds['init.year'] != 2012 or ds['init.month'] != 9  :
                pytest.fail('Picked time not the same as output time')
        except OSError :
            pytest.fail('OSError is raised with correct function input')

        try:
            ds = fcast_all3_raw_local.get_all()
            ds = fcast_all3_regrid_local.get_all()
        except OSError :
            pytest.fail('OSError raised in get_all with correct function input')

        try:
            ds = fcast_all3_raw_local.get_tercile()
            ds = fcast_all3_regrid_local.get_tercile()
            ds = fcast_all3_raw_local.get_tercile(average_type='region')
            # ds = fcast_all3_regrid_local.get_tercile(average_type='region')
        except OSError :
            pytest.fail('OSError raised in get_tercile with correct function input')


def test_MOM6Historical(location):
    """Test the Historical IO

    Parameters
    ----------
    location : str, optional
        source of the data 'opendap' or 'local', by default 'opendap'
    """
    historical_subdir = 'hist_run'
    static_subdir = 'static'

    # create local raw instance (tercile foreced None)
    histrun_raw_local = mom6_io.MOM6Historical(
        var='tob',
        data_relative_dir=historical_subdir,
        static_relative_dir=static_subdir,
        grid='raw',
        source=location
    )

    # create local regrid instance (tercile foreced None)
    histrun_regrid_local = mom6_io.MOM6Historical(
        var='tos',
        data_relative_dir=historical_subdir+'/regrid/',
        static_relative_dir=static_subdir,
        grid='regrid',
        source=location
    )

    try:
        ds = histrun_raw_local.get_single(
            year=2006,
            month=6)
        if ds['time.year'] != 2006 or ds['time.month'] != 6  :
            pytest.fail('Picked time not the same as output time')
        ds = histrun_regrid_local.get_single(
            year=2012,
            month=9)
        if ds['time.year'] != 2012 or ds['time.month'] != 9  :
            pytest.fail('Picked time not the same as output time')
        ds = histrun_raw_local.get_all()
        ds = histrun_regrid_local.get_all()
    except OSError :
        pytest.fail('OSError is raised with correct function input')


    # create local raw instance (static dir not provided expect error)
    histrun_raw_local_nostaticdir = mom6_io.MOM6Historical(
        var='tos',
        data_relative_dir=historical_subdir,
        static_relative_dir=None,
        grid='raw',
        source=location
    )
    if location == 'local':
        with pytest.raises(OSError):
            ds = histrun_raw_local_nostaticdir.get_single()
        with pytest.raises(OSError):
            ds = histrun_raw_local_nostaticdir.get_single()


    # create local regrid instance (regrid dir location error expect error)
    histrun_regrid_local_errorloc = mom6_io.MOM6Historical(
        var='tos',
        data_relative_dir=historical_subdir,
        static_relative_dir=static_subdir,
        grid='regrid',
        source=location
    )

    if location == 'local':
        with pytest.raises(OSError):
            ds = histrun_regrid_local_errorloc.get_single()
            histrun_regrid_local_errorloc.get_all()

    # create local regrid instance (raw dir location error expect error)
    histrun_regrid_local_errorgrid = mom6_io.MOM6Historical(
        var='tos',
        data_relative_dir=historical_subdir+'/regrid/',
        static_relative_dir=static_subdir,
        grid='raw',
        source=location
    )
    if location == 'local':
        with pytest.raises(OSError):
            histrun_regrid_local_errorgrid.get_single()
            histrun_regrid_local_errorgrid.get_all()


    # create local raw instance (error iyear and imonth input for method get_single expect error)
    histrun_regrid_local_erroryear = mom6_io.MOM6Historical(
        var='tos',
        data_relative_dir=historical_subdir,
        static_relative_dir=static_subdir,
        grid='raw',
        source=location
    )
    with pytest.raises(IndexError):
        histrun_regrid_local_erroryear.get_single(year=2024,month=12)
        histrun_regrid_local_erroryear.get_single(year=2024,month=8)

    # (first and last iyear and imonth input for method get_single expect NO error)
    try:
        ds = histrun_regrid_local_erroryear.get_single(year=1993,month=1)
        if ds['time.year'] != 1993 or ds['time.month'] != 1  :
            pytest.fail('Picked time not the same as output time')
        ds = histrun_regrid_local_erroryear.get_single(year=2019,month=12)
        if ds['time.year'] != 2019 or ds['time.month'] != 12  :
            pytest.fail('Picked time not the same as output time')
    except OSError :
        pytest.fail('OSError is raised with correct function input')

    # create local raw instance (no data dir expect error)
    histrun_raw_local_nodatadir = mom6_io.MOM6Historical(
        var='tos',
        data_relative_dir=None,
        static_relative_dir=static_subdir,
        grid='raw',
        source=location
    )
    if location == 'local':
        with pytest.raises(OSError):
            histrun_raw_local_nodatadir.get_single()
