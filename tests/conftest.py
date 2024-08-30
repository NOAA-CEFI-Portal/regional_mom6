"""
This is a pytest setting file to have the option of 
pytesting under different location/platform

To do pytest on the local machine
`pytest` (will do the opendap option below)
`pytest --location opendap` (testing mock index calculation)
`pytest --location local` (testing real index calculation)

"""
import pytest
import xarray as xr
from mom6.mom6_module import mom6_io

def pytest_addoption(parser):
    parser.addoption("--location", action="store", default="opendap")

@pytest.fixture
def location(request):
    return request.config.getoption("--location")

# Define the fixture that loads data
# 'session' scope allowed data to be shared across test session
@pytest.fixture
def ds_forecast(location)->xr.Dataset:
    """load data for testing

    Returns
    -------
    xr.Dataset
        The loaded data in dataset
    """
    # loading the forecast
    forecast_io_class = mom6_io.MOM6Forecast(
        var='tos',
        data_relative_dir='forecast/',
        static_relative_dir='static/',
        grid='raw',
        source=location
    )

    ds = forecast_io_class.get_all()

    return ds
