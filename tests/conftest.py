"""
This is a pytest setting file to have the option of 
pytesting under different location/platform

To do pytest on the local machine
`pytest` (will do the opendap option below)
`pytest --location opendap` (testing mock index calculation)
`pytest --location local` (testing real index calculation)

"""
import pytest

def pytest_addoption(parser):
    parser.addoption("--location", action="store", default="opendap")

@pytest.fixture
def location(request):
    return request.config.getoption("--location")
