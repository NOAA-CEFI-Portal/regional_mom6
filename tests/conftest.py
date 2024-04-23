import pytest

def pytest_addoption(parser):
    parser.addoption("--location", action="store", default="opendap")
    parser.addoption("--data", action="store", default="")
    parser.addoption("--static", action="store", default="")

@pytest.fixture
def location(request):
    return request.config.getoption("--location")
def data_file(request):
    return request.config.getoption("--data")
def static_data_file(request):
    return request.config.getoption("--static")
