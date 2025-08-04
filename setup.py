from setuptools import setup, find_packages

setup(
    name="rmom6_preprocessing",
    version="0.10.5",
    description="Regional MOM6 CEFI preprocessing and data access",
    author="Chia-Wei Hsu",
    author_email="chia-wei.hsu@noaa.gov",
    url="https://github.com/NOAA-CEFI-Portal/regional_mom6",
    packages=find_packages(),
    python_requires=">=3.11",
    license="BSD 3-Clause",
    install_requires=[
        "xarray",
        "netcdf4",
        "dask",
        "pydap",
        "pytest",
        "beautifulsoup4",
        "matplotlib",
        "zarr==2.18.3",
        "fsspec",
        "s3fs",
        "gcsfs"
    ],
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
    ],
)