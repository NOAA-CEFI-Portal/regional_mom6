[![unittest](https://github.com/NOAA-PSL/regional_mom6/actions/workflows/gha_pytest_push.yml/badge.svg)](https://github.com/NOAA-PSL/regional_mom6/actions/workflows/gha_pytest_push.yml)

NOAA Climate, Ecosystems, and Fisheries Initiative (CEFI) - Data Portal Team
========

## Regional MOM6 preprocessing package
This is a developing regional mom6 module help both preprocessing the data and perform various statistical analysis shown in [Ross et al., 2023](https://gmd.copernicus.org/articles/16/6943/2023/).
Many scripts are the modified version of the [GFDL CEFI github repository](https://github.com/NOAA-GFDL/CEFI-regional-MOM6).

Current stage of the module is for estabilishing the processing workflow in the CEFI data portal. 
Future availability of a more sophisticated python pakcage for various end-user purposes is in the roadmap of this project.
  
We welcome external contribution to the package. Please feel free to submit issue for any inputs and joining the development core team. Thank you! 

## Active developement
To use the module in the package at this stage
1. Create a conda/mamba env based on the region_mom.yml

    ```conda env create -f region_mom.yml```
2. Activate the conda env `regional_mom`

    ```conda activate regional_mom```
3. change your location to the top level of cloned repo

    ```cd <dir_path_to_regional_mom6>/regional_mom6/```
4. pip install the package in develop mode

    ```pip install -e .```
