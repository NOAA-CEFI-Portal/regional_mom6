[![unittest](https://github.com/NOAA-PSL/regional_mom6/actions/workflows/gha_pytest_push.yml/badge.svg)](https://github.com/NOAA-PSL/regional_mom6/actions/workflows/gha_pytest_push.yml)

NOAA Climate, Ecosystems, and Fisheries Initiative (CEFI) - Data Portal Team
========

## Regional MOM6 preprocessing package
This is a developing regional mom6 module help both preprocessing the data and perform various statistical analysis shown in [Ross et al., 2023](https://gmd.copernicus.org/articles/16/6943/2023/).
Many scripts are the modified version of the [GFDL CEFI github repository](https://github.com/NOAA-GFDL/CEFI-regional-MOM6).

Current stage of the module is for estabilishing the processing workflow in the [CEFI data portal](https://psl.noaa.gov/cefi_portal/). 
Future availability of a more sophisticated python pakcage for various end-user purposes is in the roadmap of this project.
  
We welcome external contribution to the package. Please feel free to submit issue for any inputs and joining the development core team. Thank you! 

## Setting up the developement environment

1. Fork this repository using the button in the upper right of the GitHub page. This will create a copy of the repository in your own GitHub profile, giving you full control over it.

2. Clone the repository to your local machine from your forked version.

   ```
   git clone <fork-repo-url-under-your-github-account>
   ```
   This create a remote `origin` to your forked version (not the NOAA-CEFI-Portal version)


1. Create a conda/mamba env based on the environment.yml

   ```
   cd regional_mom6/
   conda env create -f environment.yml
   ```
3. Activate the conda env `regional-mom6`

   ```
   conda activate regional-mom6
   ```

5. pip install the package in develop mode

   ```
   pip install -e .
   ```
6. setup config file (data path for local data directory)

   ```
   cp config.json.template config.json
   ```

   open the `config.json` and input the absolute path to the top level of the regional mom6 data

   ```
   {
   "data_path": "<your-mom6data-path-here>"
   }
   ```

   current setup assuming the data directory structure is fixed (i.e. the historical run or forecast data subdirectory (ex: hist_run and forecast) need to be under this data_path )

## Syncing with the NOAA-CEFI-Portal version
1. Create a remote `upstream` to track the changes that is on NOAA-CEFI-Portal

   ```
   git remote add upstream git@github.com:NOAA-CEFI-Portal/regional_mom6.git   
   ```
2. Create a feature branch to make code changes

   ```
   git branch <feature-branch-name>
   git checkout <feature-branch-name>
   ```
   This prevents making direct changes to the `main` branch in your local repository.

3. Sync your local repository with the upstream changes regularly

   ```
   git fetch upstream
   git checkout main
   git merge upstream/main
   ```
   This updates your local `main` branch with the latest changes from the upstream repository. 
   
3. Merge updated local `main` branch into your local `<feature-branch-name>` branch to keep it up to date.

   ```
   git checkout <feature-branch-name>
   git merge main
   ```

4. Push your changes to your forked version on GitHub

   ```
   git push origin <feature-branch-name>
   ```
   Make sure you have included the `upstream/main` changes before creating a pull request on NOAA-CEFI-Portal/regional_mom6




