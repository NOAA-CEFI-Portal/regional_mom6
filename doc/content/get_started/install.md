[![unittest](https://github.com/NOAA-PSL/regional_mom6/actions/workflows/gha_pytest_push.yml/badge.svg)](https://github.com/NOAA-PSL/regional_mom6/actions/workflows/gha_pytest_push.yml)

Installation
========

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




