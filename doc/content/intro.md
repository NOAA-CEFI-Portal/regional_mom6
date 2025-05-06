# Regional MOM6 Package

## Overview
This package is designed to facilitate the extraction and download of data from the CEFI data portal. Its primary functionality centers around a user-friendly API that allows users to programmatically access and retrieve data from multiple sources available through the portal.

Beyond data acquisition, the package includes a suite of postprocessing tools tailored to compute a wide range of statistical metrics and environmental indices. These outputs support the generation of ecosystem assessments, particularly those featured in the State of the Ecosystem (SOE) reports. These reports summarize ecological conditions using indicators such as the cold pool index (for the Northeast coastal region) and other environmentally relevant statistics derived from raw model and observational data.

In summary, the package serves two main purposes:

- Data Retrieval: Access and download environmental datasets from the CEFI portal.
- Data Analysis: Generate statistical summaries and indices commonly used in environmental monitoring and reporting frameworks such as the SOE.

## Current Status
This is a developing regional MOM6 module that supports both preprocessing of model data and various statistical analyses, as demonstrated in [Ross et al., 2023](https://gmd.copernicus.org/articles/16/6943/2023/). Many of the included scripts are modified versions of those from the [GFDL CEFI github repository](https://github.com/NOAA-GFDL/CEFI-regional-MOM6).

The current focus of this module is to establish a robust processing workflow for CEFI users/scientists that utilize the [CEFI data portal](https://psl.noaa.gov/cefi_portal/). In the future, we plan to release a more comprehensive and user-friendly Python package aimed at supporting a broader range of end-user applications.

We welcome external contributions to this project! If you have feedback, feature requests, or would like to join the development team, please feel free to open an issue or reach out. Thank you!


```{tableofcontents}
```
