from typing import Literal

ModelRegionOptions =  Literal[
    'northwest_atlantic',
    'northeast_pacific',
    'arctic',
    'pacific_islands',
    'great_lakes'
]

ModelSubdomainOptions =  Literal[
    'full_domain'
]

ModelExperimentTypeOptions =  Literal[
    'hindcast',
    'seasonal_forecast',
    'seasonal_reforecast',
    'seasonal_forecast_initialization',
    'decadal_forecast',
    'long_term_projection'
]

ModelOutputFrequencyOptions =  Literal[
    'daily',
    'monthly',
    'yearly'
]

ModelGridTypeOptions =  Literal[
    'raw',
    'regrid'
]


DataSourceOptions = Literal[
    'local','opendap','s3','gcs'
]


NWASubregionalOptions = Literal[
    'MAB', 'GOM', 'SS', 'GB', 'SS_LME', 'NEUS_LME', 'SEUS_LME',
    'GOMEX', 'GSL', 'NGOMEX', 'SGOMEX', 'Antilles', 'Floridian'
]

TimeGroupByOptions = Literal[
    'year', 'month', 'dayofyear'
]

DaskOptions = Literal[
    'lazy', 'persist', 'compute'
]