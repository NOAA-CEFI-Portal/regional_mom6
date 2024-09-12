from typing import Literal


ModelRegionOptions = Literal[
    'northwest_atlantic'
]

GridOptions = Literal[
    'raw','regrid'
]

DataTypeOptions = Literal[
    'forecast','historical'
]

DataSourceOptions = Literal[
    'local','opendap'
]

DataFreqOptions = Literal[
    'daily','monthly','annual'
]

RegionalOptions = Literal[
    'MAB', 'GOM', 'SS', 'GB', 'SS_LME', 'NEUS_LME', 'SEUS_LME',
    'GOMEX', 'GSL', 'NGOMEX', 'SGOMEX', 'Antilles', 'Floridian'
]

TimeGroupByOptions = Literal[
    'year', 'month', 'dayofyear'
]

DaskOptions = Literal[
    'lazy', 'persist', 'compute'
]