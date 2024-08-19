from typing import Literal

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