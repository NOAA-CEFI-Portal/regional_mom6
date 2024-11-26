import re
import os
from itertools import product
from dataclasses import dataclass
from typing import Tuple


@dataclass(frozen=True)
class DataStructure:
    """Provide all level naming for the CEFI portal
    data structure
    """
    top_directory: Tuple[str, ...] = (
        'cefi_regional_mom6',
    )
    region: Tuple[str, ...] = (
        'northwest_atlantic',
        'northeast_pacific',
        'arctic',
        'pacific_islands',
        'great_lakes'
    )
    subdomain: Tuple[str, ...] = (
        'full_domain',
    )
    experiment_type: Tuple[str, ...] = (
        'historical_run',
        'seasonal_forecast',
        'decadal_forecast',
        'long_term_projection'
    )
    version: Tuple[str, ...] = (
        'v2023-04-1',
    )
    output_frequency: Tuple[str, ...] = (
        'daily',
        'monthly',
        'yearly'
    )
    grid_type: Tuple[str, ...] = (
        'raw',
        'regrid'
    )

@dataclass(frozen=True)
class FilenameStructure:
    """Provide all information used for the naming
    the filename provided on cefi data portal
    """
    region: Tuple[str, ...] = (
        'nwa',
        'nep',
        'arc',
        'pci',
        'glk'
    )
    subdomain: Tuple[str, ...] = (
        'full',
    )
    experiment_type: Tuple[str, ...] = (
        'hist_run',
        'ssforecast',
        'dcforecast',
        'ltm_proj'
    )
    version: Tuple[str, ...] = (
        'v2023-04-1',
    )
    output_frequency: Tuple[str, ...] = (
        'daily',
        'monthly',
        'yearly'
    )
    ensemble_info: Tuple[str, ...] = (
        'enss',
        'ens_stats'
    )
    forcing_info: Tuple[str, ...] = (
        'picontrol',
        'historical',
        'proj_ssp126'
    )
    grid_type: Tuple[str, ...] = (
        'raw',
        'regrid'
    )


@dataclass(frozen=True)
class FileChunking:
    """Setup the chunking size
    
    current setting is around 500MB per chunk
    """
    vertical: int = 10
    horizontal: int = 200
    time: int = 100
    lead: int = 12
    member: int = 10

@dataclass(frozen=True)
class GlobalAttrs:
    """ global attribute to be in all cefi files"""

    cefi_rel_path:str = 'N/A'
    cefi_filename:str = 'N/A'
    cefi_ori_filename:str = 'N/A'
    cefi_region:str = 'N/A'
    cefi_subdomain:str = 'N/A'
    cefi_experiment_type:str = 'N/A'
    cefi_version:str = 'N/A'
    cefi_output_frequency:str = 'N/A'
    cefi_grid_type:str = 'N/A'
    cefi_date_range:str = 'N/A'
    cefi_init_date:str = 'N/A'
    cefi_ensemble_info:str = 'N/A'
    cefi_forcing:str = 'N/A'
    cefi_aux:str = 'N/A'


def validate_attribute(
        attr_value: str,
        attr_options: Tuple[str, ...],
        attr_name: str
    ):
    """validation function on the available attribute name

    Parameters
    ----------
    attr_value : str
        input attribute value
    attr_options : Tuple[str, ...]
        available attribute values
    attr_name : str
        the attribute name to check opitons

    Raises
    ------
    ValueError
        when value is not in the available options
    """
    if attr_value not in attr_options:
        raise ValueError(
            f"Invalid {attr_name}: {attr_value}. "+
            f"Must be one of {attr_options}."
        )

def create_directory_structure(base_dir:str):
    """Generate the data structure needed for
    storing the cefi data locally

    Parameters
    ----------
    base_dir : str
        the base directory where the user wish 
        the cefi data struture to be located
    """
    data_structure = DataStructure()
    # Iterate through all combinations of available value in attributes
    for combination in product(
        data_structure.top_directory,
        data_structure.region,
        data_structure.subdomain,
        data_structure.experiment_type,
        data_structure.version,
        data_structure.output_frequency,
        data_structure.grid_type
    ):
        # Build the directory path
        dir_path = os.path.join(base_dir, *combination)
        print(f"create {dir_path}")
        # Create the directory (creates intermediate dirs if they don't exist)
        os.makedirs(dir_path, exist_ok=True)


@dataclass
class DataPath:
    """constructing cefi file path
    """
    region: str
    subdomain: str
    experiment_type: str
    version: str
    output_frequency: str
    grid_type: str

    def __post_init__(self):
        data_structure = DataStructure()  # Store a single instance

        # Validate each attribute
        validate_attribute(self.region, data_structure.region, "region")
        validate_attribute(self.subdomain, data_structure.subdomain, "subdomain")
        validate_attribute(self.experiment_type, data_structure.experiment_type, "experiment_type")
        validate_attribute(self.version, data_structure.version, "version")
        validate_attribute(
            self.output_frequency, data_structure.output_frequency, "output_frequency"
        )
        validate_attribute(
            self.grid_type, data_structure.grid_type, "grid_type"
        )
    @property
    def cefi_dir(self) -> str:
        """construct the directory path based on attributes"""
        return os.path.join(
            f"{DataStructure().top_directory[0]}",
            f"{self.region}",
            f"{self.subdomain}",
            f"{self.experiment_type}",
            f"{self.version}",
            f"{self.output_frequency}",
            f"{self.grid_type}"
        )


@dataclass
class HistrunFilename:
    """constructing cefi filename for historical run
    """
    variable: str
    region: str
    subdomain: str
    output_frequency: str
    version: str
    date_range: str
    grid_type: str
    experiment_type: str = 'hist_run'

    def __post_init__(self):
        # Access the shared FilenameStructure instance
        filename_structure = FilenameStructure()

        # Validate each attribute
        validate_attribute(self.region, filename_structure.region, "region")
        validate_attribute(self.subdomain, filename_structure.subdomain, "subdomain")
        if self.experiment_type != 'hist_run':
            raise ValueError(
                f"Invalid experiment_type: {self.experiment_type}. Must be 'hist_run'."
            )
        validate_attribute(self.version, filename_structure.version, "version")
        validate_attribute(
            self.output_frequency, filename_structure.output_frequency, "output_frequency"
        )

        # Regular expression to match the required format
        if not re.match(r"^\d{6}-\d{6}$", self.date_range):
            raise ValueError(
                "date_range must be in the format 'YYYYMM-YYYYMM', e.g., '199301-200304'"
            )

    @property
    def filename(self) -> str:
        """construct the filename based on attributes
        format :
        <variable>.<region>.<subdomain>.<experiment_type>
        .<version>.<output_frequency>.<grid_type>.<YYYY0M-YYYY0M>.nc

        """
        return (
            f"{self.variable}."+
            f"{self.region}.{self.subdomain}."+
            f"{self.experiment_type}.{self.version}."+
            f"{self.output_frequency}."+
            f"{self.grid_type}."+
            f"{self.date_range}.nc"
        )

@dataclass
class ForecastFilename:
    """constructing cefi filename for forecast
    """
    variable: str
    region: str
    subdomain: str
    experiment_type: str
    output_frequency: str
    version: str
    grid_type: str
    initial_date: str
    ensemble_info: str

    def __post_init__(self):
        # Access the shared FilenameStructure instance
        filename_structure = FilenameStructure()

        # Validate each attribute
        validate_attribute(self.region, filename_structure.region, "region")
        validate_attribute(self.subdomain, filename_structure.subdomain, "subdomain")
        if self.experiment_type not in ['ssforcast','dcforecast']:
            raise ValueError(
                f"Invalid experiment_type: {self.experiment_type}. "+
                "Must be one of ['ssforcast','dcforecast']."
            )
        validate_attribute(self.version, filename_structure.version, "version")
        validate_attribute(self.grid_type, filename_structure.grid_type, "grid_type")
        validate_attribute(
            self.output_frequency, filename_structure.output_frequency, "output_frequency"
        )
        validate_attribute(
            self.ensemble_info, filename_structure.ensemble_info, "ensemble_info"
        )

        # Regular expression to match the required format
        if not re.match(r"^i\d{6}$", self.initial_date):
            raise ValueError(
                f"Invalid initial_date: {self.initial_date}. Must be in the format 'iYYYYMM'."
            )

    @property
    def filename(self) -> str:
        """construct the filename based on attributes"""
        return (
            f"{self.variable}."+
            f"{self.region}.{self.subdomain}."+
            f"{self.experiment_type}.{self.version}."+
            f"{self.output_frequency}."+
            f"{self.grid_type}."+
            f"{self.ensemble_info}."+
            f"{self.initial_date}.nc"
        )

@dataclass
class ProjectionFilename:
    """constructing cefi filename for projection run
    """
    variable: str
    region: str
    subdomain: str
    output_frequency: str
    version: str
    grid_type: str
    forcing: str
    date_range: str
    experiment_type: str = 'ltm_proj'

    def __post_init__(self):
        # Access the shared FilenameStructure instance
        filename_structure = FilenameStructure()

        # Validate each attribute
        validate_attribute(self.region, filename_structure.region, "region")
        validate_attribute(self.subdomain, filename_structure.subdomain, "subdomain")
        if self.experiment_type != 'ltm_proj':
            raise ValueError(
                f"Invalid experiment_type: {self.experiment_type}. "+
                "Must be 'ltm_proj'."
            )
        validate_attribute(self.version, filename_structure.version, "version")
        validate_attribute(self.grid_type, filename_structure.grid_type, "grid_type")
        validate_attribute(
            self.output_frequency, filename_structure.output_frequency, "output_frequency"
        )
        validate_attribute(
            self.forcing, filename_structure.ensemble_info, "ensemble_info"
        )

        # Regular expression to match the required format
        if not re.match(r"^\d{6}-\d{6}$", self.date_range):
            raise ValueError(
                "date_range must be in the format 'YYYYMM-YYYYMM', e.g., '199301-200304'"
            )

    @property
    def filename(self) -> str:
        """construct the filename based on attributes"""
        return (
            f"{self.variable}."+
            f"{self.region}.{self.subdomain}."+
            f"{self.experiment_type}.{self.version}."+
            f"{self.output_frequency}."+
            f"{self.grid_type}."+
            f"{self.forcing}."+
            f"{self.date_range}.nc"
        )
