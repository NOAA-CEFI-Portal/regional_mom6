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
    region: Tuple[str, ...] = (
        'northwest_atlantic',
        'northeast_pacific',
        'arctic',
        'pacific_islands',
        'great_lakes'
    )
    subdomain: Tuple[str, ...] = (
        'full_domain',
        'gomex_extra'
    )
    experiment_type: Tuple[str, ...] = (
        'historical_run',
        'seasonal_forecast',
        'decadal_forecast',
        'long_term_projection'
    )
    version: Tuple[str, ...] = (
        '2023-04-1'
    )
    output_frequency: Tuple[str, ...] = (
        'daily',
        'monthly',
        'yearly'
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
        'gomex'
    )
    experiment_type: Tuple[str, ...] = (
        'hist_run',
        'ssforecast',
        'dcforecast',
        'ltm_proj'
    )
    version: Tuple[str, ...] = (
        'v2023-04-1'
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

def create_directory_structure(base_dir: str):
    """Generate the data structure needed for
    storing the cefi data locally

    Parameters
    ----------
    base_dir : str
        the base directory where the user wish 
        the cefi data data struture to be located
    """
    data_structure = DataStructure()
    # Iterate through all combinations of attributes
    for combination in product(
        data_structure.region,
        data_structure.subdomain,
        data_structure.experiment_type,
        data_structure.version,
        data_structure.output_frequency,
    ):
        # Build the directory path
        dir_path = os.path.join(base_dir, *combination)
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

    @property
    def cefi_dir(self) -> str:
        """construct the directory path based on attributes"""
        return (
            f"{self.region}/{self.subdomain}/"+
            f"{self.experiment_type}/{self.version}/"+
            f"{self.output_frequency}/"
        )


@dataclass
class HistrunFilename:
    """constructing cefi filename for historical run
    """
    variable: str
    region: str
    subdomain: str
    experiment_type: str = 'hist_run'
    output_frequency: str
    version: str
    date_range: str

    def __post_init__(self):
        # Access the shared FilenameStructure instance
        filename_structure = FilenameStructure()

        # Validate each attribute
        validate_attribute(self.region, filename_structure.region, "region")
        validate_attribute(self.subdomain, filename_structure.subdomain, "subdomain")
        if self.experiment_type is not 'hist_run':
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
        """construct the filename based on attributes"""
        return (
            f"{self.variable}"+
            f"{self.region}.{self.subdomain}."+
            f"{self.experiment_type}.{self.version}."+
            f"{self.output_frequency}"+
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
            f"{self.variable}"+
            f"{self.region}.{self.subdomain}."+
            f"{self.experiment_type}.{self.version}."+
            f"{self.output_frequency}"+
            f"{self.ensemble_info}"+
            f"{self.initial_date}.nc"
        )

@dataclass
class ProjectionFilename:
    """constructing cefi filename for projection run
    """
    variable: str
    region: Tuple[str, ...]
    subdomain: Tuple[str, ...]
    experiment_type: str = 'ltm_proj'
    output_frequency: Tuple[str, ...]
    version: Tuple[str, ...]
    forcing: Tuple[str, ...]
    date_range: str

    def __post_init__(self):
        # Access the shared FilenameStructure instance
        filename_structure = FilenameStructure()

        # Validate each attribute
        validate_attribute(self.region, filename_structure.region, "region")
        validate_attribute(self.subdomain, filename_structure.subdomain, "subdomain")
        if self.experiment_type is not 'ltm_proj':
            raise ValueError(
                f"Invalid experiment_type: {self.experiment_type}. "+
                "Must be 'ltm_proj'."
            )
        validate_attribute(self.version, filename_structure.version, "version")
        validate_attribute(
            self.output_frequency, filename_structure.output_frequency, "output_frequency"
        )
        validate_attribute(
            self.forcing, filename_structure.ensemble_info, "ensemble_info"
        )

        # Regular expression to match the required format
        # Regular expression to match the required format
        if not re.match(r"^\d{6}-\d{6}$", self.date_range):
            raise ValueError(
                "date_range must be in the format 'YYYYMM-YYYYMM', e.g., '199301-200304'"
            )

    @property
    def filename(self) -> str:
        """construct the filename based on attributes"""
        return (
            f"{self.variable}"+
            f"{self.region}.{self.subdomain}."+
            f"{self.experiment_type}.{self.version}."+
            f"{self.output_frequency}"+
            f"{self.forcing}"+
            f"{self.date_range}.nc"
        )
