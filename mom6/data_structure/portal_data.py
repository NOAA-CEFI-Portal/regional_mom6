import re
import os
from dataclasses import dataclass
from typing import Tuple


@dataclass(frozen=True)
class DataStructure:
    """Provide all level naming for the CEFI portal
    data structure
    """
    top_directory: Tuple[str, ...] = (
        'cefi_portal',
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
        'hindcast',
        'seasonal_forecast',
        'seasonal_reforecast',
        'seasonal_forecast_initialization',
        'decadal_forecast',
        'long_term_projection'
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
class DataStructureAttrOrder:
    """Provide all attrs in DataStructure in 
    order of level
    
    need to contain all attributes in DataStructure class
    """
    dir_order: Tuple[str, ...] = (
        "top_directory",
        "region",
        "subdomain",
        "experiment_type",
        "output_frequency",
        "grid_type",
        "release"
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
        'hcast',
        'ss_fcast',
        'ss_refcast',
        'ss_fcast_init',
        'dc_forecast',
        'ltm_proj'
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
        'proj_ssp370',
        'proj_ssp585'
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
    cefi_variable:str = 'N/A'
    cefi_ori_filename:str = 'N/A'
    cefi_archive_version:str = 'N/A'
    cefi_run_xml:str = 'N/A'
    cefi_region:str = 'N/A'
    cefi_subdomain:str = 'N/A'
    cefi_experiment_type:str = 'N/A'
    cefi_experiment_name:str = 'N/A'
    cefi_release:str = 'N/A'
    cefi_output_frequency:str = 'N/A'
    cefi_grid_type:str = 'N/A'
    cefi_date_range:str = 'N/A'
    cefi_init_date:str = 'N/A'
    cefi_ensemble_info:str = 'N/A'
    cefi_forcing:str = 'N/A'
    cefi_data_doi:str = 'N/A'
    cefi_paper_doi:str = 'N/A'
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

def validate_release(release_date:str):
    """Regular expression to match the release date format

    Parameters
    ----------
    release_date : str
        release_date must be in the format 'rYYYYMMDD', e.g., 'r20160902'"

    Raises
    ------
    ValueError
        when release_date not in the format 'rYYYYMMDD', e.g., 'r20160902'"
    """

    if not re.match(r"^r\d{8}$", release_date):
        raise ValueError("release_date must be in the format 'rYYYYMMDD', e.g., 'r20160902'")

@dataclass
class DataPath:
    """constructing cefi file path
    """
    region: str
    subdomain: str
    experiment_type: str
    output_frequency: str
    grid_type: str
    release: str
    top_directory: str = DataStructure().top_directory[0]

    # calling ordered dir list
    list_dir_order = DataStructureAttrOrder.dir_order


    def __post_init__(self):
        data_structure = DataStructure()  # Store a single instance

        # Validate each attribute
        validate_attribute(self.region, data_structure.region, "region")
        validate_attribute(self.subdomain, data_structure.subdomain, "subdomain")
        validate_attribute(self.experiment_type, data_structure.experiment_type, "experiment_type")
        validate_attribute(
            self.output_frequency, data_structure.output_frequency, "output_frequency"
        )
        validate_attribute(
            self.grid_type, data_structure.grid_type, "grid_type"
        )

        validate_release(self.release)

    @property
    def cefi_dir(self) -> str:
        """construct the directory path based on attributes"""

        # Dynamically construct the path based on list_dir_order
        path_parts = []
        for attr in self.list_dir_order:
            attr_value = getattr(self, attr)  # Get attribute by name
            # Handle cases where an attribute is not a str
            if not isinstance(attr_value, str):
                raise TypeError('attribute value not str')
            else:
                path_parts.append(attr_value)

        # Join the parts to form the path
        return os.path.join(*path_parts)

    def find_dir_level(self, attribute_name: str) -> int:
        """find the subdirectory level based on the 
        attribute name

        Parameters
        ----------
        attribute_name : str
            name of the attribute, not the actual directory name

        Returns
        -------
        int
            level of the attribute in the data path
        """

        # set up directory structure in the form of dict (fast search)
        dict_dir_order = {}
        for ndir,dirname in enumerate(self.list_dir_order):
            dict_dir_order[dirname] = ndir

        return dict_dir_order[attribute_name]

@dataclass
class HindcastFilename:
    """constructing cefi filename for hindcast
    """
    variable: str
    region: str
    subdomain: str
    output_frequency: str
    grid_type: str
    release: str
    date_range: str
    experiment_type: str = 'hcast'

    def __post_init__(self):
        # Access the shared FilenameStructure instance
        filename_structure = FilenameStructure()

        # Validate each attribute
        validate_attribute(self.region, filename_structure.region, "region")
        validate_attribute(self.subdomain, filename_structure.subdomain, "subdomain")
        if self.experiment_type != 'hcast':
            raise ValueError(
                f"Invalid experiment_type: {self.experiment_type}. Must be 'hcast'."
            )
        validate_attribute(
            self.output_frequency, filename_structure.output_frequency, "output_frequency"
        )
        validate_release(self.release)

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
        .<output_frequency>.<grid_type>.<release>.<YYYY0M-YYYY0M>.nc

        """
        return (
            f"{self.variable}."+
            f"{self.region}.{self.subdomain}."+
            f"{self.experiment_type}."+
            f"{self.output_frequency}."+
            f"{self.grid_type}."+
            f"{self.release}."+
            f"{self.date_range}.nc"
        )

@dataclass
class SeasonalForecastFilename:
    """constructing cefi filename for forecast and reforecast
    """
    variable: str
    region: str
    subdomain: str
    experiment_type: str
    output_frequency: str
    release: str
    grid_type: str
    initial_date: str
    ensemble_info: str

    def __post_init__(self):
        # Access the shared FilenameStructure instance
        filename_structure = FilenameStructure()

        # Validate each attribute
        validate_attribute(self.region, filename_structure.region, "region")
        validate_attribute(self.subdomain, filename_structure.subdomain, "subdomain")
        if self.experiment_type not in ['ss_fcast','ss_refcast']:
            raise ValueError(
                f"Invalid experiment_type: {self.experiment_type}. "+
                "Must be one of ['ss_fcast','ss_refcast']."
            )
        validate_attribute(self.grid_type, filename_structure.grid_type, "grid_type")
        validate_attribute(
            self.output_frequency, filename_structure.output_frequency, "output_frequency"
        )
        validate_attribute(
            self.ensemble_info, filename_structure.ensemble_info, "ensemble_info"
        )

        validate_release(self.release)

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
            f"{self.experiment_type}."+
            f"{self.output_frequency}."+
            f"{self.grid_type}."+
            f"{self.release}."+
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
    release: str
    grid_type: str
    forcing: str
    ensemble_info: str
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
        validate_attribute(self.grid_type, filename_structure.grid_type, "grid_type")
        validate_attribute(
            self.output_frequency, filename_structure.output_frequency, "output_frequency"
        )
        validate_attribute(
            self.forcing, filename_structure.ensemble_info, "ensemble_info"
        )
        validate_release(self.release)

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
            f"{self.experiment_type}."+
            f"{self.output_frequency}."+
            f"{self.grid_type}."+
            f"{self.release}."+
            f"{self.forcing}."+
            f"{self.ensemble_info}."+
            f"{self.date_range}.nc"
        )
