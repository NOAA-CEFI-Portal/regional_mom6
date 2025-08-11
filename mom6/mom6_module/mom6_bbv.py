from typing import Literal
import numpy as np
import xarray as xr
from scipy.interpolate import CubicSpline
from mom6.mom6_module.mom6_density import Density

class BruntVaisalaFrequency:
    """
    Class to calculate the Brunt-Vaisala frequency (N)
    based on temperature, salinity, and depth at each longitude and latitude.

    Parameters
    ----------
    da_thetao: xr.DataArray
        Temperature field.
    da_so: xr.DataArray
        Salinity field.
    da_zl: xr.DataArray
        1D Depth profile for a water column.
    da_lon: xr.DataArray
        Longitude coordinates.
    da_lat: xr.DataArray
        Latitude coordinates.
    eos_version: Literal['eos-80','teos-10']
        Equation of state version used to determine the density profile.
    interp_method: Literal['linear', 'cubic']
        Interpolation method used to determine the first 200m N.
    """

    def __init__(
        self,
        da_thetao: xr.DataArray,
        da_so: xr.DataArray,
        da_depth: xr.DataArray,
        da_lon: xr.DataArray,
        da_lat: xr.DataArray,
        eos_version: Literal['eos-80','teos-10'] = 'teos-10',
        interp_method : Literal['linear', 'cubic'] = 'cubic',
        depth_dim_name : str = 'z_l'
    ):
        self.da_thetao = da_thetao
        self.da_so = da_so
        self.da_depth = da_depth
        self.da_lon = da_lon
        self.da_lat = da_lat
        self.eos_version = eos_version
        self.interp_method = interp_method
        self.depth_dim_name = depth_dim_name

    @staticmethod
    def column_bbv(
        temp_col: np.ndarray,
        salt_col: np.ndarray,
        depth_col: np.ndarray,
        lon: np.ndarray,
        lat: np.ndarray,
        eos_version: Literal['eos-80','teos-10'] = 'eos-80',
        interp_method : Literal['linear', 'cubic'] = 'cubic'
    ) -> np.ndarray:
        """
        Calculates the Brunt-Väisälä frequency (BBV) for a single water column.

        This function is designed to be used with xarray.apply_ufunc.

        Parameters
        ----------
        temp_col: np.ndarray
            Temperature profile for a water column.
        salt_col: np.ndarray
            Salinity profile for a water column.
        depth_col: np.ndarray
            Depth profile for a water column.
        lon: np.ndarray
            Longitude coordinates.
        lat: np.ndarray
            Latitude coordinates.
        eos_version: Literal['eos-80','teos-10']
            Equation of state version used to determine the density profile.
        interp_method: Literal['linear', 'cubic']
            Interpolation method used to determine the first 200m N.
        """

        # setup constant
        rho0 = 1027 # Reference density
        gravitational_const = 9.807   # Gravitational constant

        # Calculate density profile
        if eos_version == 'eos-80':
            dens_col = Density.sw_dens(salt_col, temp_col)
        elif eos_version == 'teos-10':
            # check if lon lat is provided
            if lon is None or lat is None:
                raise ValueError(
                    "Longitude and latitude must be provided for TEOS-10 calculations."
                )
            dens_col = Density.teos10_sigma0(salt_col, temp_col, depth_col, lon, lat)
        else:
            raise ValueError("Unknown equation of state of seawater")

        # check if depth increase monotonically and is positive
        if not np.all(np.diff(depth_col) > 0) or np.any(depth_col < 0):
            raise ValueError("Depth must be positive and increase monotonically.")

        # Remove any NaN values from the input column which would cause interpolation to fail
        valid_indices = ~np.isnan(dens_col)
        dens_col = dens_col[valid_indices]
        depth_col = depth_col[valid_indices]

        # Ensure there's enough data to interpolate
        if len(depth_col) < 2:
            return np.nan

        # 1. Interpolate density to a regular 1m grid in the vertical
        # make sure if depth of the column is less than 200 the value
        # below the bottom depth is set to NaN
        new_depth_col = np.arange(0, 201, dtype=depth_col.dtype)
        if interp_method == 'cubic':
            interp_func = CubicSpline(depth_col, dens_col, extrapolate=False)
            new_dens_col = interp_func(new_depth_col)
        elif interp_method == 'linear':
            new_dens_col = np.interp(new_depth_col, depth_col, dens_col, left=np.nan, right=np.nan)
        else:
            raise ValueError("Unknown interpolation method")

        # 2. Calculate the vertical density gradient (d(rho)/dz)
        # in this implementation we assume
        # 1. depth increase downward and positive
        # 2. density increase downward and positive when stable
        # due to ds = z[i+1]-z[i] = 1, we simplify to drho_dz = rho[i+1]-rho[i]
        # where drho_dz > 0 when stable; drho_dz < 0 when unstable
        drho_dz =  new_dens_col[1:] - new_dens_col[:-1]

        # 3. Calculate Brunt-Väisälä frequency squared (N^2)
        # The formula is N^2 = (-g / rho0) * (d(rho) / dz)
        # But since density increases with depth, d(rho)/dz is positive, making N^2 negative.
        # here we implicitly deal with this by changing the formula to
        # N^2 = (g / rho0) * (d(rho) / dz).
        n_squared = (gravitational_const / rho0) * drho_dz

        # Set unstable points (negative N^2) to 0 before taking the square root
        n_squared[n_squared < 0] = 0
        n = np.sqrt(n_squared)

        # 4. Average the frequency over the upper 200m and return
        return np.nanmean(n)

    def calculate_bbv(self) -> xr.Dataset:
        """
        Calculates the column averaged Brunt-Väisälä Frequency (BBV) for the entire dataset.
        """

        print("--- Calculating Brunt-Väisälä Frequency (BBV) ---")

        # Apply the BBV helper function across all dimensions except 'z'
        da_bbv_200 = xr.apply_ufunc(
            self.__class__.column_bbv,
            self.da_thetao,
            self.da_so,
            self.da_depth,
            self.da_lon,
            self.da_lat,
            kwargs={
                'eos_version': self.eos_version,
                'interp_method': self.interp_method
            },
            input_core_dims=[[self.depth_dim_name], [self.depth_dim_name], [self.depth_dim_name], [], []],
            output_core_dims=[[]],
            exclude_dims=set((self.depth_dim_name,)),
            vectorize=True,
            dask="parallelized",
            output_dtypes=[float]
        )
        # create correct meta data
        da_bbv_200.attrs['units'] = '1/s'
        da_bbv_200.attrs['long_name'] = 'Mean Brunt-Väisälä Frequency (200m)'
        da_bbv_200.attrs['standard_name'] = 'mean_brunt_vaisala_frequency_200m'

        # create name for the variable
        ds_bbv = xr.Dataset()
        ds_bbv['bbv'] = da_bbv_200

        return ds_bbv
