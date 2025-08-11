import numpy as np
import xarray as xr

class IsothermalLayerDepth:
    """
    Class to calculate the Isothermal Layer Depth (ILD) 
    from sea surface temperature (SST) and temperature profiles.
    
    Parameters
    ----------
    da_sst: xr.DataArray
        Sea surface temperature.
    da_thetao: xr.DataArray
        Temperature field.
    da_depth: xr.DataArray
        1D Depth profile for a water column.
    da_bottom_depth: xr.DataArray
        2D Bottom depth of the water column.
    ild_temp_offset: float, optional
        Temperature offset to define the ILD (default is 0.5).
    """
    
    def __init__(
        self, 
        da_sst: xr.DataArray,
        da_thetao: xr.DataArray,
        da_depth: xr.DataArray,
        da_bottom_depth: xr.DataArray,
        ild_temp_offset: float = 0.5,
        depth_dim_name: str = 'z_l'
    ):
        self.da_sst = da_sst
        self.da_thetao = da_thetao
        self.da_depth = da_depth
        self.da_bottom_depth = da_bottom_depth
        self.ild_temp_offset = ild_temp_offset
        self.depth_dim_name = depth_dim_name

    @staticmethod
    def column_ild(
        sst: float,
        temp_col: np.ndarray,
        depth_col: np.ndarray,
        bottom_depth: float,
        ild_temp_offset: float = 0.5
    ) -> float:
        """
        Calculates the Isothermal Layer Depth (ILD) for a single water column.
        This function is designed to be used with xarray.apply_ufunc.

        Parameters
        ----------
        sst: float
            Sea surface temperature.
        temp_col: np.ndarray
            Temperature profile for a water column.
        depth_col: np.ndarray
            Depth profile for a water column.
        bottom_depth: float
            Bottom depth of the water column.
        ild_temp_offset: float, optional
            Temperature offset to define the ILD (default is 0.5).
        """

        # check if depth_col is positive
        if np.all(depth_col < 0):
            raise ValueError("Depth values must be positive")

        # check if increase monotonically
        if not np.all(np.diff(depth_col) > 0):
            raise ValueError("Depth values must increase monotonically")

        # temperature at ild level is surface/first layer temp minus the offset
        temp_ild_val = sst-ild_temp_offset

        # if all temp_col is NaN assign NaN
        if np.all(np.isnan(temp_col)) :
            return np.nan

        # If the entire column is warmer than the target temperature, ILD is the bottom depth.
        if np.nanmin(temp_col) > temp_ild_val:
            return bottom_depth
        else:
            # Find the first depth index where temperature is colder than the target
            try:
                # np.argmax finds the first 'True' value
                ind = np.argmax(temp_col < temp_ild_val)

                # If the first point is already colder, ILD is 0. Or if no change (ind=0)
                if ind == 0:
                    # Check if the very first temp value is already below target
                    if temp_col[0] < temp_ild_val:
                        return depth_col[0] # ILD is the shallowest depth
                    # This case occurs if the value is not found, argmax returns 0.
                    else: 
                        return np.nan

                # Get the temperature and depth values just above and at the crossing point
                temp_segment = temp_col[ind-1:ind+1]
                depth_segment = depth_col[ind-1:ind+1]

                # Linearly interpolate to find the exact depth of the target temperature
                # Note: np.interp expects x-coordinates (temp) to be increasing. We sort them.
                if temp_segment[0] > temp_segment[1]: # Normal case (temp decreases with depth)
                    return np.interp(temp_ild_val, temp_segment[::-1], depth_segment[::-1])
                else: # Inverted temperature
                    return np.interp(temp_ild_val, temp_segment, depth_segment)

            except (ValueError, IndexError):
                # Handle cases with all NaNs or other unexpected errors
                return np.nan

    def calculate_ild(self) -> xr.Dataset:
        """
        Calculates the Isothermal Layer Depth (ILD) for the entire dataset.
        """

        da_ild = xr.apply_ufunc(
            self.__class__.column_ild,
            self.da_sst,
            self.da_thetao,
            self.da_depth,
            self.da_bottom_depth,
            kwargs={'ild_temp_offset': self.ild_temp_offset},
            input_core_dims=[[], [self.depth_dim_name], [self.depth_dim_name], []],
            output_core_dims=[[]],
            exclude_dims=set((self.depth_dim_name,)),
            vectorize=True,
            dask="parallelized",
            output_dtypes=[float]
        )

        # create correct meta data
        da_ild.attrs['units'] = 'meters'
        da_ild.attrs['long_name'] = f'Isothermal Layer Depth ({self.ild_temp_offset})'
        da_ild.attrs['standard_name'] = 'isothermal_layer_depth'

        # create name for the variable
        ds_ild = xr.Dataset()
        ds_ild['ild'] = da_ild

        return ds_ild
