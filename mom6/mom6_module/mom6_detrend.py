"""
This is the module to implement the detrending

"""
from typing import Tuple
import xarray as xr

class ForecastDetrend:
    """Detrend class for forecast data"""
    def __init__(
        self,
        da_data : xr.DataArray,
        initialization_name : str = 'init',
        member_name : str = 'member',
    ) -> None:
        """
        Parameters
        ----------
        da_data : xr.DataArray
            The dataarray one want to use to 
            detrend.
        initialization_name : str, optional
            initialization dimension name, by default 'init'
        member_name : str, optional
            ensemble member dimension name, by default 'member'
        """
        self.data = da_data
        self.init = initialization_name
        self.mem = member_name

    def polyfit_coef(
        self,
        deg: int = 1
    ) -> xr.Dataset:
        """determine the polyfit coefficient based on
        lead-time-dependent forecast ensemble mean anomalies

        Parameters
        ----------
        deg : int, optional
            the order of polynomical fit to use for determining the
            fit coefficient, by default 1

        Returns
        -------
        xr.Dataset
            coefficient of the polynomical fit
        """

        # calculate the ensemble mean of the anomaly
        da_ensmean = self.data.mean(dim=self.mem)
        # use the ensemble mean anomaly to determine lead time dependent trend
        ds_p = da_ensmean.polyfit(dim=self.init, deg=deg, skipna=True).compute()

        return ds_p

    def detrend_linear(
        self,
        precompute_coeff : bool = False,
        ds_coeff : xr.Dataset = None,
        in_place_memory_replace : bool = False
    ) -> Tuple[xr.DataArray,xr.Dataset]:
        """detrend the original data by using the 
        degree 1 ployfit coeff

        Returns
        -------
        xr.DataArray
            the data with linear trend removed
        """
        if precompute_coeff:
            ds_p = ds_coeff
        else:
            # get degree 1 polyfit coeff
            ds_p = self.polyfit_coef(deg=1)

        # # calculate linear trend based on polyfit coeff
        # da_linear_trend = xr.polyval(self.data[self.init], ds_p.polyfit_coefficients)
        # # remove the linear trend
        # da_detrend = (self.data - da_linear_trend).persist()

        if in_place_memory_replace:
            self.data = (
                self.data-
                xr.polyval(self.data[self.init], ds_p.polyfit_coefficients)
            ).persist()
            return self.data, ds_p
        else:
            da_detrend = (
                self.data -
                xr.polyval(self.data[self.init], ds_p.polyfit_coefficients)
            ).persist()
            return da_detrend,ds_p
