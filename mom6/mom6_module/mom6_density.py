"""
This module provides functions to calculate the density of seawater
based on temperature, salinity, and pressure using the UNESCO equation
of state.
"""

from typing import Union
import numpy as np
import gsw

# define typing used in the methods
FloatType = Union[float, np.float64]
ArrayLike = Union[FloatType, np.ndarray]

class Density:
    """
    This class provides methods to calculate the density of seawater EOS-80 and
    thermodynamic density of seawater TEOS-10
    based on temperature, salinity, and pressure.

    Notes
    -----
    All functions follow numpy broadcasting rules; function arguments must
    be broadcastable to the dimensions of the highest-dimensioned argument.
    Recall that with numpy broadcasting, extra dimensions are automatically
    added as needed on the left, but must be added explicitly as needed on the right.
    """

    @staticmethod
    def smow_dens(temperature: ArrayLike) -> ArrayLike:
        """
        Calculate the temperature based density of seawater based on equation of state (EOS-80).

        Code base provided by : Allison Cluett

        Parameters
        ----------
        temperature : float or array-like
            Temperature in degrees Celsius.

        Returns
        -------
        rho : float or ndarray
            Density in kg/m^3 at the given temperature(s).

        Notes
        -----
        Converted from Matlab toolbox function
        The coefficients are taken from the UNESCO 1983 polynomial for seawater density (Eq.14).
        https://repository.oceanbestpractices.org/bitstream/handle/11329/109/059832eb.pdf?sequence=1&isAllowed=y
        """
        # Define constants:
        a0 = 999.842594
        a1 = 6.793952e-2
        a2 = -9.095290e-3
        a3 = 1.001685e-4
        a4 = -1.120083e-6
        a5 = 6.536332e-9

        t68 = temperature * 1.00024
        rho = a0 + (a1 + (a2 + (a3 + (a4 + a5 * t68) * t68) * t68) * t68) * t68
        return rho

    @staticmethod
    def sw_dens(salinity: ArrayLike, temperature: ArrayLike) -> ArrayLike:
        """
        Calculate the density of seawater based on equation of state (EOS-80).

        Code base provided by : Allison Cluett

        Parameters
        ----------
        salinity : float or array-like
            Salinity in Practical Salinity Units (PSU).
        temperature : float or array-like
            Temperature in degrees Celsius (°C).

        Returns
        -------
        rho : float or ndarray
            Density of seawater in kg/m^3 at the given salinity and temperature (P=0).

        Notes
        -----
        Converted from Matlab toolbox function
        The coefficients are taken from the UNESCO 1983 polynomial for seawater density (Eq.13).
        https://repository.oceanbestpractices.org/bitstream/handle/11329/109/059832eb.pdf?sequence=1&isAllowed=y
        """

        # Define constants
        t68 = temperature * 1.00024
        b0 = 8.24493e-1
        b1 = -4.0899e-3
        b2 = 7.6438e-5
        b3 = -8.2467e-7
        b4 = 5.3875e-9
        c0 = -5.72466e-3
        c1 = 1.0227e-4
        c2 = -1.6546e-6
        d0 = 4.8314e-4

        rho = (
            Density.smow_dens(temperature) +
            (b0 + b1*t68 + b2*t68**2 + b3*t68**3 + b4*t68**4) * salinity +
            (c0 + c1*t68 + c2*t68**2) * salinity**1.5 +
            d0 * salinity**2
        )
        return rho

    @staticmethod
    def teos10_sigma0(
        salinity: ArrayLike,
        temperature: ArrayLike,
        depth: ArrayLike,
        longitude: ArrayLike,
        latitude: ArrayLike
    ) -> ArrayLike:
        """ 
        The function is for calculating the potential density of reference pressure 
        of 0 dbar based on the TEOS-10

        The function is using the GSW-Python module. The module is designed 
        to follow the new standard from http://www.teos-10.org/ . 

        Modules used:
        - gsw (https://teos-10.github.io/GSW-Python/intro.html)

        Functions used:
        - gsw.p_from_z : calculate pressure using depth
        - gsw.SA_from_SP : calculate absolute salinity from pratical salinity
        - gsw.CT_from_pt : calculate conservative temperature from potential temperature
        - gsw.sigma0 : calculate potential density anomaly from conservative temperature 
                       and absolute salinity

        Parameters
        ----------
        salinity : float or np.ndarray
            Pratical salinity. The DataArray need to have depth in the unit 
            of meters or dbar for correct calculation.
        temperature : float or np.ndarray
            Potential temperature with reference level at 0 dbar (sea level).
            The DataArray need to have depth in the unit of meters or dbar for
            correct calculation.
        depth : float or np.ndarray
            Depth. The DataArray need to have depth in the unit
            of meters or dbar for correct calculation.
        longitude : float or np.ndarray
            Longitude. The longitude in the unit
            of degrees for correct calculation.
        latitude : float or np.ndarray
            Latitude. The latitude in the unit
            of degrees for correct calculation.


        Returns
        -------
        sigma0 : np.ndarray
            potential density with respect to a reference pressure of 0 dbar,

        Raises
        ------
        ValueError
            If depth is not positive and increasing.
        """

        # check if depth increase monotonically and is positive
        if not np.all(np.diff(depth) > 0) or np.any(depth < 0):
            raise ValueError("depth must be positive and increase monotonically.")

        # calculate pressure using depth
        # (gsw.p_from_z need depth to decrease with depth - more negative going down)
        pressure = gsw.p_from_z(
            -depth,
            latitude,
            geo_strf_dyn_height=0,
            sea_surface_geopotential=0
        )

        # calculate absolute salinity from pratical salinity
        abs_sal = gsw.SA_from_SP(salinity, pressure, longitude, latitude)

        # calculate conservative temperature from potential temperature
        cons_temp = gsw.CT_from_pt(abs_sal, temperature)

        # calculate in-situ density from conservative temp and absolute salinity
        # rho = gsw.rho(abs_sal, cons_temp, pressure)

        # calculate potential density of reference pressure of
        # 0 dbar from conservative temp and absolute salinity
        # the original gsw.signma0 function output potential density
        # anomaly with respect to a reference pressure of 0 dbar,
        # that is, potential density - 1000 kg/m^3.
        # Therefore, adding back 1000 for the absolute potential density value
        sigma0 = gsw.sigma0(abs_sal, cons_temp)+1000

        return sigma0
