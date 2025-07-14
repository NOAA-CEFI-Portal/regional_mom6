#!/usr/bin/env python
"""
The module include Regridding class
for regional mom6 field

"""
import numpy as np
import xarray as xr
import xesmf as xe

class Regridding:
    """class to handle regridding 

    Examples
    -------
    ds_var = xr.merge([ds_var, ds_static], combine_attrs='override')

    # call regridding class
    class_regrid = Regridding(ds_var, 'tob', 'geolon', 'geolat')

    # perform regridding for 900x800 regular spacing grid
    ds_regrid = class_regrid.regrid_regular(900, 800)
    
    # perform regridding to a specific grid used in ds_specific
    ds_regrid = class_regrid.regrid_specific(ds_specific)
    """
    def  __init__(
        self,
        ori_dataset : xr.Dataset,
        varname : str,
        ori_xname : str = 'lon',
        ori_yname : str = 'lat',
    ):
        """preprocess and save the input data to self

        Parameters
        ----------
        ori_dataset : xr.Dataset
            dataset that contain the raw data matrix
        regrid_dataset : xr.Dataset
            dataset that contain the coordinate for regridding
        varname : str
            variable name
        xname : str
            x coordinate name
        yname : str
            y coordinate name

        Raises
        ------


        """
        # prepare dataset for interpolation
        try:
            self.ori_dataset = ori_dataset.rename({ori_xname:'lon',ori_yname:'lat'})
        except KeyError as e :
            raise KeyError(
                f"Coordinates should have {ori_xname} & {ori_yname}"
            ) from e

        self.varname = varname

    @staticmethod
    def generate_regridder(
        ds_ori : xr.Dataset,
        ds_regrid :xr.Dataset
    )->xe.Regridder:
        """create regridder for interpolation
        fixed to bilinear interpolation at the moment

        Parameters
        ----------
        ds_ori : xr.Dataset
            original dataset that need interpolation
        ds_regrid : xr.Dataset
            the dataset contains coordinate that need to be interpolated to

        Returns
        -------
        xe.Regridder
            regridder object used for regridding
        """
        regridder = xe.Regridder(
            ds_ori, ds_regrid, "bilinear", unmapped_to_nan=True
        )
        return regridder

    def regular_grid(self,nx:int,ny:int)->xr.Dataset:
        """create a regular grid for regridding
        
        Parameters
        ----------
        ds : xr.Dataset
            original dataset

        Returns
        -------
        xr.Dataset
            regular grid dataset
        """
        # Create longitude and latitude arrays (e.g., 1D arrays)
        x = np.linspace(
            self.ori_dataset['lon'].min().data,
            self.ori_dataset['lon'].max().data,
            nx-1
        )
        y = np.linspace(
            self.ori_dataset['lat'].min().data,
            self.ori_dataset['lat'].max().data,
            ny-1
        )

        # Create a dummy data variable (optional, can be empty)
        data = xr.DataArray(
            data=None,
            coords={'lon': x, 'lat': y},
            dims=('lon', 'lat')
        )

        # Create an xarray dataset with empty dataarray
        ds = xr.Dataset({'var': data})
        return ds

    def regrid_regular(self,nx:int,ny:int)->xr.Dataset:
        """regrid the data

        Returns
        -------
        xr.Dataset
            regridded dataset
        """
        # create regular grid
        ds_regrid = self.regular_grid(
            nx=nx,
            ny=ny
        )

        # generate regridder
        regridder = self.generate_regridder(self.ori_dataset, ds_regrid)

        # regrid to tracer point(memory intensive if the whole dataset is big)
        da = regridder(self.ori_dataset[self.varname])
        if hasattr(da.data, "persist"):
            print('.persisting data...')
            da = da.persist() #type: ignore


        # create dataset
        ds = xr.Dataset({self.varname: da})

        return ds

    def regrid_specific(self,ds_specific)->xr.Dataset:
        """regrid the data to the same grid as ds_specific
        ds_specific should have the dim name 'lon' and 'lat'

        Parameters
        ----------
        ds_specific : xr.Dataset
            dataset that contains the coordinate for regridding
        
        Returns
        -------
        xr.Dataset
            regridded dataset
        """

        # generate regridder
        regridder = self.generate_regridder(self.ori_dataset, ds_specific)

        # regrid to tracer point(memory intensive if the whole dataset is big)
        da = regridder(self.ori_dataset[self.varname])
        if hasattr(da.data, "persist"):
            print('.persisting data...')
            da = da.persist() #type: ignore

        # create dataset
        ds = xr.Dataset({self.varname: da})

        return ds
