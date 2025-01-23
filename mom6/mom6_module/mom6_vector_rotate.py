#!/usr/bin/env python
"""
The module include VectorRotation class
for regional mom6 vector field

The VectorRotation class is following the following steps:
1. regrid from the raw u,v grid (Arakawa C grid) to tracer grid
2. rotate the regridded u,v to true u (east) and true v (north)
The steps are following the Ross et al 2023

"""

import xarray as xr
import xesmf as xe

class VectorRotation:
    """class to handle vector rotation due to the grid design"""
    def  __init__(
        self,
        dataset_u : xr.Dataset,
        u_varname : str,
        dataset_v : xr.Dataset,
        v_varname : str,
        ds_rotate : xr.Dataset,
        rot_cosname : str = 'cosrot',
        rot_sinname : str = 'sinrot'
    ):
        """preprocess and save the input data to self

        Parameters
        ----------
        dataset_u : xr.Dataset
            dataset that contain the raw u matrix, need to contain coordinate 
            'geolon_u','geolat_u'
        u_varname : str
            raw u variable name
        dataset_v : xr.Dataset
            dataset that contain the raw v matrix, need to contain coordinate 
            'geolon_v','geolat_v'
        v_varname : str
            raw v variable name
        ds_rotate : xr.Dataset
            dataset that contain the rotation matrix, need to contain coordinate 
            'geolon','geolat'
        rot_cosname : str, optional
            rotation matrix cosine variable name, by default 'cosrot'
        rot_sinname : str, optional
            rotation matrix sine variable name, by default 'sinrot'

        Raises
        ------
        KeyError
            Catching the improper raw u dataset coordinate name
        KeyError
            Catching the improper raw v dataset coordinate name
        KeyError
            Catching the improper rotation matrix coordinate name
        """
        # prepare dataset for interpolation
        try:
            self.u = dataset_u.rename({'geolon_u':'lon','geolat_u':'lat'})
        except KeyError as e :
            raise KeyError(
                "Coordinates should have 'geolon_u' & 'geolat_u'"
            ) from e
        self.uname = u_varname

        try:
            self.v = dataset_v.rename({'geolon_v':'lon','geolat_v':'lat'})
        except KeyError as e :
            raise KeyError(
                "Coordinates should have 'geolon_v' & 'geolat_v'"
            ) from e
        self.vname = v_varname

        try:
            self.rotate = ds_rotate.rename({'geolon':'lon','geolat':'lat'})
        except KeyError as e :
            raise KeyError(
                "Coordinates should have 'geolon' & 'geolat'"
            ) from e
        self.cosrot = rot_cosname
        self.sinrot = rot_sinname

    @staticmethod
    def generate_regridder(
        ds_ori : xr.Dataset,
        ds_regrid :xr.Dataset
    )->xe.Regridder:
        """create regridder for interpolation

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

    def generate_true_uv(self)->dict:
        """rotate the raw u, v to true u (east) and true v (north)

        Steps:
        1. interpolate/regrid from u v point to tracer point
        2. linear combined rotate u v for true u v
            True east U  =  u*COSROT + v*SINROT
            True north V = -u*SINROT + v*COSROT

        Returns
        -------
        dict
            u: the true u dataarray
            v: the true v dataarray
        """
        # generate regridder
        regridder_u2t = self.generate_regridder(self.u, self.rotate)
        regridder_v2t = self.generate_regridder(self.v, self.rotate)

        # regrid to tracer point(memory intensive if the whole dataset is big)
        da_u_regrid = regridder_u2t(self.u[self.uname]).persist()
        da_v_regrid = regridder_v2t(self.v[self.vname]).persist()

        # rotate the regridded u, v
        da_u_true = (
            da_u_regrid*self.rotate[self.cosrot]+
            da_v_regrid*self.rotate[self.sinrot]
        )
        da_v_true = (
            -da_u_regrid*self.rotate[self.sinrot]+
            da_v_regrid*self.rotate[self.cosrot]
        )

        # correct rotate geolon geolat to data
        da_u_true['lon'] = self.u.geolon
        da_u_true['lat'] = self.u.geolat
        da_v_true['lon'] = self.v.geolon
        da_v_true['lat'] = self.v.geolat

        # drop lon lat from the dataset
        da_u_true = da_u_true.drop_vars(['lon','lat'])
        da_v_true = da_v_true.drop_vars(['lon','lat'])

        return {
            'u': da_u_true,
            'v': da_v_true
        }
