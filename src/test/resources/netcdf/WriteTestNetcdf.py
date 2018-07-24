#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2017-2018 Project-ODE
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# python script used to generate test.nc file
# base on the netcdf4 python lib documentation: http://unidata.github.io/netcdf4-python/
# Authors: Alexandre Degurse


from netCDF4 import Dataset
import numpy as np

rootgrp = Dataset("test.nc", "w")

lat = rootgrp.createDimension("lat", 10)
lon = rootgrp.createDimension("lon", 10)
time = rootgrp.createDimension("time", None)

latitudes = rootgrp.createVariable("lat","f4",("lat",))
longitudes = rootgrp.createVariable("lon","f4",("lon",))
times = rootgrp.createVariable("time","i4",("time",))
windSpeed = rootgrp.createVariable("wind_intensity","f4",("time","lat","lon"))

latitudes[:] = np.arange(1.0, 11.0, 1.0)
longitudes[:] = np.arange(-10.0, 0.0, 1.0)
times[:] = np.arange(0, 20)

for itime in range(20):
    for ilon in range(10):
        for ilat in range(10):
            windSpeed[itime,ilon,ilat] = np.cos(times[itime]) * np.sin(longitudes[ilon] * latitudes[ilat])

rootgrp.close()
