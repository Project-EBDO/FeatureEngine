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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# Authors: Dorian Cazau, Alexandre Degurse

import os
import pandas
import numpy as np

from SoundHandler import SoundHandler


class ResultsHandler:
    def __init__(
        self,
        soundHandler,
        algorithm,
        nfft,
        winSize,
        offset,
        vSysBits=64
    ):

        self.soundHandler = soundHandler
        self.algorithm = algorithm
        self.nfft = nfft
        self.winSize = winSize
        self.offset = offset
        self.vSysBits = vSysBits

        self.value = None

        self.fileName = "_".join(
            [str(p) for p in [soundHandler, algorithm, nfft,
                              winSize, offset, vSysBits]
             ]) + ".csv"

    def __str__(self):
        return self.fileName

    def setValue(self, value):
        self.value = value

    def formatComplexResults(self):
        """
        Results containing complex values are reformatted following
        the same convention as in FeatureEngine, ie:
        [z_0, z_1, ... , z_n] => [Re(z_0), Im(z_0), Re(z_1), ... Im(z_n)]
        """
        initialShape = self.value.shape

        nSeg = initialShape[1]
        segLength = initialShape[0]

        valueAsScalaFormat = np.zeros((nSeg, 2*segLength), dtype=float)
        valueAsComplex = self.value.transpose()

        for i in range(nSeg):
            valueAsScalaFormat[i, ::2] = valueAsComplex[i].real
            valueAsScalaFormat[i, 1::2] = valueAsComplex[i].imag

        self.value = valueAsScalaFormat.transpose()


    def write(self):
        if self.value is None:
            raise("No values to write")

        if self.algorithm is "vFFT":
            self.formatComplexResults()

        valueDataFrame = pandas.DataFrame(self.value)

        # store using one line per time-result
        valueDataFrame = valueDataFrame.transpose()

        valueDataFrame.to_csv(os.path.join("../values/" + str(self)),
                              index=False, header=False,
                              sep=' ', float_format='%.16f')


if __name__ == "__main__":
    s = SoundHandler("Sound1", 64, 24, 9811, 3906.0, 1)
    resHandler = ResultsHandler(soundHandler=s, algorithm="vPSD", nfft=128,
                                winSize=128, offset=128)

    print(resHandler)
