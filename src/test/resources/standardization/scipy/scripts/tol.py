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

# Authors: Alexandre Degurse


import numpy as np
import scipy.signal
import scipy.fftpack
from math import floor, log10

# We're using some accronymes here:
#   toc: third octave center
#   tob: third octave band


def tol(psd, samplingRate, nfft, winSize,
        lowFreq=None, highFreq=None):

    lowerLimit = 1.0
    upperLimit = max(samplingRate / 2.0,
                     highFreq if highFreq is not None else 0.0)

    if (lowFreq is None):
        lowFreq = lowerLimit

    if (highFreq is None):
        highFreq = upperLimit

    # when wrong lowFreq, highFreq are given,
    # computation falls back to default values
    if not lowerLimit <= lowFreq < highFreq <= upperLimit:
        lowFreq, highFreq = lowerLimit, upperLimit

    maxThirdOctaveIndex = floor(10 * log10(upperLimit))

    tobCenterFreqs = np.power(10, np.arange(0, maxThirdOctaveIndex+1)/10)

    def tobBoundsFromTOC(centerFreq):
        return centerFreq * np.power(10, np.array([-0.05, 0.05]))

    allTOB = np.array([tobBoundsFromTOC(tocFreq)
                       for tocFreq in tobCenterFreqs])

    tobBounds = np.array([tob for tob in allTOB
                          if tob[1] >= lowFreq
                          and tob[0] < highFreq
                          and tob[1] < upperLimit])

    def boundToIndex(bound):
        return np.array([floor(bound[0] * nfft / samplingRate),
                         floor(bound[1] * nfft / samplingRate)],
                        dtype=int)

    tobIndicies = np.array([boundToIndex(bound) for bound in tobBounds])

    ThridOctavePowerBands = np.array([
        np.sum(psd[indicies[0]:indicies[1]]) for indicies in tobIndicies
    ])

    tols = 10 * np.log10(ThridOctavePowerBands)

    return tols


if __name__ == "__main__":

    params = [
        {"nfft": 128, "winSize": 128, "samplingRate": 32.0,
         "freqLow": None, "freqHigh": None},
        {"nfft": 128, "winSize": 128, "samplingRate": 128.0,
         "freqLow": 35.2, "freqHigh": 50.5},
        {"nfft": 44800, "winSize": 44800, "samplingRate": 44800,
         "freqLow": 10000, "freqHigh": 20000}
    ]

    tolss = []
    bounds = []
    psds = []
    centers = []

    for param in params:
        signal = np.arange(1, param["winSize"] + 1)

        f, psd = scipy.signal.periodogram(x=signal, fs=param["samplingRate"],
                                          window='boxcar', nfft=param["nfft"],
                                          detrend=False, return_onesided=True,
                                          scaling='density')

        tols = tol(psd, param["samplingRate"],
                   param["nfft"], param["winSize"],
                   param["freqLow"], param["freqHigh"])

        tolss.append(tols)
        psds.append(psd)

        print("\nParameters: " + str(param))
        print("\n\tTOLs\n" + str(tols))
        print("\n\n")
