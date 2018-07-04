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

import scipy.signal
from itertools import product

from tol import tol
from soundParameters import SoundParameters
from referenceValueParameters import ReferenceValueParameters


class Features:
    def __init__(self, soundsParams, algos, algoParams):
        self.algoParams = algoParams
        self.soundsParams = soundsParams
        self.algos = algos

        self.refValuesParameters = []

    def __str__(self):
        return "\n".join([str(rvp) for rvp in self.refValuesParameters])

    def generate(self):
        for (soundId, sysBits, wavBits,
             sampleNumber, samplingRate, chanNumber) in self.soundsParams:

            soundParam = SoundParameters(soundId, sysBits, wavBits,
                                         sampleNumber, samplingRate,
                                         chanNumber)

            sound, fs = soundParam.read()

            for nfft, winSize, offset in self.algoParams:
                if (winSize > nfft or offset > winSize):
                    continue

                fPSD, tPSD, vPSD = scipy.signal.spectrogram(
                    x=sound, fs=fs, window='hamming', nperseg=winSize,
                    noverlap=winSize-offset, nfft=nfft, detrend=False,
                    return_onesided=True, axis=-1,
                    mode='psd', scaling="density")

                fFFT, tFFT, vFFT = scipy.signal.stft(
                    x=sound, fs=fs, window='hamming', noverlap=winSize-offset,
                    nperseg=winSize, nfft=nfft, detrend=False,
                    return_onesided=True, boundary=None, padded=False, axis=-1)

                fWelch, vWelch = scipy.signal.welch(
                    x=sound, fs=fs, window='hamming',
                    detrend=False, noverlap=winSize-offset,
                    nperseg=winSize, nfft=nfft, return_onesided=True,
                    scaling='density', axis=-1)

                vTOL = tol(psd=vWelch, samplingRate=fs, nfft=nfft,
                           winSize=winSize, lowFreq=0.2*fs, highFreq=0.4*fs)

                for algo in self.algos:
                    valueSpace = "real" if algo is not "vFFT" else "comp"

                    if (algo == "vTOL" and nfft < fs):
                        continue

                    refVal = ReferenceValueParameters(
                        soundParam=soundParam, algorithm=algo,
                        winSize=winSize, nfft=nfft, offset=offset,
                        valueSpace=valueSpace
                    )

                    refVal.setValue(locals()[algo])
                    self.refValuesParameters.append(refVal)

    def writeAll(self):
        for refValue in self.refValuesParameters:
            refValue.write()


if __name__ == "__main__":

    soundParam = [("Sound1", 64, 24, 9811, 3906.0, 1)]

    nffts = [256, 128]
    winSizes = [256, 128]
    offsets = [128]

    algoParams =[(3906, 3906, 3906)]
    # algoParams = list(product(nffts, winSizes, offsets))

    algos = ["vPSD", "fFFT", "vFFT", "vWelch", "fWelch", "vTOL"]

    spec = Features(soundParam, algos, algoParams)

    spec.generate()

    print(spec)

    spec.writeAll()
