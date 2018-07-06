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

# Authors: Dorian Cazau, Alexandre Degurse

import scipy.signal
# from itertools import product

from tol import tol
from SoundHandler import SoundHandler
from ResultsHandler import ResultsHandler


class FeatureGenerator:
    def __init__(self, resultsHandlers):
        self.resultsHandlers = resultsHandlers

    def __str__(self):
        return "\n".join([str(rh) for rh in self.resultsHandlers])

    def generate(self):
        for resultsHandler in self.resultsHandlers:

            sound, fs = resultsHandler.soundHandler.read()

            winSize = resultsHandler.winSize
            offset = resultsHandler.offset
            nfft = resultsHandler.nfft

            if (resultsHandler.algorithm in ["vFFT", "fFFT"]):
                fFFT, vFFT = scipy.signal.stft(
                    x=sound, fs=fs, window='hamming', noverlap=winSize-offset,
                    nperseg=winSize, nfft=nfft, detrend=False,
                    return_onesided=True, boundary=None,
                    padded=False, axis=-1)[::2]

                resultsHandler.setValue(
                    vFFT if resultsHandler.algorithm is "vFFT" else fFFT
                )

            elif resultsHandler.algorithm in ["vPSD", "fPSD"]:
                fPSD, vPSD = scipy.signal.spectrogram(
                    x=sound, fs=fs, window='hamming', nperseg=winSize,
                    noverlap=winSize-offset, nfft=nfft, detrend=False,
                    return_onesided=True, axis=-1,
                    mode='psd', scaling="density")[::2]

                resultsHandler.setValue(
                    vPSD if resultsHandler.algorithm is "vPSD" else fPSD
                )

            elif resultsHandler.algorithm in ["fWelch", "vWelch", "vTOL"]:
                fWelch, vWelch = scipy.signal.welch(
                    x=sound, fs=fs, window='hamming',
                    detrend=False, noverlap=winSize-offset,
                    nperseg=winSize, nfft=nfft, return_onesided=True,
                    scaling='density', axis=-1)

                if resultsHandler.algorithm is not "vTOL":
                    resultsHandler.setValue(
                        fWelch if resultsHandler.algorithm is "fWelch"
                        else vWelch
                    )

                else:
                    vTOL = tol(psd=vWelch, samplingRate=fs,
                               nfft=nfft, winSize=winSize,
                               lowFreq=0.2 * fs, highFreq=0.4 * fs)

                    resultsHandler.setValue(vTOL)

            else:
                raise("Unsupported algorithm: " + resultsHandler.algorithm)

    def writeAll(self):
        for resultsHandler in self.resultsHandlers:
            resultsHandler.write()


if __name__ == "__main__":

    soundHandler1 = SoundHandler("Sound1", 64, 24, 9811, 3906.0, 1)
    soundHandler2 = SoundHandler("Sound2", 64, 24, 3120, 2000.0, 1)

    resultsHandler = ResultsHandler(soundHandler=soundHandler2,
                                    algorithm="vTOL", nfft=2000,
                                    winSize=2000, offset=2000)

    generator = FeatureGenerator([resultsHandler])

    generator.generate()

    print(generator)

    generator.writeAll()
