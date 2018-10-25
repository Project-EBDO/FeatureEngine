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


import soundfile


class SoundHandler:
    def __init__(
        self,
        fileId,
        sysBits,
        wavBits,
        sampleNumber,
        sampleRate,
        chanNumber
    ):

        self.fileId = fileId
        self.sysBits = sysBits
        self.wavBits = wavBits
        self.sampleNumber = sampleNumber
        self.sampleRate = sampleRate
        self.chanNumber = chanNumber

        self.strParameters = "_".join(
            [str(p) for p in [fileId, sysBits, wavBits,
                              sampleNumber, sampleRate, chanNumber]]
            )

    def __str__(self):
        return self.strParameters

    @property
    def fileName(self):
        return str(self) + ".wav"

    def read(self):
        sig, fs = soundfile.read("../../sounds/" + self.fileName)

        assert(self.sampleRate == fs)
        assert(len(sig) == self.sampleNumber)

        return sig, fs


if __name__ == "__main__":
    s = SoundHandler("Sound1", 64, 24, 9811, 3906.0, 1)
    print(s)
