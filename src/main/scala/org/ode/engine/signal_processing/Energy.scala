/** Copyright (C) 2017-2018 Project-ODE
  *
  * This program is free software: you can redistribute it and/or modify
  * it under the terms of the GNU General Public License as published by
  * the Free Software Foundation, either version 3 of the License, or
  * (at your option) any later version.
  *
  * This program is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  * GNU General Public License for more details.
  *
  * You should have received a copy of the GNU General Public License
  * along with this program.  If not, see <http://www.gnu.org/licenses/>.
  */

package org.ode.engine.signal_processing

import scala.math.{cos,Pi,pow,abs,sqrt}

/**
  * Class computing energy from signal information.
  * Can be used over raw signal, FFT singled or two sided, or PSD.
  *
  * Author: Alexandre Degurse
  */


class Energy(val nfft: Int) {

  def fromRawSignal(signal: Array[Double]): Double = {
    if (signal.length > nfft) {
      throw new IllegalArgumentException(s"Incorrect signal size (${signal.length}) for Energy (${nfft})")
    }
    signal.foldLeft(0.0)((acc, v) => acc + pow(v,2))
  }

  def fromFFTTwoSided(fft: Array[Double]): Double = {
    if (fft.length != 2 * nfft) {
      throw new IllegalArgumentException(s"Incorrect fft size (${fft.length}) for Energy (${2*nfft})")
    }

    val nonNormalizedEnergy = fft
      .foldLeft(0.0)((acc, v) => acc + pow(v,2))

    nonNormalizedEnergy / (fft.length / 2.0)
  }

  def fromFFTOneSided(fft: Array[Double]): Double = {
    val expectedSize = nfft + (if (nfft % 2 == 0) 2 else 1)

    if (fft.length != expectedSize) {
      throw new IllegalArgumentException(s"Incorrect fft size (${fft.length}) for Energy (${expectedSize})")
    }

    val nfftEven = (nfft % 2 == 0)
    var energy = 0.0

    // start at 1 to not duplicate DC
    var i = 1

    // add the energy of all the points between DC and Nyquist' freq
    while (i < nfft/2) {
      // duplicate the energy due to the symetry
      energy += 2.0 * pow(fft(2*i), 2)
      energy += 2.0 * pow(fft(2*i+1), 2)
      i += 1
    }

    // add DC's energy
    energy += pow(fft(0), 2)
    // add the energy around Nyquist' freq
    energy += pow(fft(fft.length-2), 2) * (if (nfftEven) 1.0 else 2.0)
    energy += pow(fft(fft.length-1), 2) * (if (nfftEven) 1.0 else 2.0)

    energy / nfft
  }

  def fromPSD(psd: Array[Double]): Double = {
    psd.sum
  }
}
