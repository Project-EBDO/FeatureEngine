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
    signal.foldLeft(0.0)((acc, v) => acc + pow(v,2))
  }

  def fromFFTTwoSided(fft: Array[Double]): Double = {
    val nonNormalizedEnergy = fft
      .foldLeft(0.0)((acc, v) => acc + pow(v,2))

    nonNormalizedEnergy / (fft.length / 2.0)
  }

  def fromFFTOneSided(fft: Array[Double]): Double = {
    val isNfftEven = (nfft % 2 == 0)
    var energy = 0.0

    var i = 1

    while (i < nfft/2) {
      energy += 2.0 * pow(fft(2*i), 2)
      energy += 2.0 * pow(fft(2*i+1), 2)
      i += 1
    }

    energy += pow(fft(0), 2)
    energy += pow(fft(fft.length-2), 2) * (if (isNfftEven) 1.0 else 2.0)
    energy += pow(fft(fft.length-1), 2) * (if (isNfftEven) 1.0 else 2.0)

    energy / nfft
  }

  def fromPSD(psd: Array[Double]): Double = {
    psd.sum
  }
}
