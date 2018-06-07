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
  * Can be used over raw signal, FFT one-sided, or PSD.
  *
  * Author: Alexandre Degurse
  */


class Energy(val nfft: Int) {

  val expectedFFTSize = nfft + (if (nfft % 2 == 0) 2 else 1)
  val expectedPSDSize = expectedFFTSize / 2

  def fromRawSignal(signal: Array[Double]): Double = {
    if (signal.length > nfft) {
      throw new IllegalArgumentException(s"Incorrect signal size (${signal.length}) for Energy (${nfft})")
    }
    signal.foldLeft(0.0)((acc, v) => acc + pow(v,2))
  }

  def fromFFT(fft: Array[Double]): Double = {

    if (fft.length != expectedFFTSize) {
      throw new IllegalArgumentException(s"Incorrect fft size (${fft.length}) for Energy (${expectedFFTSize})")
    }

    val nfftEven = (nfft % 2 == 0)
    var energy = 0.0

    // start at 1 to not duplicate DC
    var i = 1

    /**
     * add the energy of all the points between DC and Nyquist' freq
     * An interesting point to notice is that aggregating values up to nfft / 2 and then adding
     * values for fft.length - 2 and fft.length -2 works for both nfft pair or odd
     * since expectedFFTSize is nfft / 2 + 2 if nfft is even and nfft + 1 if nfft is odd
     */
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
    if (psd.length != expectedPSDSize) {
      throw new IllegalArgumentException(s"Incorrect PSD size (${psd.length}) for Energy (${expectedPSDSize})")
    }

    psd.sum
  }
}
