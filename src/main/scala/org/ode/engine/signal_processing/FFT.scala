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

import edu.emory.mathcs.jtransforms.fft.DoubleFFT_1D

/**
  * Wrapper class over edu.emory.mathcs.jtransforms.fft.DoubleFFT_1D that
  * computes FFT of nfft size over the signal of length nfft.
  *
  * Author: Paul Nguyen HD, Alexandre Degurse, Joseph Allemandou
  *
  */


class FFT(nfft: Int) {

  // Instantiate the low level class that computes the fft
  val lowLevelFtt: DoubleFFT_1D = new DoubleFFT_1D(nfft)

  val nfftEven: Boolean = nfft % 2 == 0

  /**
    * Function that computes FFT for an Array
    * The signal is zero-padded if needed (i.e. signal.length < nfft)
    * An IllegalArgumentException is thrown if signal.length > nfft
    *
    * Returns complex values represented by two consecutive Double, thus
    * r(2*i) = Re(v_i) and r(2*i + 1) = Im(v_i) where r is the FFT over
    * the signal and v_i the i'th complex value of the transformation
    *
    * @param signal The signal to process as an Array[Double] of length nfft
    * @return The FFT over the input signal as an Array[Double] of length nfft + (1 or 2)
    *
    */
  def compute(signal: Array[Double]) : Array[Double] = {
    if (signal.length > nfft) {
      throw new IllegalArgumentException(s"Incorrect signal length (${signal.length}) for FFT ($nfft)")
    }

    // Result array containing the original signal
    // followed by 0 up to nfft + (1 or 2) values
    // depending of nfft parity (1 if odd, 2 if even)
    val fft: Array[Double] = signal ++ Array.fill(nfft - signal.length + (if (nfftEven) 2 else 1))(0.0)

    // In place computation
    lowLevelFtt.realForward(fft)
    // Reordering of values to match regular layout
    // of real followed by imaginary one
    fft(nfft) = fft(1)
    fft(1) = 0.0

    fft
  }
}
