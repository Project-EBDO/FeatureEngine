/** Copyright (C) 2017 Project-ODE
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

package org.ode.engine.signal_processing;

import scala.math

/**
  * Class that provides Power Spectral Density (PSD) functions.
  * Relies on several part of open source codes rearranged to fit Matlab / Python one-sided PSD:
  * https://github.com/lancelet/scalasignal/blob/master/src/main/scala/signal/PSD.scala
  * https://gist.github.com/awekuit/7496127
  * 
  * Author: Paul Nguyen HD, Alexandre Degurse
  */

class PSD(val nfft: Int, val normalizationFactor: Double) {


  val isEven: Boolean = nfft % 2 == 0
  // compute number of unique samples in the transformed FFT
  val nUnique: Int = if (isEven) nfft / 2 + 1 else (nfft+ 1) / 2

  /**
   * Function that computes the one-sided Power Spectral Density (PSD)
   * like in Matlab and Python using the periodogram method (mode 'psd')
   * An IllegalArgumentException is thrown if fft.length != 2*nfft
   *
   * @param fft The two-sided fft used to compute the PSD
   * @return the one-sided PSD of the given fft
   */
  def periodogram(fft: Array[Double]) : Array[Double] = {
    if (fft.length != (2*nfft)) {
      throw new IllegalArgumentException(s"Incorrect fft length (${fft.length}) for PSD (${2*nfft})")
    }

    val oneSidedPSD: Array[Double] = new Array[Double](nUnique)
    
    // Precompute first value
    oneSidedPSD(0) = normalizationFactor * (fft(0)*fft(0) + fft(1)*fft(1))

    // Start at the second value and iterate
    var i: Int = 1
    var i2: Int = 2
    var i2p1: Int = 3
    val last = nUnique - 1

    while (i < last) {
      oneSidedPSD(i) = 2.0 * normalizationFactor * (fft(i2)*fft(i2) + fft(i2p1)*fft(i2p1))
      i += 1
      i2 = 2*i
      i2p1 = i2 + 1
    }

    // manually compute last value, depending on even or not numbers of values (presence or not of nyquist frequency)
    /** at the end of the while, i = nUnique-1 = oneSidedPSD.length - 1, 
     * Therefore, i2 and i2p1 are equal to what they should be to compute the last value 
     */
    oneSidedPSD(last) = normalizationFactor * (fft(i2)*fft(i2) + fft(i2p1)*fft(i2p1))

    if (!isEven) {
      oneSidedPSD(last) *= 2.0
    }

    return oneSidedPSD
  }
}
