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

/**
  * Class that provides periodogram function to compute Power Spectral Density (PSD).
  * Relies on several part of open source codes rearranged to fit Matlab / Python one-sided PSD:
  * https://github.com/lancelet/scalasignal/blob/master/src/main/scala/signal/PSD.scala
  * https://gist.github.com/awekuit/7496127
  *
  * Author: Paul Nguyen HD, Alexandre Degurse
  *
  * @param nfft The length number of complex points of the spectrum that this class should handle.
  * @param normalizationFactor The normalization factor of the PSD, depends on the used method.
  */

class Periodogram(val nfft: Int, val normalizationFactor: Double) {

  val nfftEven: Boolean = nfft % 2 == 0
  // compute number of unique samples in the transformed FFT
  val uniqueSamples: Int = if (nfftEven) nfft / 2 + 1 else (nfft + 1) / 2

  /**
   * Function that computes the one-sided Power Spectral Density (PSD)
   * like in Matlab and Python using the periodogram method (mode 'psd')
   * An IllegalArgumentException is thrown if
   * fft.length != 2*uniqueSamples (ie fft is not one-sided)
   *
   * @param fft The one fft used to compute the PSD
   * @return the one-sided PSD of the given fft
   */
  def compute(fft: Array[Double]) : Array[Double] = {
    if (fft.length != 2*uniqueSamples) {
      throw new IllegalArgumentException(s"Incorrect fft length (${fft.length}) for PSD, it should be either a one-sided (${2*uniqueSamples}) or a two-sided (${2*nfft}) FFT")
    }

    val oneSidedPSD: Array[Double] = new Array[Double](uniqueSamples)

    // Precompute first value
    oneSidedPSD(0) = normalizationFactor * (fft(0)*fft(0) + fft(1)*fft(1))

    // Start at the second value and iterate
    var i: Int = 1
    var i2: Int = 2
    var i2p1: Int = 3
    val last = uniqueSamples - 1

    while (i < last) {
      oneSidedPSD(i) = 2.0 * normalizationFactor * (fft(i2)*fft(i2) + fft(i2p1)*fft(i2p1))
      i += 1
      i2 = 2*i
      i2p1 = i2 + 1
    }

    // manually compute last value, depending on even
    // or not numbers of values (presence or not of nyquist frequency)
    // at the end of the while, i = uniqueSamples-1 = oneSidedPSD.length - 1,
    // Therefore, i2 and i2p1 are equal to what they should be to compute the last value
    oneSidedPSD(last) = normalizationFactor * (fft(i2)*fft(i2) + fft(i2p1)*fft(i2p1))

    if (!nfftEven) {
      oneSidedPSD(last) *= 2.0
    }

    oneSidedPSD
  }
}
