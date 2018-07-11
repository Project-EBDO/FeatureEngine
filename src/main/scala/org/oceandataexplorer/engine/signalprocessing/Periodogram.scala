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

package org.oceandataexplorer.engine.signalprocessing


/**
 * Class that provides periodogram function to compute Power Spectral Density (PSD).
 * Relies on several part of open source codes rearranged to fit Matlab / Python one-sided PSD:
 * https://github.com/lancelet/scalasignal/blob/master/src/main/scala/signal/PSD.scala
 * https://gist.github.com/awekuit/7496127
 *
 * @author Paul Nguyen HD, Alexandre Degurse
 *
 * @param nfft Size of fft-computation window
 * @param normalizationFactor The normalization factor for the periodogram.
 *                            Usually:
 *                              1.0 / (samplingRate * windowNormalizationFactor)
 *                                to get a power density
 *                              1.0 / windowNormalizationFactor
 *                                to get a power spectrum
 * @param samplingRate The sampling rate of the signal
 */
case class Periodogram
(
  nfft: Int,
  normalizationFactor: Double,
  samplingRate: Float
) extends FrequencyConvertible with Serializable {

  /**
   * Computes one-sided periodograms like in Matlab
   * and Python using the periodogram method (mode 'psd')
   * An IllegalArgumentException is thrown if
   * fft.length != 2*spectrumSize (ie fft is not one-sided)
   *
   * @param fft The one-sided fft used values
   * @return the one-sided periodogram
   */
  def compute(fft: Array[Double]) : Array[Double] = {
    if (fft.length != 2*spectrumSize) {
      throw new IllegalArgumentException(s"Incorrect fft length (${fft.length}) for Periodogram" +
        s"it should be a one-sided (${2*spectrumSize}) FFT")
    }

    val oneSidedPeriodogram: Array[Double] = new Array[Double](spectrumSize)

    // Precompute first value
    oneSidedPeriodogram(0) = normalizationFactor * (fft(0)*fft(0) + fft(1)*fft(1))

    // Using while with local variables on purpose -- See performance test
    // scalastyle:off while var.local

    // Start at the second value and iterate
    var i: Int = 1
    var i2: Int = 2
    var i2p1: Int = 3
    val last = spectrumSize - 1

    while (i < last) {
      oneSidedPeriodogram(i) = 2.0 * normalizationFactor * (fft(i2)*fft(i2) + fft(i2p1)*fft(i2p1))
      i += 1
      i2 = 2*i
      i2p1 = i2 + 1
    }
    // scalastyle:on while var.local

    // manually compute last value, depending on even
    // or not numbers of values (presence or not of nyquist frequency)
    // at the end of the while, i = spectrumSize-1 = oneSidedPeriodogram.length - 1,
    // Therefore, i2 and i2p1 are equal to what they should be to compute the last value
    oneSidedPeriodogram(last) = normalizationFactor * (fft(i2)*fft(i2) + fft(i2p1)*fft(i2p1))

    if (!nfftEven) {
      oneSidedPeriodogram(last) *= 2.0
    }

    oneSidedPeriodogram
  }
}
