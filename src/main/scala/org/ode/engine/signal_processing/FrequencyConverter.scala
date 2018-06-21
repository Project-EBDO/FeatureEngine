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
 * Trait that provides frequency conversion function for final features
 *
 * @author Alexandre Degurse
 */
trait FrequencyConverter extends Serializable {
  /**
   * the size of the fft-computation window
   */
  val nfft: Int

  /**
   * the sampling rate of the sound the FFT is computed upon
   */
  val samplingRate: Float

  protected val nfftEven: Boolean = nfft % 2 == 0
  protected val spectrumSize: Int = (if (nfftEven) (nfft + 2) / 2 else (nfft + 1) / 2)

  /**
   * Function that converts a frequency to a index in the spectrum
   * Spectrum designates either a Periodogram or a Welch, for both have
   * frequency vector given the same nfft and samplingRate
   *
   * @param freq The frequency to be converted
   * @return The index in spectrum that corresponds to the given frequency
   */
  def frequencyToSpectrumIndex(freq: Double): Int = (freq * nfft / samplingRate).toInt

  /**
   * Function that converts a index in the spectrum to a frequency
   * Spectrum designates either a Periodogram or a Welch, for both have
   * frequency vector given the same nfft and samplingRate
   *
   * @param idx The index to be converted
   * @return The frequency that corresponds to the given index
   */
  def spectrumIndexToFrequency(idx: Int): Double = idx.toDouble * samplingRate / nfft

  /**
   * Function computes the frequency vector given a nfft and a samplingRate
   *
   * @return The frequency vector that corresponds to the current nfft and samplingRate
   */
  def frequencyVector(): Array[Double] = (0 to spectrumSize).map(spectrumIndexToFrequency).toArray
}
