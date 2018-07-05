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
 * Trait providing frequency conversion function for frequency features
 *
 * @author Alexandre Degurse
 */
trait FrequencyConvertible extends Serializable {
  /**
   * Size of the fft-computation window
   */
  val nfft: Int

  private val nfftEven = nfft % 2 == 0

  /**
   * Sampling rate of the sound the FFT is computed upon
   */
  val samplingRate: Float

  /**
   * The default size of the feature is the size of a Power Spectrum
   */
  val featureSize: Int = if (nfftEven) nfft / 2 + 1 else (nfft + 1) / 2

  /**
   * Function converting a frequency to a index in the Power Spectrum
   *
   * @param freq Frequency to be converted
   * @return Index in spectrum that corresponds to the given frequency
   */
  def frequencyToIndex(freq: Double): Int = {
    if (freq > samplingRate / 2.0 || freq < 0.0) {
      throw new IllegalArgumentException(
        s"Incorrect frequency ($freq) for conversion (${samplingRate / 2.0})"
      )
    }

    (freq * nfft / samplingRate).toInt
  }

  /**
   * Function converting a index in the Power Spectrum to a frequency
   *
   * @param idx Index to be converted
   * @return Frequency that corresponds to the given index
   */
  def indexToFrequency(idx: Int): Double = {
    if (idx >= featureSize || idx < 0) {
      throw new IllegalArgumentException(
        s"Incorrect index ($idx) for conversion ($featureSize)"
      )
    }

    idx.toDouble * samplingRate / nfft
  }

  /**
   * Function computing the frequency vector given a nfft and a samplingRate
   *
   * @return The frequency vector that corresponds to the current nfft and samplingRate
   */
  def frequencyVector(): Array[Double] = {
    (0 until featureSize).map(idx => indexToFrequency(idx)).toArray
  }
}
