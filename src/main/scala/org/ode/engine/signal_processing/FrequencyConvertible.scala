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

  /**
   * Sampling rate of the sound the FFT is computed upon
   */
  val samplingRate: Float

  /**
   * The size of the feature to be converted
   */
  val featureSize: Int

  /**
   * Function converting a frequency to a index
   *
   * @param freq Frequency to be converted
   * @return Index corresponding to the given frequency
   */
  def frequencyToIndex(freq: Double): Int

  /**
   * Function converting a index to a frequency
   *
   * @param idx Index to be converted
   * @return Frequency correspondig to the given index
   */
  def indexToFrequency(idx: Int): Double

  /**
   * Function computing the frequency vector given a nfft and a samplingRate
   *
   * @return The frequency vector that corresponds to the current nfft and samplingRate
   */
  def frequencyVector(): Array[Double] = {
    (0 until featureSize).map(idx => indexToFrequency(idx)).toArray
  }
}
