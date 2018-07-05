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
 * @author Paul Nguyen HD, Alexandre Degurse, Joseph Allemandou
 *
 * @param nfft The size of the fft-computation window
 * @param samplingRate The sampling rate of the signal
 */
case class FFT
(
  nfft: Int,
  samplingRate: Float
) extends FrequencyConvertible with Serializable {

  // Instantiate the low level class that computes the fft
  // This class needs to be a var in order to be reinitialised
  // after object deserialization
  // scalastyle:off var.field
  @transient
  private var lowLevelFtt: DoubleFFT_1D = new DoubleFFT_1D(nfft)
  // scalastyle:on var.field

  /**
   * Function converting a frequency to a index in the spectrum
   *
   * @param freq Frequency to be converted
   * @return Index in spectrum that corresponds to the given frequency
   */
  override def frequencyToIndex(freq: Double): Int = 2 * super.frequencyToIndex(freq)

  /**
   * Function converting a index in the spectrum to a frequency
   *
   * @param idx Index to be converted
   * @return Frequency that corresponds to the given index
   */
  override def indexToFrequency(idx: Int): Double = super.indexToFrequency(idx / 2)

  /**
   * Function computing the frequency vector given a nfft and a samplingRate for FFT
   *
   * @return The frequency vector that corresponds to the current nfft and samplingRate
   */
  override lazy val frequencyVector: Array[Double] = {
    (0 until 2 * spectrumSize).map(idx => indexToFrequency(idx)).toArray
  }

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
   */
  def compute(signal: Array[Double]) : Array[Double] = {
    if (signal.length > nfft) {
      throw new IllegalArgumentException(
        s"Incorrect signal length (${signal.length}) for FFT ($nfft)"
      )
    }

    // Initialise lowLevelFtt if null (seserialization)
    // scalastyle:off null
    if (null == lowLevelFtt) {
      lowLevelFtt = new DoubleFFT_1D(nfft)
    }
    // scalastyle:on null

    // Result array containing the original signal
    // followed by 0 up to nfft + (1 or 2) values
    // depending of nfft parity (1 if odd, 2 if even)
    val fft: Array[Double] = signal ++
      Array.fill(nfft - signal.length + (if (nfftEven) 2 else 1))(0.0)

    // In place computation
    lowLevelFtt.realForward(fft)
    // Reordering of values to match regular layout
    // of real followed by imaginary one
    fft(nfft) = fft(1)
    fft(1) = 0.0

    fft
  }
}
