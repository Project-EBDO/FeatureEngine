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
 * 2-sided-FTT Wrapper to compare with the 1-sided we use
 *
 * @param nfft the fft-computation window size
 *
 * @author Alexandre Degurse
 */
class FFTTwoSided(nfft: Int) {

  import edu.emory.mathcs.jtransforms.fft.DoubleFFT_1D

  private val lowLevelFtt: DoubleFFT_1D = new DoubleFFT_1D(nfft)

  /**
   * Computes FFT over a signal portion
   * @param signal the signal portion (should be of smaller length then nfft)
   * @return the 2-sided-FFT computed over the signal portion
   */
  def compute(signal: Array[Double]) : Array[Double] = {
    if (signal.length > nfft) {
      throw new IllegalArgumentException(s"Incorrect signal length (${signal.length}) for FFT ($nfft)")
    }

    // new value that contains the signal and padded with nfft zeros
    // because the size doubles due to complex values
    val fft: Array[Double] = signal ++ Array.fill(2 * nfft - signal.length)(0.0)

    // // In place computation
    lowLevelFtt.realForwardFull(fft)

    fft
  }
}
