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

package org.ode.engine.signal_processing

/**
 * Class that provides Welch Power Spectral Density estimate function.
 *
 * Author: Alexandre Degurse
 */

class WelchSpectralDensity(val nfft: Int) {

  val expectedPSDSize = (if (nfft % 2 == 0) nfft/2 + 1 else (nfft+1)/2)

  /**
   * That compute Wech estimate of the Power Spectral Density out of
   * multiple periodograms on the signal
   *
   * @param psds The PSDs on the signal that must be one-sided
   * This function doesn't handle the normalization of the returned PSD,
   * input PSDs must either be already normalized or the output PSD
   * must be normalized afterwards.
   * @return The Welch Power Spectral Density estimate over all the PSDs
   *
   */
  def compute(psds: Array[Array[Double]]): Array[Double] = {
    if (!psds.forall(psd => psd.length == expectedPSDSize)) {
      throw new IllegalArgumentException(s"Inconsistent psd lengths for Welch aggregation")
    }

    val psdAgg: Array[Double] = new Array[Double](expectedPSDSize)

    var i: Int = 0
    var j: Int = 0

    while (i < expectedPSDSize){
      while(j < psds.length) {
        psdAgg(i) += psds(j)(i)
        j += 1
      }

      psdAgg(i) /= j

      j = 0
      i += 1
    }

    psdAgg
  }
}
