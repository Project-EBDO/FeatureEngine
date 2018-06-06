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
 * Class that provides aggregation function for signal processing
 *
 * Author: Alexandre Degurse
 */

class Aggregation {


  /**
   * That compute Wech estimate of the Power Spectral Density out of
   * multiple periodograms on the signal
   *
   * @param psds The PSDs on the signal that can indifferently be one or two sided
   * The PSD must be normalized either before or after the aggregation
   * @return The Welch Power Spectral Density estimate over all the PSDs
   *
   */
  def welch(psds: Array[Array[Double]]): Array[Double] = {

    if (!psds.foldLeft(true)((isSameSize, psd) => (psd.length == psds(0).length) && isSameSize)) {
      throw new IllegalArgumentException("Incorrect psd length for Welch aggregation")
    }

    val psdAgg: Array[Double] = new Array[Double](psds(0).length)

    var i: Int = 0
    var j: Int = 0

    while (i < psds(0).length){
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