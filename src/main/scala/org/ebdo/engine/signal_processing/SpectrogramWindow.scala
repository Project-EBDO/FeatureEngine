/** Copyright (C) 2017 Project-EBDO
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

package org.ebdo.engine.signal_processing

/**
 * SpectrogramWindow defined as a trait
 * Author: Joseph Allemandou
 *
 * Provides a function to apply a spectrogram window to a signal,
 * leaving the actual coefficients computation to the implementations.
 * An IllegalArgumentException is thrown if signal.length != windowSize
 *
 */
trait SpectrogramWindow {

  val windowSize: Int
  val windowCoefficients: Array[Double]

  def applyToSignal(signal: Array[Double]): Array[Double] = {
    if (signal.length != windowSize) {
      throw new IllegalArgumentException(s"Incorrect signal length (${signal.length}) for SpectrogramWindow (${windowSize})")
    }
    // Use mutables for performance
    var i: Int = 0
    val res: Array[Double] = new Array[Double](windowSize)
    while (i < windowSize) {
      res(i) = signal(i) * windowCoefficients(i)
      i += 1
    }
    return res
  }
}
