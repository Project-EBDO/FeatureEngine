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
 * SpectrogramWindow defined as a trait
 *
 * @author Joseph Allemandou
 *
 * Provides a function to apply a spectrogram window to a signal,
 * leaving the actual coefficients computation to the implementations.
 * An IllegalArgumentException is thrown if signal.length != windowSize
 */
trait SpectrogramWindow extends Serializable {

  /**
   * The size of the window
   */
  val windowSize: Int

  /**
   *  The coefficients of the window (an array of size windowSize)
   */
  val windowCoefficients: Array[Double]

  /**
   * Function applying a SpectrogramWindow implementation to a signal portion
   *
   * @param signal The signal portion to transform
   * @return the transformed signal
   */
  def applyToSignal(signal: Array[Double]): Array[Double] = {
    if (signal.length != windowSize) {
      throw new IllegalArgumentException(s"Incorrect signal length (${signal.length}) " +
        s"for SpectrogramWindow ($windowSize)")
    }
    // Using while with local variables on purpose -- See performance test
    // scalastyle:off while var.local
    var i: Int = 0
    val res: Array[Double] = new Array[Double](windowSize)
    while (i < windowSize) {
      res(i) = signal(i) * windowCoefficients(i)
      i += 1
    }
    // scalastyle:on while var.local
    res
  }
}
