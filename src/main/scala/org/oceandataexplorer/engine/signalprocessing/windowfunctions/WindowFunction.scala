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

package org.oceandataexplorer.engine.signalprocessing.windowfunctions


/**
 * WindowFunction defined as a trait
 *
 * @author Joseph Allemandou
 *
 * Provides a function to apply a window function to a signal,
 * leaving the actual coefficients computation to the implementations.
 * An IllegalArgumentException is thrown if signal.length != windowSize
 */
trait WindowFunction extends Serializable {

  /**
   * The size of the window
   */
  val windowSize: Int

  /**
   *  The coefficients of the window function (an array of size windowSize)
   */
  val windowCoefficients: Array[Double]

  /**
   * Compute the raw density normalization factor for power spectral density
   * which is sum(W_i ^ 2). lazy val so to have windowCoefficients instanciated before
   */
  lazy val densityRawNormFactor: Double = windowCoefficients
    .foldLeft(0.0)((acc, v) => acc + math.pow(v,2))

  /**
   * Compute the raw spectrum normalization factor
   * which is sum(w_i)^2. lazy val so to have windowCoefficients instanciated before
   */
  lazy val spectrumRawNormFactor: Double = math.pow(windowCoefficients.sum, 2)

  /**
   * Function computing a density normalization factor
   * as densityRawNormFactor / math/pow(alpha, 2)
   *
   * @param alpha User definied normalization factor for the window
   * With the default value of 1.0, the returned normalization factor is the energy of the windows
   * @return The density normalization factor of the window
   */
  def densityNormalizationFactor(alpha: Double = 1.0): Double = {
    densityRawNormFactor / math.pow(alpha, 2)
  }

  /**
   * Function computing a spectrum normalization factor
   * as spectrumRawNormFactor / math.pow(alpha, 2)
   *
   * @param alpha User definied normalization factor for the window
   * With the default value of 1.0, the returned normalization factor is the energy of the windows
   * @return The spectrum normalization factor of the window
   */
  def spectrumNormalizationFactor(alpha: Double = 1.0): Double = {
    spectrumRawNormFactor / math.pow(alpha, 2)
  }


  /**
   * Function applying a WindowFunction implementation to a signal portion
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
