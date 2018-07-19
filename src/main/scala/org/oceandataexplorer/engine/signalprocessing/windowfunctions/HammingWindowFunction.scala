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

import WindowFunctionTypes.{WindowFunctionType, Periodic}

/**
 * HammingWindowFunction, extending the [[WindowFunction]] trait
 * A hamming window can be compute in two ways:
 *  - symmetric used for filter design,
 *   w(n) = 0.54 - 0.46 * cos(2* Pi * n / N) where 0 <= n <= N and N = windowLength - 1
 *  - periodic used for spectral analysis (extends discrete Fourier transform periodicity)
 *   w(n) = 0.54 - 0.46 * cos(2* Pi * n / N) where N/2 <= n <= N/2 - 1 and N = windowLength
 *
 * Hamming coefficients computation functions defined in companion object
 * and used to precompute coefficients for a given instance of window.
 *
 * @author Joseph Allemandou, Paul NGuyenhongduc, Alexandre Degurse
 *
 * @param windowSize The size of the window to be computed
 * @param windowType The type of hamming window to compute (periodic or symmetric),
 * default is periodic for spectral analysis
 */
case class HammingWindowFunction
(
  windowSize: Int,
  windowType: WindowFunctionType = Periodic
) extends CosineWindowFunction {

  /**
   * The cosine coefficients that defines the Hamming window function
   */
  val cosineCoefficients: Array[Double] = Array.apply(0.54, 0.46)
}
