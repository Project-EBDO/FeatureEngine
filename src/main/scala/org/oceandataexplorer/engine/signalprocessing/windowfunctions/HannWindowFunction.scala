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

import WindowFunctionTypes.{Periodic, WindowFunctionType}

/**
 * HannWindowFunction, extending the [[CosineWindowFunction]] abstract class
 *
 * @author Alexandre Degurse
 *
 * @param windowSize The size of the window to be computed
 * @param windowType The type of window to compute (periodic or symmetric),
 * default is periodic for spectral analysis
 */
case class HannWindowFunction
(
  windowSize: Int,
  windowType: WindowFunctionType = Periodic
) extends CosineWindowFunction {

  /**
   * The cosine coefficients that defines the Hann window function
   */
  val cosineCoefficients: Array[Double] = Array.apply(0.50, 0.5)
}
