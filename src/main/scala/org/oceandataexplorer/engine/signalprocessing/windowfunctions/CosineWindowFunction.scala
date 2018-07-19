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

import WindowFunctionTypes.{WindowFunctionType, Periodic, Symmetric}

/**
 * Abstract cosine-window-function class to facilitate creation of other window functions
 * This approach is based on Scipy implementation of cosine window functions
 *
 * @author Alexandre Degurse
 */
abstract class CosineWindowFunction extends WindowFunction {

  /**
   * The cosine coefficients used to generate the window coefficients
   */
  val cosineCoefficients: Array[Double]

  /**
   * Type of the window (either Symmetric or Periodic)
   */
  val windowType: WindowFunctionType

  /**
   * Eagerly instantiated array of coefficients
   */
  lazy val windowCoefficients: Array[Double] = {
    // using Range of indices instead of Range.Double to avoid rounding issues
    val window = windowType match {
      case Symmetric =>
        Range(0, windowSize).map(idx =>
          -math.Pi + idx * 2.0 * math.Pi / (windowSize - 1.0)
        ).toArray

      case Periodic =>
        Range(0, windowSize).map(idx =>
          -math.Pi + idx * 2.0 * math.Pi / windowSize
        ).toArray
    }

    window.map(x =>
      cosineCoefficients.indices.foldLeft(0.0)((res, idx) =>
        res + cosineCoefficients(idx) * math.cos(idx * x)
      )
    )
  }
}
