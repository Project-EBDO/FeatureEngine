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

import scala.math.{cos, Pi}

/**
 * HammingWindow, extending the [[SpectrogramWindow]] trait
 * Author: Joseph Allemandou, Paul NGuyenhongduc
 *
 * Hamming coefficients function defined in companion object 
 * and used to precompute coefficients for a given instance of window.
 */

class HammingWindow(val windowSize: Int) extends SpectrogramWindow {
  val windowCoefficients: Array[Double] =
    (0 until windowSize).map(idx => HammingWindow.coefficient(idx, windowSize)).toArray
}

object HammingWindow {
  // Generate the i-th coefficient of a N-point Hamming window
  def coefficient(idx: Int, windowSize: Int): Double = 0.54 - 0.46 * cos(2 * Pi * idx / (windowSize - 1))
}
