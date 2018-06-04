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

package org.ode.engine.signal_processing;


/**
  * Class that provides segmention functions
  * 
  * For now, its feature are limited to a simple segmentation that drops
  * incomple windows and support only overlap in samples.
  *
  * Author: Alexandre Degurse
  * 
  * @param winSize The size of a window
  * @param overlap The overlap used to slide on the signal as a Double.
  *
  */

class Segmentation(val winSize: Int, val overlap: Int = 0) {

  if (overlap > winSize || overlap < 0) {
    throw new IllegalArgumentException(s"Incorrect overlap (${overlap}) for segmentation with a winSize of ($winSize)")
  }

  val offset: Int = if (overlap > 0) overlap else winSize

  /**
   * Funtion that segmentates a signal and drops incomplete windows
   * @param signal The signal to be segmented as a Array[Double]
   * @return The segmented signal as a Array[Array[Double]]
   */
  def compute(signal: Array[Double]) : Array[Array[Double]] = {
    
    // nWindows is the number of complete windows that will be generated
    var nWindows: Int = 1 + (signal.length - winSize) / offset

    val segmentedSignal: Array[Array[Double]] = Array.ofDim[Double](nWindows, winSize)
    
    var i: Int = 0

    while (i < nWindows) {
      Array.copy(signal, i*offset, segmentedSignal(i), 0, winSize)
      i += 1
    }

    return segmentedSignal
  }
}
