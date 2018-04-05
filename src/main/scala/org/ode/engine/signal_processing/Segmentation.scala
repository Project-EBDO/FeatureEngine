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
  * incomple windows and doesn't support overlap.
  * New features will come in the near future
  *
  * Author: Alexandre Degurse
  * 
  * @param winSize The size of a window
  *
  */

class Segmentation(val winSize: Int) {

  /**
   * Funtion that segmentates a signal and drops incomplete windows
   * @param signal The signal to be segmented as a Array[Double]
   * @return The segmented signal as a Array[Array[Double]]
   */
  def compute(signal: Array[Double]) : Array[Array[Double]] = {
    
    // nWindows is the number of complete windows that will be generated
    var nWindows: Int = signal.length / winSize

    val segmentedSignal: Array[Array[Double]] = Array.ofDim[Double](nWindows, winSize)
    
    var i: Int = 0

    while (i < nWindows) {
      Array.copy(signal, i*winSize, segmentedSignal(i), 0, winSize)
      i += 1
    }

    return segmentedSignal
  }
}
