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
  * Author: Alexandre Degurse
  * 
  * @param winSize The size of segment
  *
  */


class Segmentation(val winSize: Int) {

  /**
   * Funtion that segmentes a signal and drops incomplete windows
   * @param signal The signal to be segmented
   * @return The segmented signal
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
