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
  * @param offset The distance between two consecutive segments
  * @param partial Bool that tells whether to keep the last buffer or not
  *
  */


class Segmentation(val winSize: Int, val offset: Int = 0) {

  /**
   * Funtion that segmentes a signal in the most common way
   * @param signal The signal to be segmented
   * @return The segmented signal
   */
  def segmention(signal: Array[Double]) : Array[Array[Double]] = {

    // if offset > winSize, it means that data will be lost in the process
    if (offset > winSize){
      throw new IllegalArgumentException(s"Incorrect offset (${offset}) for segmention (${winSize} max)")
    }
    
    var nWindows: Int = (signal.length - winSize) / (winSize - offset)
    val step: Int = nWindows - offset

    println("nWIndows : " + nWindows.toString + " step : " + step.toString + " winSize : " + winSize.toString)

    val segmentedSignal = Array.ofDim[Double](nWindows, winSize)
    
    var i: Int = 0
    var j: Int = 0

    while (i < nWindows) {
      while (j < winSize) {
        segmentedSignal(i)(j) = signal(i*step + j)
        j += 1
      }
      j = 0
      i += 1
    }

    return segmentedSignal
  }
}
