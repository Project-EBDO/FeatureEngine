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
  * @param overlap The distance between two consecutive segments
  * @param partial Bool that tells whether to keep the last buffer or not
  *
  */


class Segmentation(val winSize: Int, val overlap: Double = 1.0, val partial: Boolean = false) {

  /**
   * Funtion that segmentes a signal
   * @param signal The signal to be segmented
   * @return The segmented signal
   */
  def compute(signal: Array[Double]) : Array[Array[Double]] = {

    // if overlap > winSize, it means that data will be lost in the process
    if ((overlap < 0.0) || (overlap > 1.0)){
      throw new IllegalArgumentException(s"Incorrect overlap (${overlap}) for segmention")
    }
    
    val step: Int = (winSize* overlap).toInt
    // nWindows is the number of complete windows that will be generated
    var nWindows: Int = 1 + (signal.length - winSize) / step

    val segmentedSignal = if(partial) {
      // allocate an additionnal window if the partial chunck is kept
      Array.ofDim[Double](nWindows + 1, winSize)
      } else {
      Array.ofDim[Double](nWindows, winSize)
      }
    
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

    // manually compute the incomplete chunck with zero-padding
    if (partial) {
      // i == nWindow now, the index of the incomplete chunck and j == 0
      // add the last values in the incomplete chunk, the rest is already
      // initialized to 0.0 
      while (i*step + j  < signal.length) {
        segmentedSignal(i)(j) = signal(i*step + j)
        j += 1
      }
    }

    return segmentedSignal
  }
}
