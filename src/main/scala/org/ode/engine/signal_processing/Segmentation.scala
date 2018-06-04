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
  * @param offset The offset used to slide the window over the signal (in number of values).
  *
  */

class Segmentation(val winSize: Int, val offset: Option[Int] = None) {

  if (winSize < 0) {
    throw new IllegalArgumentException(s"Incorrect winSize for segmentation (${winSize})")
  }

  if (offset.getOrElse(winSize) > winSize || offset.getOrElse(winSize) <= 0) {
    throw new IllegalArgumentException(s"Incorrect offset for segmentation (${offset.getOrElse(winSize)})")
  }


  /**
   * Funtion that segmentates a signal and drops incomplete windows
   * @param signal The signal to be segmented as a Array[Double]
   * @return The segmented signal as a Array[Array[Double]]
   */
  def compute(signal: Array[Double]) : Array[Array[Double]] = {

  if (signal.length < winSize) {
    throw new IllegalArgumentException(s"Incorrect signal length for segmentation (${signal.length}), winSize is greater (${winSize})")
  }

    // nWindows is the number of complete windows that will be generated
    var nWindows: Int = 1 + (signal.length - winSize) / offset.getOrElse(winSize)

    val segmentedSignal: Array[Array[Double]] = Array.ofDim[Double](nWindows, winSize)

    var i: Int = 0

    while (i < nWindows) {
      Array.copy(signal, i * offset.getOrElse(winSize), segmentedSignal(i), 0, winSize)
      i += 1
    }

    return segmentedSignal
  }
}
