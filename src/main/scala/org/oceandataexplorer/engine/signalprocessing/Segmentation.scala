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

package org.oceandataexplorer.engine.signalprocessing


/**
 * Segmention class
 *
 * Simple segmentation dropping incomplete windows and allowing for overlap.
 *
 * @author Alexandre Degurse
 *
 * @param windowSize The size of a window
 * @param windowOverlap The overlap used to slide the window over the signal (in number of samples).
 */
case class Segmentation(windowSize: Int, windowOverlap: Int = 0) extends Serializable {

  if (windowSize < 0) {
    throw new IllegalArgumentException(s"Incorrect winSize for segmentation ($windowSize)")
  }

  if (windowOverlap < 0 || windowOverlap >= windowSize) {
    throw new IllegalArgumentException(
      s"Incorrect overlap for segmentation "
      + s"($windowOverlap shouldn't be negative or higher (or equal) than $windowSize)"
    )
  }

  /**
   * The window offset, derived from windowSize and overlap
   */
  val offset = windowSize - windowOverlap

  /**
   * Funtion segmenting a signal and droping incomplete windows
   *
   * @param signal The signal to be segmented as a Array[Double]
   * @return The segmented signal as a Array of Array[Double]
   */
  def compute(signal: Array[Double]) : Array[Array[Double]] = {

    if (signal.length < windowSize) {
      throw new IllegalArgumentException(
        s"Incorrect segmentation signal length (${signal.length}), " +
        s"it should be larger than winSize ($windowSize)"
      )
    }
    // nWindows is the number of complete windows that will be generated
    val nWindows: Int = 1 + (signal.length - windowSize) / offset
    val segmentedSignal: Array[Array[Double]] = Array.ofDim[Double](nWindows, windowSize)

    Range(0, nWindows).foreach(i => {
      Array.copy(signal, i * offset, segmentedSignal(i), 0, windowSize)
    })

    segmentedSignal
  }
}
