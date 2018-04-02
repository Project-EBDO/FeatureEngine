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
  * @param winAgg The size of segment
  * @param overlap The distance between two consecutive segments
  * @param partial Bool that tells whether to keep the last buffer or not
  *
  */


class Segmentation(val winAgg: Int, val overlap: Int = 0, val partial: Boolean = false) {

  /**
   * Funtion that segmentes a signal in the most common way
   * @param signal The signal to be segmented
   * @return The segmented signal
   */
  def segmention(signal: Array[Double]) : Array[Array[Double]] = {

    // if overlap > winAgg, it means that data will be lost in the process
    if (overlap > winAgg){
      throw new IllegalArgumentException(s"Incorrect overlap (${overlap}) for segmention (${winAgg} max)")
    }

    val segmentedSignal = if (partial) {
      signal.iterator
        .sliding(winAgg, winAgg - overlap)
        .withPadding(0.0)
    } else {
      signal.iterator
        .sliding(winAgg, winAgg - overlap)
        .withPartial(false)
    }

    return segmentedSignal
      .map(_.toArray)
      .toArray
  }

  /**
   * Funtion that segmentes a signal using matlab method
   * @param signal The signal to be segmented
   * @param segmentedSize The size of a segment after data wrap
   *   segmentedSize < winAgg for the matlab data wrap to work
   * @return The segmented signal
   */
  def matlabSegmentation(
    signal: Array[Double],
    segmentedSize: Int
  ) : Array[Array[Double]] = {

    val matlabSeg = new Segmentation(segmentedSize, 0, false)

    return segmention(signal)
      // apply matlab data wrap
      .map(
        matlabSeg.segmention(_)
        .iterator
        .map(_.toArray)
        .toArray
        .transpose
        .map(_.sum)
      )
  }
}
