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

package org.ebdo.engine.signal_processing;


/**
  * Generic functions for signal processing
  * Author: Alexandre Degurse
  *
  */


object Segmentation {


  /**
   * Funtion that segmentates a signal in the most common way
   * @param signal The signal to be segmentated
   * @param winAgg The size of segment
   * @param overlap The distance between two consecutive segments
   * @param partial Bool that tells whether to keep the last buffer or not
   * @return The segmentated signal
   */
  def segmentation(
    signal: Array[Double],
    winAgg: Int,
    overlap: Int = 0,
    partial: Boolean = false
  ) : Array[Array[Double]] = {

    // if overlap > winAgg, it means that data will be lost in the process
    if (overlap > winAgg){
      throw new IllegalArgumentException(s"Incorrect overlap for segmentation")
    }

    val segmentedSignal = if (partial){
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
   * Funtion that segmentates a signal using matlab method
   * @param signal The signal to be segmentated
   * @param winAgg The size of segment
   * @param segmentatedSize The size of a segment
   * @param overlap The distance between two consecutive segments
   * @return The segmentated signal
   */
  def matlabSegmentation(
    signal: Array[Double],
    winAgg: Int,
    segmentedSize: Int,
    overlap: Int = 0
  ) : Array[Array[Double]] = {
    return segmentation(signal, winAgg, overlap, false)
      // apply matlab data wrap
      .map(
        segmentation(_, segmentedSize, 0, true)
        .iterator
        .map(_.toArray)
        .toArray
        .transpose
        .map(_.sum)
      )
  }
}
