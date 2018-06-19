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

package org.ode.perf

import org.ode.engine.signal_processing.Segmentation

/**
 * Performance test for mutables-segmentation vs functional-segmentation
 * Offset is on purpose removed from the test for simplicity
 *
 * @author Alexandre Degurse, Joseph Allemandou
 */
class PerfTestSegmentationFunctional
  extends PerfSpec[Array[Double], Array[Double], Array[Array[Double]]]
  with ArraySizeSpec {

  private def computeSegmentationFunctional(
    signal: Array[Double],
    windowSize: Int
  ): Array[Array[Double]] = {
    // Here to match code in class
    if (signal.length < windowSize) {
      throw new IllegalArgumentException(
        s"Incorrect segmentation signal length (${signal.length}), " +
          s"it should be larger than winSize ($windowSize)"
      )
    }

    // nWindows is the number of complete windows that will be generated
    val nWindows: Int = 1 + (signal.length - windowSize) / windowSize
    val segmentedSignal: Array[Array[Double]] = Array.ofDim[Double](nWindows, windowSize)
    Range(0, nWindows).foreach(i => {
      Array.copy(signal, i * windowSize, segmentedSignal(i), 0, windowSize)
    })
    segmentedSignal
  }

  // until needed here, to have even number of values
  val d1 = (dataStart to dataEnd by dataStep).toArray
  val d2 = (dataStart to dataEnd by dataStep).toArray

  private val segmentation = new Segmentation(1000)

  val f1 = (array: Array[Double]) => segmentation.compute(array)
  val f2 = (array: Array[Double]) => computeSegmentationFunctional(array, 1000)

  val f1Desc = "mutables-segmentation"
  val f2Desc = "functional-segmentation"

}
