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

import org.ode.engine.signal_processing.TestWindow

/**
 * Performance test for mutables-spectrogramWindow vs functional-spectrogramWindow
 *
 * @author Alexandre Degurse, Joseph Allemandou
 */
class PerfTestSpectrogramWindowFunctional
  extends PerfSpec[Array[Double], Array[Double], Array[Double]]
  with ArraySizeSpec {

  private def applyWindowToSignalFunctional(
    signal: Array[Double],
    windowCoefficients: Array[Double],
    windowSize: Int
  ): Array[Double] = {
    // Here to match code in class
    if (signal.length != windowSize) {
      throw new IllegalArgumentException(s"Incorrect signal length (${signal.length}) " +
        s"for SpectrogramWindow ($windowSize)")
    }

    signal.zip(windowCoefficients).map{ case (s, c) => s * c}
  }

  // until needed here, to have even number of values
  val d1 = (dataStart to dataEnd by dataStep).toArray
  val d2 = (dataStart to dataEnd by dataStep).toArray

  private val testWindow = new TestWindow(d1.length)

  val f1 = (array: Array[Double]) => testWindow.applyToSignal(array)
  val f2 = (array: Array[Double]) => {
    applyWindowToSignalFunctional(array, testWindow.windowCoefficients, array.length)
  }
  val f1Desc = "mutables-SpectrogramWindow"
  val f2Desc = "functional-SpectrogramWindow"

}
