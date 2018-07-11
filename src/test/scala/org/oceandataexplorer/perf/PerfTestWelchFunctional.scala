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

package org.oceandataexplorer.perf

import org.oceandataexplorer.engine.signalprocessing.WelchSpectralDensity

/**
 * Performance test for mutables-welch vs functional-welch
 *
 * @author Alexandre Degurse, Joseph Allemandou
 */
class PerfTestWelchFunctional
  extends PerfSpec[Array[Array[Double]], Array[Array[Double]], Array[Double]]
  with ArraySizeSpec {

  private def computeWelchFunctional(
    periodograms: Array[Array[Double]],
    expectedPeriodogramSize: Int
  ): Array[Double] = {
    val psdAgg: Array[Double] = new Array[Double](expectedPeriodogramSize)
    // Not needed functionally, but to mimic the original method
    if (!periodograms.forall(_.length == expectedPeriodogramSize)) {
      throw new IllegalArgumentException(
        s"Inconsistent periodogram lengths for Welch aggregation ($expectedPeriodogramSize)"
      )
    }
    Range(0, expectedPeriodogramSize).foreach(i => {
      periodograms.indices.foreach(j => {
        psdAgg(i) += periodograms(j)(i)
      })
      psdAgg(i) /= periodograms.length
    })
    psdAgg
  }

  // until needed here, to have even number of values
  val d1 = (dataStart until dataEnd by dataStep).grouped(1000).toArray.map(_.toArray)
  val d2 = (dataStart until dataEnd by dataStep).grouped(1000).toArray.map(_.toArray)

  val nfft = 1998
  val welchSpectralDensityClass: WelchSpectralDensity = new WelchSpectralDensity(nfft, 1.0f)

  val f1 = (array: Array[Array[Double]]) => welchSpectralDensityClass.compute(array)
  val f2 = (array: Array[Array[Double]]) => computeWelchFunctional(array, array.head.length)
  val f1Desc = "mutables-welch"
  val f2Desc = "functional-welch"

}
