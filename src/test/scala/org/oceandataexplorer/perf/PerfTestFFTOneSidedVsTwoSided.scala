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

import org.oceandataexplorer.engine.signalprocessing.{FFT, FFTTwoSided}
import org.oceandataexplorer.utils.test.OdeCustomMatchers


/**
 * Tests for one-sided FFT
 * Authors: Alexandre Degurse, Joseph Allemandou
 */
class PerfTestFFTOneSidedVsTwoSided
  extends PerfSpec[Array[Double], Array[Double], Array[Double]]
  with ArraySizeSpec
  with OdeCustomMatchers
{
  /**
   * Maximum error allowed for [[OdeCustomMatchers.RmseMatcher]]
   */
  val maxRMSE = 1.0E-13

  val d1 = (dataStart to dataEnd by dataStep).toArray
  val d2 = (dataStart to dataEnd by dataStep).toArray
  val f1 = (array: Array[Double]) => FFT(array.length, 1.0f).compute(array)
  val f2 = (array: Array[Double]) => new FFTTwoSided(array.length).compute(array)
  val f1Desc = "fft-1-sided"
  val f2Desc = "fft-2-sided"

  // Results are not equal, need to override equalCheck
  override val equalCheck = (r1: Array[Double], r2: Array[Double]) => {
    r1.length should equal(r2.length / 2 + 1) // nfft odd
    r1 should rmseMatch(r2.splitAt(r2.length / 2 + 1)._1)
  }

}
