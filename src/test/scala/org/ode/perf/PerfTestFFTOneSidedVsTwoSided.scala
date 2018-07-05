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

import org.ode.engine.signal_processing.{FFT, FFTTwoSided}
import org.ode.utils.test.ErrorMetrics


/**
 * Tests for one-sided FFT
 * Authors: Alexandre Degurse, Joseph Allemandou
 */
class PerfTestFFTOneSidedVsTwoSided
  extends PerfSpec[Array[Double], Array[Double], Array[Double]]
  with ArraySizeSpec {

  val d1 = (dataStart to dataEnd by dataStep).toArray
  val d2 = (dataStart to dataEnd by dataStep).toArray
  val f1 = (array: Array[Double]) => new FFT(array.length, 1.0f).compute(array)
  val f2 = (array: Array[Double]) => new FFTTwoSided(array.length).compute(array)
  val f1Desc = "fft-1-sided"
  val f2Desc = "fft-2-sided"

  // Results are not equal, need to override equalCheck
  override val equalCheck = (r1: Array[Double], r2: Array[Double]) => {
    r1.length should equal(r2.length / 2 + 1) // nfft odd
    ErrorMetrics.rmse(r1.toSeq, r2.splitAt(r2.length / 2 + 1)._1.toSeq) should be < 1.0e-13
  }

}
