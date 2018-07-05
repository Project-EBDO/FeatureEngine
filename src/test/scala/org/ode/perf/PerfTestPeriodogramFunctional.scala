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

import org.ode.engine.signal_processing.Periodogram

/**
 * Performance test for mutables-periodogram vs functional-periodogram
 *
 * @author Alexandre Degurse, Joseph Allemandou
 */
class PerfTestPeriodogramFunctional
  extends PerfSpec[Array[Double], Array[Double], Array[Double]]
  with ArraySizeSpec {


  /**
   * Method computing periodogram using a functional syntax
   * Used to show that while-syntax is faster than functional one
   */
  def computePeriodogramFunctional(
    fft: Array[Double],
    nfftEven: Boolean,
    uniqueSamples: Int,
    normalizationFactor: Double
  ) : Array[Double] = {
    // Only here to match non-functional computation for perf tests
    if (fft.length != 2*uniqueSamples) {
      throw new IllegalArgumentException(s"Incorrect fft length (${fft.length}) for Periodogram" +
        s"it should be a one-sided (${2*uniqueSamples}) FFT")
    }

    val oneSidedPeriodogram: Array[Double] = new Array[Double](uniqueSamples)

    // Precompute first value
    oneSidedPeriodogram(0) = normalizationFactor * (math.pow(fft(0), 2) + math.pow(fft(1), 2))

    val last = uniqueSamples - 1
    // Start at the second value and iterate
    Range(1, last).foreach(i => {
      val i2 = 2 * i
      val i2p1 = i2 + 1
      oneSidedPeriodogram(i) = 2.0 * normalizationFactor *
        (math.pow(fft(i2), 2) + math.pow(fft(i2p1), 2))
    })

    // manually compute last value, depending on even
    // or not numbers of values (presence or not of nyquist frequency)
    // at the end of the while, i = uniqueSamples-1 = oneSidedPeriodogram.length - 1,
    // Therefore, i2 and i2p1 are equal to what they should be to compute the last value
    val last2 = 2 * last
    val last2p1 = last2 + 1
    oneSidedPeriodogram(last) = normalizationFactor *
      (math.pow(fft(last2), 2) + math.pow(fft(last2p1), 2))

    if (!nfftEven) {
      oneSidedPeriodogram(last) *= 2.0
    }

    oneSidedPeriodogram
  }

  // until needed here, to have even number of values
  val d1 = (dataStart until dataEnd by dataStep).toArray
  val d2 = (dataStart until dataEnd by dataStep).toArray

  val fs = 1000.0
  val nfft = d1.length - 2
  val nfftEven = nfft % 2 == 0
  val uniqueSamples = if (nfftEven) nfft / 2 + 1 else (nfft + 1) / 2
  val normalizationFactor = 1 / (fs * nfft)
  val periodogramClass: Periodogram = new Periodogram(nfft, normalizationFactor, 1.0f)

  val f1 = (array: Array[Double]) => periodogramClass.compute(array)
  val f2 = (array: Array[Double]) => {
    computePeriodogramFunctional(array, nfftEven, uniqueSamples, normalizationFactor)
  }
  val f1Desc = "mutables-periodogram"
  val f2Desc = "functional-periodogram"

}
