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

import org.oceandataexplorer.engine.signalprocessing.Energy

import scala.math._

/**
 * Performance tests for mutables-energy vs functional-energy -- From Signal
 *
 * @author Alexandre Degurse, Joseph Allemandou
 */
class PerfTestMutablesEnergyFromSignalVsFunctionalEnergyFromSignal
  extends PerfSpec[Array[Double], Array[Double], Double]
  with ArraySizeSpec {

  /**
   * Functionan version of energy from signal - for perf test
   */
  private def computeRawFromRawSignalFunctional(signal: Array[Double], nfft: Int): Double = {
    if (signal.length > nfft) {
      throw new IllegalArgumentException(
        s"Incorrect signal size (${signal.length}) for Energy ($nfft)"
      )
    }
    signal.foldLeft(0.0d)((e, v) => e + pow(v, 2))
  }

  val d1 = (dataStart to dataEnd by dataStep).toArray
  val d2 = (dataStart to dataEnd by dataStep).toArray

  private val energyClassEven = new Energy(d1.length)

  val f1 = (array: Array[Double]) => energyClassEven.computeRawFromRawSignal(array)
  val f2 = (array: Array[Double]) => computeRawFromRawSignalFunctional(array, array.length)

  val f1Desc = "mutables-energy-from-signal"
  val f2Desc = "functional-energy-from-signal"

}

/**
 * Performance tests for mutables-energy vs functional-energy -- From FTT
 *
 * @author Alexandre Degurse, Joseph Allemandou
 */
class PerfTestMutablesEnergyFromFFTVsFunctionalEnergyFromFFT
  extends PerfSpec[Array[Double], Array[Double], Double]
  with ArraySizeSpec {

  /**
   * Functional version of energy from fft - for perf test
   */
  private def computeRawFromFFTFunctional(
    fft: Array[Double],
    nfft: Int,
    expectedFFTSize: Int,
    nfftEven: Boolean
  ): Double = {

    if (fft.length != expectedFFTSize) {
      throw new IllegalArgumentException(
        s"Incorrect fft size (${fft.length}) for Energy ($expectedFFTSize)"
      )
    }

    (Range(1,  nfft/2).foldLeft(0.0d)((e, i) => {
      // duplicate the energy due to the symetry
      val i2 = i * 2
      val i2p1 = i2 + 1
      e +
        2.0 * pow(fft(i2), 2) +
        2.0 * pow(fft(i2p1), 2)
    }) +
      pow(fft(0), 2) +
      pow(fft(fft.length - 2), 2) * (if (nfftEven) 1.0 else 2.0) +
      pow(fft(fft.length - 1), 2) * (if (nfftEven) 1.0 else 2.0)) / nfft.toDouble

  }

  // until needed here, to have even number of values
  val d1 = (dataStart until dataEnd by dataStep).toArray
  val d2 = (dataStart until dataEnd by dataStep).toArray
  val nfft = d1.length - 2
  private val energyClassEven = new Energy(nfft)

  val f1 = (array: Array[Double]) => energyClassEven.computeRawFromFFT(array)
  val f2 = (array: Array[Double]) => computeRawFromFFTFunctional(array, nfft, d2.length, nfftEven = true)

  val f1Desc = "mutables-energy-from-fft"
  val f2Desc = "functional-energy-from-fft"

}

/**
 * Performance tests for mutables-energy vs functional-energy -- From PSD
 *
 * @author Alexandre Degurse, Joseph Allemandou
 */
class PerfTestMutablesEnergyFromPSDVsFunctionalEnergyFromPSD
  extends PerfSpec[Array[Double], Array[Double], Double]
  with ArraySizeSpec {

  /**
   * Functional version of energy from PSD for perf test
   */
  def computeRawFromPSDFunctional(psd: Array[Double], expectedPSDSize: Int): Double = {
    if (psd.length != expectedPSDSize) {
      throw new IllegalArgumentException(
        s"Incorrect PSD size (${psd.length}) for Energy ($expectedPSDSize)"
      )
    }
    psd.sum
  }

  // until needed here, to have even number of values
  val d1 = (dataStart until dataEnd by dataStep).toArray
  val d2 = (dataStart until dataEnd by dataStep).toArray
  val nfft = (d1.length - 1) * 2
  private val energyClassEven = new Energy(nfft)

  val f1 = (array: Array[Double]) => energyClassEven.computeRawFromPSD(array)
  val f2 = (array: Array[Double]) => computeRawFromPSDFunctional(array, array.length)

  val f1Desc = "mutables-energy-from-psd"
  val f2Desc = "functional-energy-from-psd"

}
