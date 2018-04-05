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

package org.ode.perf



import org.ode.utils.test.ErrorMetrics.rmse
import org.ode.engine.signal_processing.FFT
import edu.emory.mathcs.jtransforms.fft.DoubleFFT_1D;
import org.scalatest.{FlatSpec, Matchers}

/**
  * Tests for one-sided FFT
  * Author: Alexandre Degurse
  */

class FFTOnesided(nfft: Int) {

  val lowLevelFtt: DoubleFFT_1D = new DoubleFFT_1D(nfft)

  def compute(signal: Array[Double]) : Array[Double] = {
    if (signal.length > nfft) {
      throw new IllegalArgumentException(s"Incorrect signal length (${signal.length}) for FFT (${nfft})")
    }

    val fft: Array[Double] = signal ++ Array.fill(nfft - signal.length)(0.0)

    lowLevelFtt.realForward(fft)

    return fft
  }
}

class TestFFTOnesided extends FlatSpec with Matchers {


  "FFTOnesided" should "compute a fft slower than the two-sided version" in {
    val signal: Array[Double] = (1.0 to 1000.0 by 1.0).toArray
    val fftClass: FFT = new FFT(1000)
    val fftClassOnesided: FFTOnesided = new FFTOnesided(1000)

    val tBefore1 = System.nanoTime()
    val fft1 = fftClassOnesided.compute(signal)
    val tAfter1 = System.nanoTime()
    val d1 = (tAfter1 - tBefore1).toDouble

    val tBefore2 = System.nanoTime()
    val fft2 = fftClass.compute(signal)
    val tAfter2 = System.nanoTime()
    val d2 = (tAfter2 - tBefore2).toDouble

    println(d2/d1)

    d2 * 1.40 should be < d1
  }
}