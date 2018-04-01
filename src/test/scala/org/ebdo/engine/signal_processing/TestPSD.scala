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



import org.ebdo.utils.test.ErrorMetrics.rmse;
import org.scalatest.{FlatSpec, Matchers};
import scala.math.cos;

/**
  * Tests for PSD class
  * Author: Alexandre Degurse
  */


class TestPSD extends FlatSpec with Matchers {

  val maxRMSE = 1E-15

  "PSD" should "compute the same psd as matlab on a fake signal" in {

    val signal: Array[Double] = (0.0 to 10.0 by 0.5).map(cos).toArray
    val fs: Double = 1000.0
    val nfft: Int = signal.length
    val normalizationFactor = 1 / (nfft * fs)


    val fftClass: FFT = new FFT(nfft)
    val fft: Array[Double] = fftClass.compute(signal)
    val psdClass: PSD = new PSD(nfft, normalizationFactor)
    val psd: Array[Double] = psdClass.compute(fft)

    val expectedPSD: Array[Double] = Array(
      0.000046183877317531,
      0.001108588145743445,
      0.008475517383663853,
      0.000724845355249338,
      0.000297021096487393,
      0.000175292738540965,
      0.000122987393805515,
      0.000096068184832849,
      0.000081114323956207,
      0.000072923862937450,
      0.000069261920931957
    )

    rmse(psd, expectedPSD) should be < maxRMSE
  }


  it should "raise IllegalArgumentException when given a signal of the wrong length" in {
    val signal: Array[Double] = (0.0 to 10.0 by 0.1).map(cos).toArray
    val psdClass: PSD = new PSD(50, 1.0)

    an [IllegalArgumentException] should be thrownBy psdClass.compute(signal)
  }
}