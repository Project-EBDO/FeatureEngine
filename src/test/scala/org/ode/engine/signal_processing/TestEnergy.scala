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

package org.ode.engine.signal_processing

import org.ode.utils.test.ErrorMetrics
import scala.math.abs

import org.scalatest.{FlatSpec, Matchers}

/**
  * Tests for Energy Functions
  * Author: Alexandre Degurse
  */

class TestEnergy extends FlatSpec with Matchers {
  // The error is computed relatively to the expected value
  val maxError = 1.0E-3

  val signal = (1.0 to 50.0 by 1.0).toArray

  val fft = Array(1275.0,0.0,-24.999999999999943,397.3636210966325,-24.99999999999994,
    197.89537720764565,-25.000000000000046,131.05458952782936,-25.00000000000001,
    97.36857137324647,-24.99999999999998,76.94208842938131,-24.99999999999998,
    63.14279223618263,-24.999999999999968,53.12770432893008,-25.0,45.47483118202664,
    -25.000000000000025,39.393696499216254,-24.99999999999997,34.40954801177932,
    -24.999999999999968,30.219808760240255,-24.999999999999993,26.622296008119804,
    -25.000000000000014,23.476562645437284,-25.000000000000032,20.681798649311865,
    -25.0,18.163563200133993,-24.99999999999999,15.86548243860372,-24.999999999999993,
    13.743866304819262,-24.999999999999996,11.764107030306281,-25.000000000000007,
    9.898200219943021,-25.00000000000002,8.122992405822643,-25.000000000000004,
    6.418909009193172,-25.000000000000004,4.769005055464177,-25.00000000000001,
    3.158234461152702,-25.000000000000007,1.572866681341253,-25.0,0.0)

  // normalized PSD !
  val psd = Array(32512.5, 6340.913894841126, 1591.5032128062544, 712.0122174523138,
    404.2255476506797, 261.80339887498934, 184.48048845526898, 137.90211869048858,
    107.7184108413529, 87.07453295489456, 72.3606797749978, 61.529473660219686,
    53.34986578975805, 47.045959745805675, 42.109471814827195, 38.196601125010474,
    35.068541320385705, 32.55575444018984, 30.53576856882006, 28.918974703763215,
    27.639320225002134, 26.648095714732058, 25.909736368761724, 25.398977796464525,
    25.09895638389095, 12.5)

  val eSig = Energy.computeFromSignal(signal)
  val eFFT = Energy.computeFromFFT(fft)
  val ePSD = Energy.computeFromPSD(psd)

  // can be obtained with the following matlab code:
  // signal = 1:50;
  // f = fft(signal);
  // eExpected = sum(F.*conj(F)) / 50
  val eExpected = 42925.0

  "Energy" should "compute the energy of the signal when given the signal or the FFT or the PSD" in {
    abs((eSig - eExpected) / eExpected) should be < maxError
    abs((eFFT - eSig) / eSig) should be < maxError
    abs((ePSD - eSig) / eSig) should be < maxError
  }
}
