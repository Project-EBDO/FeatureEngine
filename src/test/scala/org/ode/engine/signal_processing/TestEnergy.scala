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
  val maxError = 1.0E-15


  it should "compute the energy of the signal when given a raw even signal, its FFT or its PSD" in {

    val signal = (1.0 to 10.0 by 1.0).toArray

    // two-sided FFT, non normalized
    val fft = Array(
      5.5000000000000000e+01,  0.0000000000000000e+00,
      -4.9999999999999893e+00,  1.5388417685876263e+01,
      -5.0000000000000000e+00,  6.8819096023558695e+00,
      -4.9999999999999964e+00,  3.6327126400268090e+00,
      -5.0000000000000000e+00,  1.6245984811645304e+00,
      -5.0000000000000009e+00,  4.4408920985006262e-16,
      -5.0000000000000000e+00,  1.6245984811645304e+00,
      -5.0000000000000027e+00,  3.6327126400268006e+00,
      -5.0000000000000000e+00,  6.8819096023558695e+00,
      -5.0000000000000107e+00,  1.5388417685876270e+01
    )

    // normalized PSD
    val psd = Array(
      302.5               ,  52.36067977499792  ,  14.472135954999573 ,
      7.639320225002109 ,   5.5278640450004275,   2.5
    )

    val nfft = fft.length / 2
    val oneSidedLength = nfft + (if (nfft % 2 == 0) 2 else 1)

    val energyClass = new Energy(nfft)

    val eSig = energyClass.fromRawSignal(signal)
    val eFFTTwo = energyClass.fromFFTTwoSided(fft)
    val eFFTOne = energyClass.fromFFTOneSided(fft.take(oneSidedLength))
    val ePSD = energyClass.fromPSD(psd)


    val eExpected = 385.0

    abs((eSig - eExpected) / eExpected) should be < maxError
    abs((eFFTOne - eSig) / eSig) should be < maxError
    abs((eFFTTwo - eSig) / eSig) should be < maxError
    abs((ePSD - eSig) / eSig) should be < maxError
  }

  it should "compute the energy of the signal when given a raw odd signal, its FFT or its PSD" in {

    val signal = (1.0 to 11.0 by 1.0).toArray

    // two-sided FFT, non normalized
    val fft = Array(
      66.0                ,  0.0               , -5.500000000000039,
      18.731279813890872  , -5.499999999999956,  8.55816705136492  ,
      -5.499999999999981  ,  4.765777128986838, -5.499999999999951 ,
      2.5117658384695414 , -5.49999999999997  ,  0.7907806169723581,
      -5.49999999999997  ,  0.7907806169723581, -5.499999999999951 ,
      2.5117658384695414 , -5.499999999999981 ,  4.765777128986838 ,
      -5.499999999999956 ,  8.55816705136492  , -5.500000000000039 ,
      18.731279813890872
    )

    // normalized PSD
    val psd = Array(
      396.0            ,  69.29288063023205 ,  18.816767868921374,
      9.629569389668113,   6.647085023145817,   5.613697088032706
    )

    val nfft = fft.length / 2
    val oneSidedLength = nfft + (if (nfft % 2 == 0) 2 else 1)

    val energyClass = new Energy(nfft)

    val eSig = energyClass.fromRawSignal(signal)
    val eFFTTwo = energyClass.fromFFTTwoSided(fft)
    val eFFTOne = energyClass.fromFFTOneSided(fft.take(oneSidedLength))
    val ePSD = energyClass.fromPSD(psd)

    val eExpected = 506.0

    abs((eSig - eExpected) / eExpected) should be < maxError
    abs((eFFTOne - eSig) / eSig) should be < maxError
    abs((eFFTTwo - eSig) / eSig) should be < maxError
    abs((ePSD - eSig) / eSig) should be < maxError
  }

  it should "raise an IllegalArgumentException when given a mishaped two-sided FFT" in {
    val energyClass = new Energy(100)

    an [IllegalArgumentException] should be thrownBy energyClass.fromFFTTwoSided(Array(1.0))
  }

  it should "raise an IllegalArgumentException when given a mishaped one-sided FFT" in {
    val energyClass = new Energy(101)

    an [IllegalArgumentException] should be thrownBy energyClass.fromFFTTwoSided(Array(1.0))
  }

  it should "raise an IllegalArgumentException when given a mishaped raw signal" in {
    val energyClass = new Energy(5)
    val signal = (1.0 to 10.0 by 1.0).toArray

    an [IllegalArgumentException] should be thrownBy energyClass.fromRawSignal(signal)
  }
}
