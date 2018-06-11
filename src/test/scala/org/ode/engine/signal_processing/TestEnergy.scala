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



  val signalEven = (1.0 to 10.0 by 1.0).toArray

  // one-sided FFT, non normalized
  val fftEven = Array(
    5.5000000000000000e+01,  0.0000000000000000e+00,
    -4.9999999999999893e+00,  1.5388417685876263e+01,
    -5.0000000000000000e+00,  6.8819096023558695e+00,
    -4.9999999999999964e+00,  3.6327126400268090e+00,
    -5.0000000000000000e+00,  1.6245984811645304e+00,
    -5.0000000000000009e+00,  4.4408920985006262e-16
  )

  // normalized PSD
  val psdEven = Array(
    302.5               ,  52.36067977499792  ,  14.472135954999573 ,
    7.639320225002109 ,   5.5278640450004275,   2.5
  )

  val nfftEven = signalEven.length

  val energyClassEven = new Energy(nfftEven)

  val signalOdd = (1.0 to 11.0 by 1.0).toArray

  // one-sided FFT, non normalized
  val fftOdd = Array(
    66.0                ,  0.0               , -5.500000000000039,
    18.731279813890872  , -5.499999999999956,  8.55816705136492  ,
    -5.499999999999981  ,  4.765777128986838, -5.499999999999951 ,
    2.5117658384695414 , -5.49999999999997  ,  0.7907806169723581
  )

  // normalized PSD
  val psdOdd = Array(
    396.0            ,  69.29288063023205 ,  18.816767868921374,
    9.629569389668113,   6.647085023145817,   5.613697088032706
  )

  val nfftOdd = signalOdd.length
  val oneSidedLengthOdd = nfftOdd + (if (nfftOdd % 2 == 0) 2 else 1)

  val energyClassOdd = new Energy(nfftOdd)

  it should "compute the energy of the signal when given a raw even signal, its FFT or its PSD" in {
    val eSig = energyClassEven.computeRawFromRawSignal(signalEven)
    val eFFT = energyClassEven.computeRawFromFFT(fftEven)
    val ePSD = energyClassEven.computeRawFromPSD(psdEven)

    val eExpected = 385.0

    abs((eSig - eExpected) / eExpected) should be < maxError
    abs((eFFT - eSig) / eSig) should be < maxError
    abs((ePSD - eSig) / eSig) should be < maxError
  }

  it should "compute the energy of the signal when given a raw odd signal, its FFT or its PSD" in {
    val eSig = energyClassOdd.computeRawFromRawSignal(signalOdd)
    val eFFT = energyClassOdd.computeRawFromFFT(fftOdd)
    val ePSD = energyClassOdd.computeRawFromPSD(psdOdd)

    val eExpected = 506.0

    abs((eSig - eExpected) / eExpected) should be < maxError
    abs((eFFT - eSig) / eSig) should be < maxError
    abs((ePSD - eSig) / eSig) should be < maxError
  }

  it should "compute the Sound Pressure Level of the signal when given a raw even signal, its FFT or its PSD" in {
    val splSig = energyClassEven.computeSPLFromRawSignal(signalEven)
    val splFFT = energyClassEven.computeSPLFromFFT(fftEven)
    val splPSD = energyClassEven.computeSPLFromPSD(psdEven)

    val splExpected = 25.854607295085007

    abs((splSig - splExpected) / splExpected) should be < maxError
    abs((splFFT - splSig) / splSig) should be < maxError
    abs((splPSD - splSig) / splSig) should be < maxError
  }

  it should "compute the Sound Pressure Level of the signal when given a raw odd signal, its FFT or its PSD" in {
    val splSig = energyClassOdd.computeSPLFromRawSignal(signalOdd)
    val splFFT = energyClassOdd.computeSPLFromFFT(fftOdd)
    val splPSD = energyClassOdd.computeSPLFromPSD(psdOdd)

    val splExpected = 27.041505168397993

    abs((splSig - splExpected) / splExpected) should be < maxError
    abs((splFFT - splSig) / splSig) should be < maxError
    abs((splPSD - splSig) / splSig) should be < maxError
  }

  it should "raise an IllegalArgumentException when given a mishaped FFT" in {
    val energyClass = new Energy(101)

    an [IllegalArgumentException] should be thrownBy energyClass.computeRawFromFFT(Array(1.0))
    an [IllegalArgumentException] should be thrownBy energyClass.computeSPLFromFFT(Array(1.0))
  }

  it should "raise an IllegalArgumentException when given a mishaped raw signal" in {
    val energyClass = new Energy(5)
    val signal = (1.0 to 10.0 by 1.0).toArray

    an [IllegalArgumentException] should be thrownBy energyClass.computeRawFromRawSignal(signal)
    an [IllegalArgumentException] should be thrownBy energyClass.computeSPLFromRawSignal(signal)
  }

  it should "raise an IllegalArgumentException when given a mishaped PSD" in {
    val energyClass = new Energy(1000)

    an [IllegalArgumentException] should be thrownBy energyClass.computeRawFromPSD(Array(1.0))
    an [IllegalArgumentException] should be thrownBy energyClass.computeSPLFromPSD(Array(1.0))
  }
}
